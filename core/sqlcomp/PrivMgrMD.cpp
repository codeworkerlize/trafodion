//*****************************************************************************

//*****************************************************************************

// ==========================================================================
// Contains non inline methods in the following classes
//   PrivMgrMDAdmmin
// ==========================================================================

// Needed for parser flag manipulation
#define SQLPARSERGLOBALS_FLAGS
#include "sqlcomp/PrivMgrMD.h"

#include <algorithm>
#include <set>
#include <string>

#include "PrivMgrComponentOperations.h"
#include "PrivMgrComponents.h"
#include "cli/SQLCLIdev.h"
#include "common/ComDistribution.h"
#include "common/ComSmallDefs.h"
#include "common/ComUser.h"
#include "parser/SqlParserGlobalsCmn.h"
#include "sqlcomp/CmpSeabaseDDL.h"
#include "sqlcomp/CmpSeabaseDDLauth.h"
#include "sqlcomp/PrivMgrComponentPrivileges.h"
#include "sqlcomp/PrivMgrMDDefs.h"
#include "sqlcomp/PrivMgrObjects.h"
#include "sqlcomp/PrivMgrPrivileges.h"
#include "sqlcomp/PrivMgrRoles.h"
// sqlcli.h included because ExExeUtilCli.h needs it (and does not include it!)
#include "arkcmp/CmpContext.h"
#include "cli/sqlcli.h"
#include "comexe/ComQueue.h"
#include "common/CmpCommon.h"
#include "common/ComUser.h"
#include "executor/ExExeUtilCli.h"
#include "export/ComDiags.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"

// *****************************************************************************
//    PrivMgrMDAdmin static methods
// *****************************************************************************

static int32_t createTable(const char *tableName, const QString *tableDDL, ExeCliInterface *cliInterface,
                           ComDiagsArea *pDiags);

static int32_t dropTable(const char *objectName, ExeCliInterface &cliInterface, ComDiagsArea *pDiags);

static void cleanupTable(const char *objectName, ExeCliInterface &cliInterface, ComDiagsArea *pDiags);

static short injectError(const char *errEnvvar, const char *details) {
  char *injectError = getenv(errEnvvar);
  if (injectError) {
    if (strcmp(injectError, "assert") == 0) assert(0);
    NAString reason("Error injected at ");
    reason += errEnvvar;
    reason += " - ";
    reason += details;
    *CmpCommon::diags() << DgSqlCode(-CAT_INTERNAL_EXCEPTION_ERROR) << DgString0(__FILE__) << DgInt0(__LINE__)
                        << DgString1(reason.data());

    return -1;
  }

  return 0;
}

static int32_t renameTable(const char *originalObjectName, const char *newObjectName, ExeCliInterface &cliInterface,
                           ComDiagsArea *pDiags);

// *****************************************************************************
//    PrivMgrMDAdmin class methods
// *****************************************************************************
// -----------------------------------------------------------------------
// Default Constructor
// -----------------------------------------------------------------------
PrivMgrMDAdmin::PrivMgrMDAdmin() : PrivMgr(){};

// --------------------------------------------------------------------------
// Construct a PrivMgrMDAdmin object for with a different metadata locations
// --------------------------------------------------------------------------
PrivMgrMDAdmin::PrivMgrMDAdmin(const std::string &trafMetadataLocation, const std::string &metadataLocation,
                               ComDiagsArea *pDiags)
    : PrivMgr(trafMetadataLocation, metadataLocation, pDiags){};

// -----------------------------------------------------------------------
// Construct a PrivMgrMDAdmin object for with a different metadata location
// -----------------------------------------------------------------------
PrivMgrMDAdmin::PrivMgrMDAdmin(const std::string &metadataLocation, ComDiagsArea *pDiags)
    : PrivMgr(metadataLocation, pDiags){};

// -----------------------------------------------------------------------
// Copy constructor
// -----------------------------------------------------------------------
PrivMgrMDAdmin::PrivMgrMDAdmin(const PrivMgrMDAdmin &other) : PrivMgr(other) {}

// -----------------------------------------------------------------------
// Destructor.
// -----------------------------------------------------------------------
PrivMgrMDAdmin::~PrivMgrMDAdmin() {}

// ----------------------------------------------------------------------------
// Method:  initializeComponentPrivileges
//
// This method registers standard Trafodion components, creates the
// standard operations, and grants the privilege on those operations to the
// owner (DB__ROOT), user DB__ADMIN, roles DB__ROOTROLE, DB_ADMINROLE, and
// PUBLIC.
//
// Returns PrivStatus
//    STATUS_GOOD
//    STATUS_ERROR
//
// A cli error is put into the diags area if there is an error
// ----------------------------------------------------------------------------
PrivStatus PrivMgrMDAdmin::initializeComponentPrivileges() {
  std::string traceMsg;
  log(__FILE__, "Initializing component privileges", -1);
  PrivStatus privStatus = STATUS_GOOD;

  PrivMgrComponents components(metadataLocation_, pDiags_);
  size_t numComps = sizeof(componentList) / sizeof(ComponentListStruct);
  for (int c = 0; c < numComps; c++) {
    // Get description of component
    const ComponentListStruct &compDefinition = componentList[c];
    int64_t compUID(compDefinition.componentUID);
    std::string compName(compDefinition.componentName);
    std::string compDef("System component ");
    compDef += compName;

    log(__FILE__, compDef, -1);

    bool componentExists = (components.exists(compName));
    if (!componentExists) {
      // Register component
      privStatus = components.registerComponentInternal(compName, compUID, true, compDef);
      if (privStatus != STATUS_GOOD) {
        traceMsg = "ERROR: unable to register component ";
        traceMsg += compName.c_str();
        log(__FILE__, traceMsg.c_str(), -1);
        return STATUS_ERROR;
      }
    }

    // Component is registered, now create all the operations associated with
    // the component.  A grant from the system to the owner (DB__ROOT) will
    // be added for each operation. In addition, set up the list of grants
    // for different users/roles.
    //   allOpsList - list of operations (granted to owner)
    //   rootRoleList - list of operations granted to DB__ROOTROLE
    //   adminList - list of operations granted to both DB__ADMIN and DB__ADMINROLE
    //   publicList - list of operations granted to PUBLIC
    std::vector<std::string> allOpsList;
    std::vector<std::string> rootRoleList;
    std::vector<std::string> adminList;
    std::vector<std::string> publicList;

    PrivMgrComponentPrivileges componentPrivileges(metadataLocation_, pDiags_);
    PrivMgrComponentOperations componentOperations(metadataLocation_, pDiags_);
    int32_t DB__ROOTID = ComUser::getRootUserID();
    std::string DB__ROOTName(ComUser::getRootUserName());

    int32_t numOps = compDefinition.numOps;
    int32_t numExistingOps = 0;
    int32_t numExistingUnusedOps = 0;
    if (componentOperations.getCount(compUID, numExistingOps, numExistingUnusedOps) == STATUS_ERROR)
      return STATUS_ERROR;

    // Add any new operations
    if (numExistingOps < numOps) {
      // The ComponentOpStruct describes the component operations required for
      // each component. Each entry contains the operationCode,
      // operationName, whether the privileges should be granted for
      // DB__ROOTROLE, and PUBLIC, etc.
      for (int i = 0; i < numOps; i++) {
        const ComponentOpStruct opDefinition = compDefinition.componentOps[i];

        std::string description = "Allow grantee to perform ";
        description += opDefinition.operationName;
        description += " operation";

        // create the operation
        privStatus = componentOperations.createOperationInternal(
            compUID, opDefinition.operationName, opDefinition.operationCode, opDefinition.unusedOp, description,
            DB__ROOTID, DB__ROOTName, -1, componentExists);

        if (privStatus == STATUS_GOOD) {
          // All operations are included in the allOpsList
          allOpsList.push_back(opDefinition.operationName);
          if (opDefinition.isRootRoleOp) rootRoleList.push_back(opDefinition.operationCode);
          if (opDefinition.isAdminOp) adminList.push_back(opDefinition.operationCode);
          if (opDefinition.isPublicOp) publicList.push_back(opDefinition.operationCode);
        } else {
          traceMsg = "WARNING unable to create component operation: ";
          traceMsg += opDefinition.operationName;
          log(__FILE__, traceMsg, -1);
          return privStatus;
        }
      }

      // In the unlikely event no operations were created, we are done.
      if (allOpsList.size() == 0) continue;

      // Grant all SQL_OPERATIONS to DB__ROOTROLE WITH GRANT OPTION
      privStatus =
          componentPrivileges.grantPrivilegeInternal(compUID, rootRoleList, DB__ROOTID, ComUser::getRootUserName(),
                                                     ROOT_ROLE_ID, DB__ROOTROLE, -1, componentExists);

      if (privStatus != STATUS_GOOD) {
        traceMsg = "ERROR unable to grant DB__ROOTROLE to components";
        log(__FILE__, traceMsg, -1);
        return privStatus;
      }

      // Grant component operations to DB__ADMIN user
      privStatus = componentPrivileges.grantPrivilegeInternal(
          compUID, adminList, DB__ROOTID, ComUser::getRootUserName(), ADMIN_USER_ID, DB__ADMIN, 0, componentExists);

      if (privStatus != STATUS_GOOD) {
        traceMsg = "ERROR unable to grant DB__ADMIN to components";
        log(__FILE__, traceMsg, -1);
        return privStatus;
      }

      // Grant component operations to DB__ADMINROLE role.
      CmpSeabaseDDLrole role;
      int roleID = 0;
      if (!role.getRoleIDFromRoleName(DB__ADMINROLE, roleID))
        PRIVMGR_INTERNAL_ERROR("Unable to get role_id for DB__ADMINROLE");
      privStatus = componentPrivileges.grantPrivilegeInternal(
          compUID, adminList, DB__ROOTID, ComUser::getRootUserName(), roleID, DB__ADMINROLE, 0, componentExists);

      if (privStatus != STATUS_GOOD) {
        traceMsg = "ERROR unable to grant DB__ADMINROLE to components";
        log(__FILE__, traceMsg, -1);
        return privStatus;
      }

      // Grant privileges to PUBLIC
      privStatus =
          componentPrivileges.grantPrivilegeInternal(compUID, publicList, DB__ROOTID, ComUser::getRootUserName(),
                                                     PUBLIC_USER, PUBLIC_AUTH_NAME, 0, componentExists);
      if (privStatus != STATUS_GOOD) {
        traceMsg = "ERROR unable to grant PUBLIC to components";
        log(__FILE__, traceMsg, -1);
        return privStatus;
      }
    }

    // Update component_privileges and update operation codes appropriately
    size_t numUnusedOps = PrivMgr::getSQLUnusedOpsCount();
    if (numExistingOps > 0 /* doing upgrade */ && (numUnusedOps != numExistingUnusedOps)) {
      privStatus = componentOperations.updateOperationCodes(compUID);
      if (privStatus == STATUS_ERROR) return privStatus;
    }

    // Verify counts from tables.

    // Minimum number of privileges granted is:
    //   one for each operation (owner)
    //   one for each entry in rootRoleList and publicList
    //   two for each entry in adminList (DB__ADMIN & DB__ADMINROLE)
    // This check was added because of issues with insert/upsert, is it still needed?
    int64_t expectedPrivCount = numOps + rootRoleList.size() + (adminList.size() * 2) + publicList.size();

    if (componentPrivileges.getCount(compUID) < expectedPrivCount) {
      std::string message("Expecting ");
      message += PrivMgr::UIDToString(expectedPrivCount);
      message += " component privileges, instead ";
      message += PrivMgr::authIDToString(numExistingOps);
      message += " were found.";
      traceMsg = "ERROR: ";
      traceMsg += message;
      log(__FILE__, message, -1);
      PRIVMGR_INTERNAL_ERROR(message.c_str());
      return STATUS_ERROR;
    }
  }

  // See if any operations were not created change diags to warnings
  // if (allOpsList.size() != numOps)
  //  pDiags_.negateAllErrors();
  return STATUS_GOOD;
}

// ----------------------------------------------------------------------------
// Method:  initializeMetadata
//
// This method:
//   - creates PrivMgr metadata if it does not exist
//   - adds or upgrades contents of PrivMgr metadata tables
//
// Returns PrivStatus
//    STATUS_GOOD
//    STATUS_ERROR - the diags area is populated with the error
// ----------------------------------------------------------------------------
PrivStatus PrivMgrMDAdmin::initializeMetadata(std::vector<std::string> tablesCreated, bool isUpgrade,
                                              ExeCliInterface *cliInterface) {
  std::string traceMsg;
  log(__FILE__, "*** Initialize Authorization ***", -1);

  PrivStatus retcode = STATUS_GOOD;

  // Authorization check
  if (!isAuthorized()) {
    *pDiags_ << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return STATUS_ERROR;
  }

  int cliRC = 0;
  if (cliInterface == NULL) {
    PRIVMGR_INTERNAL_ERROR("Missing required parameter");
    return STATUS_ERROR;
  }

  bool initializeMD = !isUpgrade;
  size_t numTables = sizeof(privMgrTables) / sizeof(PrivMgrTableStruct);
  bool populateObjectPrivs = false;

  // See if metadata exists
  std::string whereClause("where schema_name = '");
  whereClause += SEABASE_PRIVMGR_SCHEMA;
  whereClause += "' and object_type = 'BT'";
  int64_t rowCount;
  PrivMgrObjects objects;
  retcode = objects.selectCountWhere(whereClause, rowCount);
  if (retcode == STATUS_ERROR) return STATUS_ERROR;
  if (rowCount < numTables) initializeMD = true;

  if (initializeMD) {
    log(__FILE__, "Creating _PRIVMGR_MD_ schema", -1);

    std::string schemaCommand("CREATE PRIVATE SCHEMA IF NOT EXISTS ");

    // always add a namespace
    schemaCommand += metadataLocation_;
    schemaCommand += " NAMESPACE '";
    schemaCommand += TRAF_RESERVED_NAMESPACE1;
    schemaCommand += "'";

    cliRC = cliInterface->executeImmediate(schemaCommand.c_str());
    if (cliRC < 0) return STATUS_ERROR;

    // Create the tables
    log(__FILE__, "Creating _PRIVMGR_MD_ tables", -1);
    for (int ndx_tl = 0; ndx_tl < numTables; ndx_tl++) {
      const PrivMgrTableStruct &tableDefinition = privMgrTables[ndx_tl];
      std::string tableName = deriveTableName(tableDefinition.tableName);

      traceMsg = ": ";
      traceMsg = tableName;
      log(__FILE__, traceMsg, -1);

      // only creates the table IF NOT EXISTS
      cliRC = createTable(tableName.c_str(), tableDefinition.tableDDL, cliInterface, pDiags_);

      if (cliRC < 0) {
        log(__FILE__, " create failed", -1);
        return STATUS_ERROR;
      }
      tablesCreated.push_back(tableDefinition.tableName);

      if (tableDefinition.tableName == PRIVMGR_OBJECT_PRIVILEGES) populateObjectPrivs = true;
    }
  }

  // update contents of metadata tables
  if (updatePrivMgrMetadata(populateObjectPrivs) == STATUS_ERROR) return STATUS_ERROR;

  // Grant privileges to default roles
  if (initializeMD) {
    int roleID = NA_UserIdDefault;
    PrivMgrRoles roles;
    CmpSeabaseDDLrole auth;
    if (!auth.getRoleIDFromRoleName(DB__ADMINROLE, roleID)) {
      PRIVMGR_INTERNAL_ERROR("Unexpected error trying to retrieve ID for DB__ADMINROLE");
      return STATUS_ERROR;
    }

    if (roles.grantRoleToCreator(roleID, DB__ADMINROLE, ROOT_USER_ID, DB__ROOT, ADMIN_USER_ID, DB__ADMIN) ==
        STATUS_ERROR)
      return STATUS_ERROR;

    if (!auth.getRoleIDFromRoleName(DB__LIBMGRROLE, roleID)) {
      PRIVMGR_INTERNAL_ERROR("Unexpected error trying to retrieve ID for DB__LIBMGRROLE");
      return STATUS_ERROR;
    }

    if (roles.grantRoleToCreator(roleID, DB__LIBMGRROLE, ROOT_USER_ID, DB__ROOT, ADMIN_USER_ID, DB__ADMIN) ==
        STATUS_ERROR)
      return STATUS_ERROR;
  }

  log(__FILE__, "*** Initialize authorization completed ***", -1);
  return STATUS_GOOD;
}

// ----------------------------------------------------------------------------
// Method:  dropMetadata
//
// This method drops the metadata tables used by privilege management
//
// Returns PrivStatus
//    STATUS_GOOD
//    STATUS_WARNING
//    STATUS_NOTFOUND
//    STATUS_ERROR
//
// A cli error is put into the diags area if there is an error
// ----------------------------------------------------------------------------
PrivStatus PrivMgrMDAdmin::dropMetadata(const std::vector<std::string> &objectsToDrop, bool doCleanup) {
  std::string traceMsg;
  log(__FILE__, "*** Drop Authorization ***", -1);
  PrivStatus retcode = STATUS_GOOD;

  // Authorization check
  if (!isAuthorized()) {
    *pDiags_ << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return STATUS_ERROR;
  }

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  int cliRC = 0;
  if (doCleanup)
    cleanupMetadata(cliInterface);
  else {
    // See what does and does not exist
    std::set<std::string> existingObjectList;
    PrivMDStatus initStatus = authorizationEnabled(existingObjectList);

    // If unable to access metadata, return STATUS_ERROR
    //   (pDiags contains error details)
    if (initStatus == PRIV_INITIALIZE_UNKNOWN) {
      log(__FILE__, "ERROR: unable to access PRIVMGR metadata", -1);
      return STATUS_ERROR;
    }

    // If metadata tables don't exist, just return STATUS_GOOD
    if (initStatus == PRIV_UNINITIALIZED) {
      log(__FILE__, "WARNING: authorization is not enabled", -1);
      return STATUS_GOOD;
    }
  }

  // Call Trafodion to drop the schema cascade

  log(__FILE__, "dropping _PRIVMGR_MD_ schema cascade", -1);
  std::string schemaDDL("DROP SCHEMA IF EXISTS ");
  schemaDDL += metadataLocation_;
  schemaDDL += "CASCADE";
  cliRC = cliInterface.executeImmediate(schemaDDL.c_str());
  if (cliRC < 0) {
    traceMsg = "ERROR unable to drop schema cascade: ";
    traceMsg += to_string((long long int)cliRC);
    log(__FILE__, traceMsg, -1);
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    retcode = STATUS_ERROR;
  }

  CmpSeabaseDDLrole role;
  std::vector<std::string> rolesCreated;
  int32_t numberRoles = sizeof(systemRoles) / sizeof(SystemAuthsStruct);
  for (int32_t i = 0; i < numberRoles; i++) {
    const SystemAuthsStruct &roleDefinition = systemRoles[i];

    // special Auth includes roles that are not registered in the metadata
    if (roleDefinition.isSpecialAuth) continue;

    role.dropStandardRole(roleDefinition.authName);
  }

  int32_t actualSize = 0;
  char buf[500];
  if (ComUser::getSystemRoleList(buf, actualSize, 500) == -1) {
    std::string message("Unable to retrieve list of system roles");
    PRIVMGR_INTERNAL_ERROR(message.c_str());
    return STATUS_ERROR;
  }

  buf[actualSize] = 0;
  traceMsg = "dropped roles: ";
  traceMsg + buf;
  log(__FILE__, traceMsg, -1);

  // TODO: should notify QI
  log(__FILE__, "*** drop authorization completed ***", -1);
  return retcode;
}

// ----------------------------------------------------------------------------
// Method:  cleanupMetadata
//
// This method cleans up the metadata tables used by privilege management
//
// Error messages are expected, so they are suppressed
// ----------------------------------------------------------------------------
void PrivMgrMDAdmin::cleanupMetadata(ExeCliInterface &cliInterface) {
  std::string traceMsg;
  log(__FILE__, "cleaning up PRIVMGR tables: ", -1);

  // cleanup histogram tables, if they exist
  std::vector<std::string> histTables = CmpSeabaseDDL::getHistogramTables();
  int numHistTables = histTables.size();
  for (int i = 0; i < numHistTables; i++) {
    cleanupTable(histTables[i].c_str(), cliInterface, pDiags_);
  }

  size_t numTables = sizeof(privMgrTables) / sizeof(PrivMgrTableStruct);
  for (int ndx_tl = 0; ndx_tl < numTables; ndx_tl++) {
    const PrivMgrTableStruct &tableDefinition = privMgrTables[ndx_tl];
    std::string tableName = deriveTableName(tableDefinition.tableName);
    log(__FILE__, tableName, -1);
    cleanupTable(tableName.c_str(), cliInterface, pDiags_);
  }
}

// ----------------------------------------------------------------------------
// Method:  isAuthorized
//
// This method verifies that the current user is able to initialize or
// drop privilege manager metadata.  Currently this is restricted to the
// root database user, but in the future an operator or service ID may have
// the authority.
//
// Returns true if user is authorized
// ----------------------------------------------------------------------------
bool PrivMgrMDAdmin::isAuthorized(void) { return ComUser::isRootUserID(); }

// ****************************************************************************
// method:  getColumnReferences
//
//  This method stores the list of columns for the object in the
//  ObjectReference.
// ****************************************************************************
PrivStatus PrivMgrMDAdmin::getColumnReferences(ObjectReference *objectRef) {
  std::string colMDTable = trafMetadataLocation_ + ".COLUMNS c";

  // Select column details for object
  std::string selectStmt = "select c.column_number from ";
  selectStmt += colMDTable;
  selectStmt += " where c.object_uid = ";
  selectStmt += UIDToString(objectRef->objectUID);
  selectStmt += " order by column_number";

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Queue *objectsQueue = NULL;

  int32_t cliRC = cliInterface.fetchAllRows(objectsQueue, (char *)selectStmt.c_str(), 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return STATUS_ERROR;
  }

  if (cliRC == 100)  // did not find the row
  {
    std::string message("No columns found for referenced object");
    PRIVMGR_INTERNAL_ERROR(message.c_str());
    return STATUS_ERROR;
  }

  char *ptr = NULL;
  int len = 0;

  objectRef->columnReferences = new std::vector<ColumnReference *>;

  // For each row, create a ColumnReference and add it to the objectRef
  objectsQueue->position();
  for (int idx = 0; idx < objectsQueue->numEntries(); idx++) {
    OutputInfo *pCliRow = (OutputInfo *)objectsQueue->getNext();
    ColumnReference *columnReference = new ColumnReference;

    // column 0:  columnNumber
    pCliRow->get(0, ptr, len);
    columnReference->columnOrdinal = *(reinterpret_cast<int32_t *>(ptr));

    objectRef->columnReferences->push_back(columnReference);
  }
  return STATUS_GOOD;
}

// ****************************************************************************
// method:  getViewColUsages
//
//  This method reads the TEXT table to obtain the view-col <=> referenced-col
//  relationship.
//
//  This relationship is stored in one or more text records with the text_type
//  COM_VIEW_REF_COLS_TEXT (8) see ComSmallDefs.h
//
//  The text rows are concatenated together and saved in the ViewUsage.
// ****************************************************************************
PrivStatus PrivMgrMDAdmin::getViewColUsages(ViewUsage &viewUsage) {
  std::string textMDTable = trafMetadataLocation_ + ".TEXT t";

  // Select text rows describing view <=> object column relationships
  std::string selectStmt = "select text from ";
  selectStmt += textMDTable;
  selectStmt += " where t.text_uid = ";
  selectStmt += UIDToString(viewUsage.viewUID);
  selectStmt += "and t.text_type = 8";
  selectStmt += " order by seq_num";

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Queue *objectsQueue = NULL;

  int32_t cliRC = cliInterface.fetchAllRows(objectsQueue, (char *)selectStmt.c_str(), 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return STATUS_ERROR;
  }

  // View prior to column privilege support do not store view <=> object column
  // relationships.  These views were not created based on column privileges
  // so just return.
  if (cliRC == 100)  // did not find the row
  {
    return STATUS_NOTFOUND;
  }

  char *ptr = NULL;
  int len = 0;

  // the text column length in the TEXT table is 10000
  char value[10000 + 1];

  // For each row, add it to the existing viewColUsages string
  objectsQueue->position();
  for (int idx = 0; idx < objectsQueue->numEntries(); idx++) {
    OutputInfo *pCliRow = (OutputInfo *)objectsQueue->getNext();

    // column 0: text
    pCliRow->get(0, ptr, len);
    strncpy(value, ptr, len);
    value[len] = 0;
    viewUsage.viewColUsagesStr += value;
  }

  return STATUS_GOOD;
}

// ****************************************************************************
// method:  getViewsThatReferenceObject
//
//  this method gets the list of views associated with the passed in
//  objectUID that are owned by the granteeID.
// ****************************************************************************
PrivStatus PrivMgrMDAdmin::getViewsThatReferenceObject(const ObjectUsage &objectUsage,
                                                       std::vector<ViewUsage> &viewUsages) {
  std::string objectMDTable = trafMetadataLocation_ + ".OBJECTS o";
  std::string viewUsageMDTable = trafMetadataLocation_ + ".VIEWS_USAGE u";
  std::string viewsMDTable = trafMetadataLocation_ + ".VIEWS v";
  std::string roleUsageMDTable = metadataLocation_ + ".ROLE_USAGE";

  // Select all the views that are referenced by the table or view owned by the granteeID
  std::string selectStmt =
      "select o.object_uid, o.object_owner, o.catalog_name, o.schema_name, o.object_name, v.is_insertable, "
      "v.is_updatable from ";
  selectStmt += objectMDTable;
  selectStmt += ", ";
  selectStmt += viewUsageMDTable;
  selectStmt += ", ";
  selectStmt += viewsMDTable;
  selectStmt += " where o.object_type = 'VI' and u.used_object_uid = ";
  selectStmt += UIDToString(objectUsage.objectUID);
  selectStmt += " and u.using_view_uid = o.object_uid ";
  selectStmt += "and o.object_uid = v.view_uid";

  // only return rows where user owns the view either directly or through one of
  // their granted roles
  selectStmt += " and (o.object_owner = ";
  selectStmt += UIDToString(objectUsage.granteeID);
  selectStmt += "  or o.object_owner in (select role_id from ";
  selectStmt += roleUsageMDTable;
  selectStmt += " where grantee_id = ";
  selectStmt += UIDToString(objectUsage.granteeID);

  // for role owners, get list of users granted role
  selectStmt += " ) or o.object_owner in (select grantee_id from ";
  selectStmt += roleUsageMDTable;
  selectStmt += " where role_id = ";
  selectStmt += UIDToString(objectUsage.granteeID);

  selectStmt += ")) order by o.create_time ";

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Queue *objectsQueue = NULL;

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  int32_t cliRC = cliInterface.fetchAllRows(objectsQueue, (char *)selectStmt.c_str(), 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return STATUS_ERROR;
  }

  if (cliRC == 100)  // did not find the row
  {
    pDiags_->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  char *ptr = NULL;
  int len = 0;
  char value[MAX_SQL_IDENTIFIER_NAME_LEN + 1];

  // For each row returned, add it to the viewUsages structure.
  objectsQueue->position();
  for (int idx = 0; idx < objectsQueue->numEntries(); idx++) {
    OutputInfo *pCliRow = (OutputInfo *)objectsQueue->getNext();
    ViewUsage viewUsage;

    // column 0:  object uid
    pCliRow->get(0, ptr, len);
    viewUsage.viewUID = *(reinterpret_cast<int64_t *>(ptr));

    // column 1: object owner
    pCliRow->get(1, ptr, len);
    viewUsage.viewOwner = *(reinterpret_cast<int32_t *>(ptr));

    // column 2: catalog name
    pCliRow->get(2, ptr, len);
    assert(len < 257);
    strncpy(value, ptr, len);
    value[len] = 0;
    std::string viewName(value);
    viewName += ".";

    // column 3:  schema name
    pCliRow->get(3, ptr, len);
    assert(len < 257);
    strncpy(value, ptr, len);
    value[len] = 0;
    viewName += value;
    viewName += ".";

    // column 4:  object name
    pCliRow->get(4, ptr, len);
    assert(len < 257);
    strncpy(value, ptr, len);
    value[len] = 0;
    viewName += value;
    viewUsage.viewName = viewName;

    // column 5: is insertable
    pCliRow->get(5, ptr, len);
    viewUsage.isInsertable = (*ptr == 0) ? false : true;

    // column 6: is updatable
    pCliRow->get(6, ptr, len);
    viewUsage.isUpdatable = (*ptr == 0) ? false : true;

    viewUsages.push_back(viewUsage);
  }

  return STATUS_GOOD;
}

// ****************************************************************************
// method:  getObjectsThatViewReferences
//
//  this method gets the list of objects (tables or views) that are referenced
//  by the view.
// ****************************************************************************
PrivStatus PrivMgrMDAdmin::getObjectsThatViewReferences(const ViewUsage &viewUsage,
                                                        std::vector<ObjectReference *> &objectReferences) {
  std::string objectMDTable = trafMetadataLocation_ + ".OBJECTS o";
  std::string viewUsageMDTable = trafMetadataLocation_ + ".VIEWS_USAGE u";

  // Select all the objects that are referenced by the view
  std::string selectStmt = "select o.object_uid, o.object_owner, o.object_type, ";
  selectStmt += "trim(o.catalog_name) || '.\"' || ";
  selectStmt += "trim (o.schema_name) || '\".\"' || ";
  selectStmt += "trim (o.object_name) || '\"' from ";
  selectStmt += objectMDTable;
  selectStmt += ", ";
  selectStmt += viewUsageMDTable;
  selectStmt += " where u.using_view_uid = ";
  selectStmt += UIDToString(viewUsage.viewUID);
  selectStmt += " and u.used_object_uid = o.object_uid ";
  selectStmt += " order by o.create_time ";

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Queue *objectsQueue = NULL;

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  int32_t cliRC = cliInterface.fetchAllRows(objectsQueue, (char *)selectStmt.c_str(), 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return STATUS_ERROR;
  }

  if (cliRC == 100)  // did not find the row
  {
    pDiags_->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  char *ptr = NULL;
  int len = 0;
  char value[MAX_SQL_IDENTIFIER_NAME_LEN + 1];

  // For each row returned, add it to the viewUsages structure.
  objectsQueue->position();
  for (int idx = 0; idx < objectsQueue->numEntries(); idx++) {
    OutputInfo *pCliRow = (OutputInfo *)objectsQueue->getNext();
    ObjectReference *pObjectReference = new ObjectReference;

    // column 0:  object uid
    pCliRow->get(0, ptr, len);
    pObjectReference->objectUID = *(reinterpret_cast<int64_t *>(ptr));

    // column 1: object owner
    pCliRow->get(1, ptr, len);
    pObjectReference->objectOwner = *(reinterpret_cast<int32_t *>(ptr));

    // column 2: object type
    pCliRow->get(2, ptr, len);
    strncpy(value, ptr, len);
    value[len] = 0;
    pObjectReference->objectType = ObjectLitToEnum(value);

    // column 3: object name
    pCliRow->get(3, ptr, len);
    strncpy(value, ptr, len);
    value[len] = 0;
    pObjectReference->objectName = value;

    objectReferences.push_back(pObjectReference);
  }

  return STATUS_GOOD;
}

// ****************************************************************************
// method:  getUdrsThatReferenceLibrary
//
// This method gets the list of objects (functions & procedures) that are
// referenced by the library.
//
// ****************************************************************************
PrivStatus PrivMgrMDAdmin::getUdrsThatReferenceLibrary(const ObjectUsage &objectUsage,
                                                       std::vector<ObjectReference *> &objectReferences) {
  std::string objectMDTable = trafMetadataLocation_ + ".OBJECTS o";
  std::string librariesUsageMDTable = trafMetadataLocation_ + ".LIBRARIES_USAGE u";
  std::string roleUsageMDTable = metadataLocation_ + ".ROLE_USAGE r";

  // Select all the objects that are referenced by the library
  std::string selectStmt = "select o.object_uid, o.object_owner, o.object_type, ";
  selectStmt += "trim(o.catalog_name) || '.\"' || ";
  selectStmt += "trim (o.schema_name) || '\".\"' ||";
  selectStmt += "trim (o.object_name)|| '\"' from ";
  selectStmt += objectMDTable;
  selectStmt += ", ";
  selectStmt += librariesUsageMDTable;
  selectStmt += " where u.using_library_uid = ";
  selectStmt += UIDToString(objectUsage.objectUID);
  selectStmt += " and u.used_udr_uid = o.object_uid ";
  selectStmt += " and (o.object_owner = ";
  selectStmt += UIDToString(objectUsage.granteeID);
  selectStmt += "  or o.object_owner in (select role_id from ";
  selectStmt += roleUsageMDTable;
  selectStmt += " where grantee_id = ";
  selectStmt += UIDToString(objectUsage.granteeID);
  selectStmt += ")) order by o.create_time ";

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Queue *objectsQueue = NULL;

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  int32_t cliRC = cliInterface.fetchAllRows(objectsQueue, (char *)selectStmt.c_str(), 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return STATUS_ERROR;
  }

  if (cliRC == 100)  // did not find the row
  {
    pDiags_->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  char *ptr = NULL;
  int len = 0;
  char value[MAX_SQL_IDENTIFIER_NAME_LEN + 1];

  // For each row returned, add it to the objectReferences structure.
  objectsQueue->position();
  for (int idx = 0; idx < objectsQueue->numEntries(); idx++) {
    OutputInfo *pCliRow = (OutputInfo *)objectsQueue->getNext();
    ObjectReference *pObjectReference = new ObjectReference;

    // column 0:  object uid
    pCliRow->get(0, ptr, len);
    pObjectReference->objectUID = *(reinterpret_cast<int64_t *>(ptr));

    // column 1: object owner
    pCliRow->get(1, ptr, len);
    pObjectReference->objectOwner = *(reinterpret_cast<int32_t *>(ptr));

    // column 2: object type
    pCliRow->get(2, ptr, len);
    strncpy(value, ptr, len);
    value[len] = 0;
    pObjectReference->objectType = ObjectLitToEnum(value);

    // column 3: object name
    pCliRow->get(3, ptr, len);
    strncpy(value, ptr, len);
    value[len] = 0;
    pObjectReference->objectName = value;

    objectReferences.push_back(pObjectReference);
  }

  return STATUS_GOOD;
}

// ****************************************************************************
//
// method:  getReferencingTablesForConstraints
//
// This method returns the list of underlying tables and their columns that are
// associated with RI constraints referencing an ObjectUsage.
//
// An RI constraint is a relationship between a set of columns on one table
// (the referencing table) with a set of columns on another table (the
// referenced table).  The set of columns must be defined as an unique
// constraint (which include primary key constraints) on the referenced table.
// Relationships are stored through the constraints not their underlying tables.
//
//  for example:
//    user1:
//      create table dept (dept_no int not null primary key, ... );
//      grant references on dept to user2;
//
//    user2:
//      create table empl (empl_no int not null primary key, dept_no int, ... );
//      alter table empl
//          add constraint empl_dept foreign key(dept_no) references dept;
//
//  empl_dept is the name of the RI constraint
//     The empl table references dept
//     The dept table is being referenced by empl
//
//  The following query is called to get the list of tables and their columns:
//    <Gets underlying table and columns for the foreign key's constraint>
//    select (for referenced table)
//       objects (o): object_uid, object_owner, object_type, create_time, name,
//       list of columns (c): column_number
//    from table_constraints t, objects o, unique_ref_constr_usage u
//         (list of foreign key/unique constraint uids on referenced table) r,
//         (list of column numbers on referenced table's unique constraints) c
//    where o.object_uid = t.table_uid
//      and t.constraint_uid = r.foreign_constraint_uid
//      and r.unique_constraint_uid = c.object_uid
//    order by object_owner & create time
//
//    <Get list of foreign key/unique constraint uids on reference table>
//    (select foreign_constraint_uid, unique_constraint_uid
//     from table_constraints t, unique_ref_constr_usage u
//     where t.table_uid = objectUsage.objectUID
//       and t.constraint_uid = u.unique_constraint_uid)
//     order by o.create_time
//
//    <Get list of column numbers on referenced table's unique constraints>
//    (select object_uid, column_number, column_name
//     from TRAFODION."_MD_".KEYS
//     where object_uid in
//       (select unique_constraint_uid
//        from TABLE_CONSTRAINTS t,UNIQUE_REF_CONSTR_USAGE u
//        where t.table_uid = objectUsage.objectUID
//          and t.constraint_uid = u.unique_constraint_uid))
//
// input:  ObjectUsage - object desiring list of referencing tables
//         In the example above, this would be the DEPT table
//
// output: std::vector<ObjectReference *> - list of table references describing
//         the underlying table of one or more referencing constraints
//         In the example above, EMPL would be returned
//
// ****************************************************************************
PrivStatus PrivMgrMDAdmin::getReferencingTablesForConstraints(const ObjectUsage &objectUsage,
                                                              std::vector<ObjectReference *> &objectReferences) {
  std::string objectsMDTable = trafMetadataLocation_ + ".OBJECTS o";
  std::string tblConstraintsMDTable = trafMetadataLocation_ + ".TABLE_CONSTRAINTS t";
  std::string keysMDTable = trafMetadataLocation_ + ".KEYS k";
  std::string uniqueRefConstraintsMDTable = trafMetadataLocation_ + ".UNIQUE_REF_CONSTR_USAGE u";

  // Select all the constraints that are referenced by the table
  std::string selectStmt = "select distinct o.object_uid, o.object_owner, o.object_type, o.create_time, ";
  selectStmt += "trim(o.catalog_name) || '.\"' || ";
  selectStmt += "trim (o.schema_name) || '\".\"' ||";
  selectStmt += "trim (o.object_name)|| '\"' ";
  selectStmt += ", c.column_number ";
  selectStmt += "from " + uniqueRefConstraintsMDTable + std::string(", ");
  selectStmt += tblConstraintsMDTable + std::string(", ") + objectsMDTable;
  selectStmt += " , (select foreign_constraint_uid, unique_constraint_uid from ";
  selectStmt += tblConstraintsMDTable + std::string(", ") + uniqueRefConstraintsMDTable;
  selectStmt += " where t.table_uid = " + UIDToString(objectUsage.objectUID);
  selectStmt += " and t.constraint_uid = u.unique_constraint_uid) r ";
  selectStmt += " , (select object_uid, column_number, column_name from ";
  selectStmt += keysMDTable;
  selectStmt += " where object_uid in (select unique_constraint_uid from ";
  selectStmt += tblConstraintsMDTable + std::string(", ");
  selectStmt += uniqueRefConstraintsMDTable + std::string(" where t.table_uid = ");
  selectStmt += UIDToString(objectUsage.objectUID);
  selectStmt += " and t.constraint_uid = u.unique_constraint_uid)) c ";
  selectStmt += "where o.object_uid = t.table_uid ";
  selectStmt += " and t.constraint_uid = r.foreign_constraint_uid ";
  selectStmt += " and r.unique_constraint_uid = c.object_uid ";
  selectStmt += " order by o.object_owner, o.create_time ";

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Queue *objectsQueue = NULL;

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  int32_t cliRC = cliInterface.fetchAllRows(objectsQueue, (char *)selectStmt.c_str(), 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return STATUS_ERROR;
  }

  if (cliRC == 100)  // did not find the row
  {
    pDiags_->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  char *ptr = NULL;
  int len = 0;
  char value[MAX_SQL_IDENTIFIER_NAME_LEN + 1];
  objectsQueue->position();

  std::vector<ColumnReference *> *columnReferences = new std::vector<ColumnReference *>;
  ObjectReference *pObjectReference(NULL);
  ColumnReference *pColumnReference(NULL);
  int64_t currentObjectUID = 0;

  // Set up an objectReference for any objects found, the caller manages
  // space for this list
  for (int idx = 0; idx < objectsQueue->numEntries(); idx++) {
    OutputInfo *pCliRow = (OutputInfo *)objectsQueue->getNext();

    // column 0:  object uid
    pCliRow->get(0, ptr, len);
    int64_t nextObjectUID = *(reinterpret_cast<int64_t *>(ptr));

    // If a new object uid then save previous object reference
    // and prepare for new object reference
    if (nextObjectUID != currentObjectUID) {
      // Save off previous reference, if previous reference exists
      if (currentObjectUID > 0) {
        pObjectReference->columnReferences = columnReferences;
        objectReferences.push_back(pObjectReference);
      }
      currentObjectUID = nextObjectUID;

      // prepare for new object reference
      pObjectReference = new ObjectReference;
      columnReferences = new std::vector<ColumnReference *>;

      // object UID
      pObjectReference->objectUID = nextObjectUID;

      // column 1: object owner
      pCliRow->get(1, ptr, len);
      pObjectReference->objectOwner = *(reinterpret_cast<int32_t *>(ptr));

      // column 2: object type
      pCliRow->get(2, ptr, len);
      strncpy(value, ptr, len);
      value[len] = 0;
      pObjectReference->objectType = ObjectLitToEnum(value);

      // skip create_time (column 3)

      // column 4: object name
      pCliRow->get(4, ptr, len);
      strncpy(value, ptr, len);
      value[len] = 0;
      pObjectReference->objectName = value;
    }

    // set up the column reference
    // column 5: column number
    ColumnReference *pColumnReference = new ColumnReference;
    pCliRow->get(5, ptr, len);
    pColumnReference->columnOrdinal = *(reinterpret_cast<int32_t *>(ptr));

    // add to column list
    columnReferences->push_back(pColumnReference);
  }

  //  Add the final object reference to list
  pObjectReference->columnReferences = columnReferences;
  objectReferences.push_back(pObjectReference);

  return STATUS_GOOD;
}

// ****************************************************************************
// method:  getConstraintName
//
// This method returns the the name of the first referenced constraint involved
// with a foreign key constraint between two tables (the referenced table and
// the referencing table)
//
// This method is only called to obtain the constraint name to include in an
// error message.
//
//  assuming the example from getReferencingTablesForConstraints
//    user1:
//      create table dept (dept_no int not null primary key, ... );
//      grant references on dept to user2;
//
//    user2:
//      create table empl (empl_no int not null primary key, dept_no int, ... );
//      alter table empl add constraint empl_dept foreign key(dept_no) references dept;
//
// This method returns the constraint named empl_dept
//
// The following query is called to get the constraint name:
//
// select [first 1] object_name as referenced_constraint_name
//   from objects o, table_constraints t, keys k
//   where o.object_uid = t.constraint_uid
//     and t.table_uid = <referencing table UID>
//     and t.constraint_uid in
//        (select ref_constraint_uid from ref_constraints
//         where unique_constraint_uid in
//            (select constraint_uid from table_constraints
//             where t.table_uid = <referenced table uid>
//               and constraint_uid = k.object_uid
//               and k.column_number = <column number>))
//
// input:   referencedTableUID - the referenced table
//          in the example above, the is the DEPT table UID
//
//          referencingTableUID - the referencing table
//          in the example above, this is the EMPL table UID
//
// returns:  true, found constraint name
//           false, unable to get constraint name
//
//           This method does not clear out the ComDiags area in case of
//           a failure.
//
// ****************************************************************************
bool PrivMgrMDAdmin::getConstraintName(const int64_t referencedTableUID, const int64_t referencingTableUID,
                                       const int32_t columnNumber, std::string &constraintName) {
  std::string objectsMDTable = trafMetadataLocation_ + ".OBJECTS o";
  std::string tblConstraintsMDTable = trafMetadataLocation_ + ".TABLE_CONSTRAINTS t";
  std::string refConstraintsMDTable = trafMetadataLocation_ + ".REF_CONSTRAINTS u";
  std::string keysMDTable = trafMetadataLocation_ + ".KEYS k";

  // select object_name based on passed in object UID
  std::string quote("\"");
  std::string selectStmt = "select [first 1] ";
  selectStmt += "trim(o.catalog_name) || '.\"' || ";
  selectStmt += "trim (o.schema_name) || '\".\"' ||";
  selectStmt += "trim (o.object_name)|| '\"' from ";
  selectStmt += objectsMDTable + ", " + tblConstraintsMDTable;
  selectStmt += " where o.object_uid = t.constraint_uid";
  selectStmt += " and t.table_uid = " + UIDToString(referencingTableUID);
  selectStmt += " and t.constraint_uid in (select ref_constraint_uid from ";
  selectStmt += refConstraintsMDTable + ", " + keysMDTable;
  selectStmt += " where unique_constraint_uid in ";
  selectStmt += " (select constraint_uid from " + tblConstraintsMDTable;
  selectStmt += " where t.table_uid = " + UIDToString(referencedTableUID);
  selectStmt += ") and k.column_number = " + UIDToString(columnNumber);
  selectStmt += " and unique_constraint_uid = k.object_uid) ";

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());
  Queue *objectsQueue = NULL;

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  int32_t cliRC = cliInterface.fetchAllRows(objectsQueue, (char *)selectStmt.c_str(), 0, FALSE, FALSE, TRUE);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(pDiags_);
    return false;
  }

  // did not find a row, or found too many rows
  if (cliRC == 100) {
    pDiags_->rewind(diagsMark);
    return false;
  }

  // Return the constraint name.
  char *ptr = NULL;
  int len = 0;
  char value[MAX_SQL_IDENTIFIER_NAME_LEN + 1];
  objectsQueue->position();
  OutputInfo *pCliRow = (OutputInfo *)objectsQueue->getNext();

  // column 0: object name
  pCliRow->get(0, ptr, len);
  strncpy(value, ptr, len);
  value[len] = 0;
  constraintName += std::string(value);

  return true;
}

// ----------------------------------------------------------------------------
// Method:  createTable
//
// This method creates a table
//
// Params:
//   tableName - fully qualified name of table to create
//   tableDDL - a QString describing the table
//   cliInterface - a reference to the CLI interface
//   pDiags - pointer to the diags area
//
// Returns:
//   The cli code returned from the create statement
//
// A cli error is put into the diags area if there is an error
// ----------------------------------------------------------------------------
int32_t createTable(const char *tableName, const QString *tableDDL, ExeCliInterface *cliInterface,
                    ComDiagsArea *pDiags) {
  std::string createStmt("CREATE TABLE IF NOT EXISTS ");
  createStmt += tableName;
  createStmt += tableDDL->str;

  int cliRC = cliInterface->executeImmediate(createStmt.c_str());
  if (cliRC != 0) cliInterface->retrieveSQLDiagnostics(pDiags);

  return cliRC;
}

// ----------------------------------------------------------------------------
// Method:  dropTable
//
// This method drops a table
//
// Params:
//   tableName - fully qualified name of table to drop
//   cliInterface - a reference to the CLI interface
//   pDiags - pointer to the diags area
//
// Returns:
//   The cli code returned from the drop statement
//
// A cli error is put into the diags area if there is an error
// ----------------------------------------------------------------------------
static int32_t dropTable(const char *tableName, ExeCliInterface &cliInterface, ComDiagsArea *pDiags) {
  std::string tableDDL("DROP TABLE IF EXISTS ");
  tableDDL += tableName;

  int cliRC = cliInterface.executeImmediate(tableDDL.c_str());
  if (cliRC < 0) cliInterface.retrieveSQLDiagnostics(pDiags);

  return cliRC;
}

// ----------------------------------------------------------------------------
// Method:  cleanupTable
//
// This method removes a table through the brute force "cleanup" method.
//
// Params:
//   tableName - fully qualified name of table to drop
//   cliInterface - a reference to the CLI interface
//   pDiags - pointer to the diags area
//
// This method is called when metadata is corrupt so errors are expected.
// Error messages are suppressed.
// ----------------------------------------------------------------------------
static void cleanupTable(const char *tableName, ExeCliInterface &cliInterface, ComDiagsArea *pDiags) {
  std::string tableDDL("CLEANUP TABLE ");
  tableDDL += tableName;
  int32_t diagsMark = pDiags->mark();
  int cliRC = cliInterface.executeImmediate(tableDDL.c_str());
  if (cliRC < 0) pDiags->rewind(diagsMark);
}

// ----------------------------------------------------------------------------
// Method:  renameTable
//
// This method renames a table
//
// Params:
//   originalObjectName - fully qualified name of object to rename
//   newObjectName - fully qualified new name
//   cliInterface - a reference to the CLI interface
//   pDiags - pointer to the diags area
//
// Returns:
//   The cli code returned from the rename statement
//
// A cli error is put into the diags area if there is an error
// ----------------------------------------------------------------------------
static int32_t renameTable(const char *originalObjectName, const char *newObjectName, ExeCliInterface &cliInterface,
                           ComDiagsArea *pDiags) {
  std::string tableDDL("ALTER TABLE  ");
  tableDDL += newObjectName;
  tableDDL += " RENAME TO ";
  tableDDL += originalObjectName;

  int cliRC = cliInterface.executeImmediate(tableDDL.c_str());
  if (cliRC < 0) cliInterface.retrieveSQLDiagnostics(pDiags);

  return cliRC;
}

// ****************************************************************************
// method:  updatePrivMgrMetadata
//
//  This method updates the contents of privilege manager metadata. It is
//  called during installation and upgrade.
//
// ****************************************************************************
PrivStatus PrivMgrMDAdmin::updatePrivMgrMetadata(const bool shouldPopulateObjectPrivs) {
  std::string traceMsg;
  PrivStatus privStatus = STATUS_GOOD;

  // This is called to add owner privileges to any existing objects
  // to the OBJECT_PRIVILEGES table.  This is called during initialization.
  std::string objectsLocation = getTrafMetadataLocation() + "." + SEABASE_OBJECTS;
  std::string authsLocation = getTrafMetadataLocation() + "." + SEABASE_AUTHS;
  PrivMgrPrivileges objectPrivs(metadataLocation_, pDiags_);
  if (shouldPopulateObjectPrivs) {
    privStatus = objectPrivs.populateObjectPriv(objectsLocation, authsLocation);
    if (privStatus != STATUS_GOOD && privStatus != STATUS_NOTFOUND) return STATUS_ERROR;
  }

  // Create any roles.  If this is an upgrade operation, some roles may
  // already exist, just create any new roles. If this is an initialize
  // operation, than all system roles are created.
  CmpSeabaseDDLrole role;
  std::vector<std::string> rolesCreated;
  int32_t numberRoles = sizeof(systemRoles) / sizeof(SystemAuthsStruct);
  for (int32_t i = 0; i < numberRoles; i++) {
    const SystemAuthsStruct &roleDefinition = systemRoles[i];

    // special Auth includes roles that are not registered in the metadata
    if (roleDefinition.isSpecialAuth) continue;

    // returns true if role was created, false if it already existed
    if (role.createStandardRole(roleDefinition.authName, roleDefinition.authID))
      rolesCreated.push_back(roleDefinition.authName);
  }

  // Report the number roles created
  traceMsg = "created roles ";
  char buf[MAX_AUTHNAME_LEN + 5];
  char sep = ' ';
  for (size_t i = 0; i < rolesCreated.size(); i++) {
    sprintf(buf, "%c'%s' ", sep, rolesCreated[i].c_str());
    traceMsg.append(buf);
    sep = ',';
  }
  log(__FILE__, traceMsg, -1);

  if (rolesCreated.size() > 0) {
    PrivMgrRoles role(" ", metadataLocation_, pDiags_);

    privStatus = role.populateCreatorGrants(authsLocation, rolesCreated);
    if (privStatus != STATUS_GOOD) return STATUS_ERROR;
  }

  // If someone initializes authorization, creates some roles, then drops
  // authorization, these roles exist in the system metadata (e.g. AUTHS table)
  // but all usages are lost, including the initial creator grants.
  // See if there are any roles that exist in AUTHS but do not have creator
  // grants - probably should add creator grants.
  // TBD

  privStatus = initializeComponentPrivileges();
  if (privStatus != STATUS_GOOD) return STATUS_ERROR;

  // In release 2.7, added new privileges for objects (ALTER, DROP).
  // Fix up existing metadata to set these bits for existing tables.
  int64_t rowCount = 0;
  if (updatePrivBitmap(DROP_PRIV, rowCount) < 0) return STATUS_ERROR;
  traceMsg = "updated privilege bitmaps: DROP_PRIV ";
  traceMsg += UIDToString(rowCount);
  if (updatePrivBitmap(ALTER_PRIV, rowCount) < 0) return STATUS_ERROR;
  traceMsg = " ALTER_PRIV ";
  traceMsg += UIDToString(rowCount);
  log(__FILE__, traceMsg, -1);

  return STATUS_GOOD;
}

// *****************************************************************************
// PrivMgr upgrade methods
//
//   See sqlcomp/CmpSeabaseDDLupgrade.h for upgrade details
//   See sqlcomp/CmpSeabaseDDLmdcommon.h for general upgrade structures
//   See sqlcomp/PrivMgrMDDefs.h for PrivMgr table and upgrade descriptions
//   See sqlcomp/PrivMgrMD.h for class description
// *****************************************************************************

// ----------------------------------------------------------------------------
// method:  alterRenamePMTbl
//
// Renames PrivMgr metadata tables scheduled for upgrade
//
// Parameters:
//   cliInterface - class that manages CLI requests
//   origToOld
//     TRUE - rename existing files to files appended "_OLD_MD"
//     FALSE - rename files appended with "_OLD_MD" to names without the suffix
// ----------------------------------------------------------------------------
short PrivMgrMDAdmin::alterRenamePMTbl(ExeCliInterface *cliInterface, NABoolean origToOld) {
  int cliRC = 0;

  int bufSize = (strlen(TRAFODION_SYSCAT_LIT) * 2) + (strlen(SEABASE_MD_SCHEMA) * 2) + (256 * 2) + 100;
  char queryBuf[bufSize];

  CmpSeabaseDDL cmpDDL(STMTHEAP);

  // Alter table rename manages its own transactions, if one is active, error
  if (cmpDDL.xnInProgress(cliInterface)) {
    *pDiags_ << DgSqlCode(-20123);
    return -1;
  }

  std::string traceMsg("Upgrade: alterRenamePMTbl: origToOld: ");
  traceMsg += (origToOld ? 'T' : 'F');
  log(__FILE__, traceMsg, -1);

  int tables_upgraded = 0;

  // Rename each table slated for upgrade
  for (int i = 0; i < sizeof(allPrivMgrUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &pmti = allPrivMgrUpgradeInfo[i];

    //  If not upgrading the table, skip
    if (!pmti.upgradeNeeded) continue;

    const char *srcObject = (origToOld ? pmti.newName : pmti.oldName);
    const char *tgtObject = (origToOld ? pmti.oldName : pmti.newName);

    // Based old origToOld -> either
    //  rename the orig table (src) to the same table with "_OLD_MD" appended (tgt) or
    //  rename the "_OLD_MD" suffixed table (src) to the orig name (tgt)
    str_sprintf(queryBuf, "alter table %s.\"%s\".%s rename to %s skip view check; ", TRAFODION_SYSCAT_LIT,
                SEABASE_PRIVMGR_SCHEMA, srcObject, tgtObject);

    traceMsg += "  cmd: ";
    traceMsg += queryBuf;
    log(__FILE__, traceMsg, -1);

    cliRC = cliInterface->executeImmediate(queryBuf);

    // During recovery, we could get "object not found" if error occurred
    // before one or more tables were renamed. We could get "object exists error if
    // rename was never successful initially.
    if (cliRC == -CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION || cliRC == -CAT_TRAFODION_OBJECT_EXISTS ||
        cliRC == -CAT_NOT_A_BASE_TABLE)
      cliRC = 0;

    else if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(pDiags_);
      return -1;
    }

    tables_upgraded++;
    if (tables_upgraded > 1) {
      if (origToOld)
        if (injectError("UPGRADE_PRIVMGR_INJECT_3", "alterRenamePMTbl."))
          return -1;
        else if (injectError("UPGRADE_PRIVMGR_INJECT_12", "undo alterRenamePMTbl."))
          return -1;
    }
  }
  return 0;
}

// ----------------------------------------------------------------------------
// method:  checkForOldPMTbl
//
// When starting the upgrade operations, there should be no files with the
// suffix "_OLD_MD".  If so then a previous upgrade failed.
//
// TBD - if both the OLD and original files exist then it is probably safe
// to remove the old file.  But if the original file does not exist, then
// removing the old file will lose existing data.
//
// Parameters:
//   cliInterface - class that manages CLI requests
//
// If there are "_OLD_MD" tables, an error is put in the diags area
//
// returns
//    -1 - unexpected error
//     0 - no old tables exist
//     1 - old tables exist
// ----------------------------------------------------------------------------
short PrivMgrMDAdmin::checkForOldPMTbl(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  int bufSize = (strlen(TRAFODION_SYSCAT_LIT) * 2) + strlen(SEABASE_MD_SCHEMA) + strlen(SEABASE_OBJECTS) +
                strlen(SEABASE_PRIVMGR_SCHEMA) + (3 * 60);
  char queryBuf[bufSize];

  str_sprintf(queryBuf,
              "select count(*) from %s.\"%s\".%s "
              "where catalog_name = '%s' and schema_name = '%s' and "
              "object_type = 'BT' and object_name like '%%OLD_MD'",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA);

  int len = 0;
  long rowCount = 0;
  cliRC = cliInterface->executeImmediate(queryBuf, (char *)&rowCount, &len, FALSE);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  if (rowCount > 0) {
    PRIVMGR_INTERNAL_ERROR(
        "PrivMgrMDAdmin::checkForOldPMTbl failed, tables with the _OLD_MD suffix exist in the _PRIVMGR_MD_ SCHEMA");
    return 1;
  }
  return 0;
}

// ----------------------------------------------------------------------------
// method: copyPMOldToNew
//
// Copy data from old tables to new tables based on the information
//   found in the MDUpgradeInfo structure
//
// Parameters:
//   cliInterface - class that manages CLI requests
// ----------------------------------------------------------------------------
short PrivMgrMDAdmin::copyPMOldToNew(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  int bufSize = 10000;
  char queryBuf[bufSize];
  CmpSeabaseDDL cmpDDL(STMTHEAP);
  NABoolean xnWasStartedHere = FALSE;

  // Run all copies in a single txn
  if (cmpDDL.beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) {
    cliInterface->retrieveSQLDiagnostics(pDiags_);
    return -1;
  }

  // Copy data for tables marked for upgrade
  for (int i = 0; i < sizeof(allPrivMgrUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &pmti = allPrivMgrUpgradeInfo[i];

    // If not upgrading the table, skip
    if (!pmti.upgradeNeeded) continue;

    str_sprintf(queryBuf,
                "upsert into %s.\"%s\".%s %s%s%s select "
                "%s from %s.\"%s\".%s SRC %s",
                TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA, pmti.newName, (pmti.insertedCols ? "(" : ""),
                (pmti.insertedCols ? pmti.insertedCols : ""), (pmti.insertedCols ? ")" : ""),
                (pmti.selectedCols ? pmti.selectedCols : "*"), TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA,
                pmti.oldName, (pmti.wherePred ? pmti.wherePred : ""));

    // Perform the copy
    long rowCount;
    cliRC = cliInterface->executeImmediate(queryBuf, NULL, NULL, TRUE, &rowCount);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(pDiags_);
      break;
    }

    if (injectError("UPGRADE_PRIVMGR_INJECT_6", "copyPMOldToNew.")) {
      cliRC = -1;
      break;
    }

    // TBD - log the number of rows copied
    //
  }  // for

  if (cmpDDL.endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) {
    cliInterface->retrieveSQLDiagnostics(pDiags_);
    return -1;
  }

  if (cliRC < 0) return -1;
  return 0;
}

// ----------------------------------------------------------------------------
// method: createPMTbl
//
// Create any new tables
//
// Parameters:
//   cliInterface - class that manages CLI requests
// ----------------------------------------------------------------------------
short PrivMgrMDAdmin::createPMTbl(ExeCliInterface *cliInterface) {
  int cliRC = 0;
  int bufSize = strlen(TRAFODION_SYSCAT_LIT) + strlen(SEABASE_PRIVMGR_SCHEMA) + 300;

  char queryBuf[bufSize];
  CmpSeabaseDDL cmpDDL(STMTHEAP);
  NABoolean xnWasStartedHere = FALSE;

  std::string traceMsg = "Upgrade:  createPMTbl";
  log(__FILE__, traceMsg, -1);

  // Run all creates in a single txn
  if (cmpDDL.beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) {
    cliInterface->retrieveSQLDiagnostics(pDiags_);
    return -1;
  }

  // create required tables in the schema
  int tables_upgraded = 0;
  for (int i = 0; i < sizeof(allPrivMgrUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &pmti = allPrivMgrUpgradeInfo[i];

    // If this is not a new metadata table and not doing an upgrade, skip
    if (!pmti.upgradeNeeded && !pmti.addedTable) continue;

    traceMsg = "  table: ";
    traceMsg += pmti.newName;
    log(__FILE__, traceMsg, -1);

    str_sprintf(queryBuf, "%s.\"%s\".%s ", TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA, pmti.newName);
    cliRC = createTable(queryBuf, pmti.newDDL, cliInterface, pDiags_);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(pDiags_);
      cliRC = -1;
      break;
    }
    tables_upgraded++;
    if (tables_upgraded > 1) {
      if (injectError("UPGRADE_PRIVMGR_INJECT_4", "createPMTbl.")) {
        cliRC = -1;
        break;
      }
    }
  }

  if (cmpDDL.endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) {
    cliInterface->retrieveSQLDiagnostics(pDiags_);
    return -1;
  }

  if (cliRC < 0) return -1;
  return 0;
}

// ----------------------------------------------------------------------------
// method: dropAndLogPMViews
//
// Finds views on PrivMgr metadata tables and logs their definition
// Currently, PrivMgr schema contains no metadata views. This code added
// for completeness.
//
// Parameters:
//   cliInterface - class that manages CLI requests
//   someViewsSaved
//     TRUE - views need to be recreated
//     FALSE - no views exist
//
// See CmpSeabaseDDLrepos.cpp - dropAndLogPMViews for additional details
// ----------------------------------------------------------------------------
short PrivMgrMDAdmin::dropAndLogPMViews(ExeCliInterface *cliInterface, NABoolean &someViewSaved /* out */) {
  int cliRC = 0;  // assume success
  someViewSaved = FALSE;
  CmpSeabaseDDL cmpDDL(STMTHEAP);

  // For each table that has been migrated, drop any views that existed
  // on the old table and save their view text. (In the future, we can
  // add logic to recreate the views on the new tables.)
  for (int i = 0; i < sizeof(allPrivMgrUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &pmti = allPrivMgrUpgradeInfo[i];

    if ((!pmti.oldName) || (NOT pmti.upgradeNeeded)) continue;

    long tableUID = cmpDDL.getObjectUID(cliInterface, TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA, pmti.oldName,
                                        COM_BASE_TABLE_OBJECT_LIT, NULL, NULL, NULL, FALSE, FALSE /* ignore error */);

    if (tableUID > 0)  // if we got it
    {
      NAList<NAString> viewNameList(STMTHEAP);
      NAList<NAString> viewDefnList(STMTHEAP);

      if (cmpDDL.saveAndDropUsingViews(tableUID, cliInterface, viewNameList /* out */, viewDefnList /* out */) == -1)
        cliRC = -1;  // couldn't get views for old repository table

      else if (viewDefnList.entries() > 0)  // if we dropped some views
      {
        for (int i = 0; i < viewDefnList.entries(); i++) {
          SQLMXLoggingArea::logSQLMXPredefinedEvent(
              "The following view on a Repository table was dropped as part of upgrade processing:", LL_ERROR);
          SQLMXLoggingArea::logSQLMXPredefinedEvent(viewDefnList[i].data(), LL_ERROR);
        }
        someViewSaved = TRUE;
      }
    } else
      cliRC = -1;  // couldn't get objectUID for old repository table
  }

  return cliRC;
}

// -----------------------------------------------------------------------------
// method:  dropPMTbl
//
// Drop tables from the PrivMgr metadata
//
// Called in the following instances:
//   Drop repository tables with a "_OLD_MD" suffix (oldPMTbl = TRUE)
//    - at the end of upgrade to remove the old metadata copies
//
//   Drop repository tables without an "_OLD_MD" suffix (oldPMTbl = FALSE)
//    - during a failed upgrade to remove the in-flight tables
//
// Parameters:
//   cliInterface - class that manages CLI requests
//   oldPMTbl - drop tables from files with the "_OLD_MD" suffix
//   inRecovery - dropping tables after a failed upgrade
// ----------------------------------------------------------------------------
short PrivMgrMDAdmin::dropPMTbl(ExeCliInterface *cliInterface, NABoolean oldPMTbl, NABoolean inRecovery) {
  int cliRC = 0;
  int bufSize = 500;
  char queryBuf[bufSize];
  CmpSeabaseDDL cmpDDL(STMTHEAP);
  NABoolean xnWasStartedHere = FALSE;

  std::string traceMsg("Upgrade: dropPMTbl: drop old tables: ");
  traceMsg += (oldPMTbl ? 'T' : 'F');
  traceMsg += " inRecovery: ";
  traceMsg += (inRecovery ? 'T' : 'F');
  log(__FILE__, traceMsg, -1);

  // Run all drops in a single tx
  if (cmpDDL.beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) {
    cliInterface->retrieveSQLDiagnostics(pDiags_);
    return -1;
  }

  // drop relevant tables
  for (int i = 0; i < sizeof(allPrivMgrUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &pmti = allPrivMgrUpgradeInfo[i];

    // Asked to remove tables with the "_OLD_MD" suffix
    if (oldPMTbl) {
      // If this is a new metadata table or not upgrading then no _OLD_MD
      // table exists, continue
      if (pmti.addedTable || !pmti.upgradeNeeded) continue;
    } else {
      // Asked to remove real tables.  This happens during recovery since
      // currently upgrade is not instrumented to remove exisitng
      // metadata tables  - we should always be in recovery mode.
      if (!inRecovery) continue;

      // We are in recovery: if this table is not being upgraded, don't remove
      // Don't want to accidently remove a table not being upgraded
      if (inRecovery && !pmti.upgradeNeeded) continue;
    }

    const char *objectName = (oldPMTbl ? pmti.oldName : pmti.newName);

    // Go ahead and drop the table
    if (snprintf(queryBuf, bufSize, "drop table if exists %s.\"%s\".%s cascade; ", TRAFODION_SYSCAT_LIT,
                 SEABASE_PRIVMGR_SCHEMA, objectName) >= bufSize) {
      PRIVMGR_INTERNAL_ERROR("PrivMgrMDAdmin::dropPMTbl failed, internal buffer size too small");
      cliRC = -1;
      break;
    }

    traceMsg = "  drop table: ";
    traceMsg += objectName;
    log(__FILE__, traceMsg, -1);

    cliRC = cliInterface->executeImmediate(queryBuf);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(pDiags_);
      cliRC = -1;
      break;
    }
    if (oldPMTbl) {
      if (injectError("UPGRADE_PRIVMGR_INJECT_7", "dropPMTbl.")) {
        cliRC = -1;
        break;
      }
    } else {
      if (injectError("UPGRADE_PRIVMGR_INJECT_11", "undo dropPMTbl.")) {
        cliRC = -1;
        break;
      }
    }
  }

  if (cmpDDL.endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) {
    cliInterface->retrieveSQLDiagnostics(pDiags_);
    return -1;
  }

  if (cliRC < 0) return -1;
  return 0;
}

// ----------------------------------------------------------------------------
// method: grantPMTbl
//
// Generally when a table is created, owner privileges for the table are
// also granted.  In this case, we are upgrading privilege manager
// tables so the existing create table code ignores granting owner privs.
// That is, we don't want to insert into tables that may not yet exist
//
// Parameters:
//   cliInterface - class that manages CLI requests
// ----------------------------------------------------------------------------
short PrivMgrMDAdmin::grantPMTbl(ExeCliInterface *cliInterface, NABoolean inRecovery) {
  int cliRC = 0;
  CmpSeabaseDDL cmpDDL(STMTHEAP);
  std::string traceMsg("Upgrade: grantPMTbl:");
  NABoolean xnWasStartedHere = FALSE;

  // revoke privs on tables in single txn
  if (cmpDDL.beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) {
    cliInterface->retrieveSQLDiagnostics(pDiags_);
    return -1;
  }

  // grant owner level privileges on tables
  for (int i = 0; i < sizeof(allPrivMgrUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &pmti = allPrivMgrUpgradeInfo[i];

    // If this is not a new metadata table and not doing an upgrade, skip
    if (!pmti.upgradeNeeded && !pmti.addedTable) continue;

    traceMsg = "  grant owner privs: ";
    traceMsg += pmti.newName;
    log(__FILE__, traceMsg, -1);

    // get object UID, object should exist
    long objectUID = cmpDDL.getObjectUID(cliInterface, TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA, pmti.newName,
                                         COM_BASE_TABLE_OBJECT_LIT, NULL, NULL, NULL, FALSE, FALSE /* ignore error */);
    if (objectUID <= 0) {
      PRIVMGR_INTERNAL_ERROR("PrivMgrMDAdmin::grantPMTbl unable to retrieve object details");
      cliRC = -1;
      break;
    }

    NAString privMgrMDLoc(cmpDDL.getSystemCatalogStatic());
    ;
    privMgrMDLoc += ".\"";
    privMgrMDLoc += SEABASE_PRIVMGR_SCHEMA;
    privMgrMDLoc += "\"";

    std::string objectName(privMgrMDLoc);
    objectName += '.';
    objectName += pmti.newName;
    PrivMgrPrivileges privileges(objectUID, objectName, SYSTEM_USER, std::string(privMgrMDLoc.data()),
                                 CmpCommon::diags());
    if (privileges.grantToOwners(COM_BASE_TABLE_OBJECT, ROOT_USER_ID, DB__ROOT, ROOT_USER_ID, DB__ROOT, ROOT_USER_ID,
                                 DB__ROOT) == STATUS_ERROR) {
      cliRC = -1;
      break;
    }

    if (inRecovery) {
      if (injectError("UPGRADE_PRIVMGR_INJECT_13", "undo grantPMTbl.")) {
        cliRC = -1;
        break;
      }
    } else {
      if (injectError("UPGRADE_PRIVMGR_INJECT_5", "grantPMTbl.")) {
        cliRC = -1;
        break;
      }
    }
  }

  if (cmpDDL.endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) {
    cliInterface->retrieveSQLDiagnostics(pDiags_);
    return -1;
  }

  if (cliRC < 0) return -1;
  return 0;
}

// ----------------------------------------------------------------------------
// method: revokePMTbl
//
// Generally when a table is dropped, the privileges assigned to the
// table are also dropped.  In this case, we are upgrading privilege manager
// tables so the existing drop table code ignores updating privs; therefore,
// the privileges have to be manually revoked/granted
//
// Since this is called during upgrade, we do not store privileges for tables
// with an "_OLD_MD" suffix so only revoke privileges for the orig/new tables.
//
// Parameters:
//   cliInterface - class that manages CLI requests
//   inRecovery -
//     FALSE - an error is returned if unable to revoke privileges
//      TRUE - ignore errors if unable to revoke privileges
// ----------------------------------------------------------------------------
short PrivMgrMDAdmin::revokePMTbl(ExeCliInterface *cliInterface, NABoolean oldPMTbl, NABoolean inRecovery) {
  int cliRC = 0;
  CmpSeabaseDDL cmpDDL(STMTHEAP);
  std::string traceMsg("Upgrade: revokePMTbl:");
  NABoolean xnWasStartedHere = FALSE;

  // revoke privs on tables in single txn
  if (cmpDDL.beginXnIfNotInProgress(cliInterface, xnWasStartedHere)) {
    cliInterface->retrieveSQLDiagnostics(pDiags_);
    return -1;
  }

  // revoke privileges
  for (int i = 0; i < sizeof(allPrivMgrUpgradeInfo) / sizeof(MDUpgradeInfo); i++) {
    const MDUpgradeInfo &pmti = allPrivMgrUpgradeInfo[i];

    // If this is not a new metadata table and not doing an upgrade, skip
    if (!pmti.upgradeNeeded && !pmti.addedTable) continue;

    traceMsg = "  revoke privs: ";
    traceMsg += pmti.newName;
    log(__FILE__, traceMsg, -1);

    // get the objectUID for the table
    long objectUID = cmpDDL.getObjectUID(cliInterface, TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA, pmti.newName,
                                         COM_BASE_TABLE_OBJECT_LIT, NULL, NULL, NULL, FALSE, FALSE /* ignore error */);
    if (objectUID <= 0) {
      if (inRecovery) continue;

      PRIVMGR_INTERNAL_ERROR("PrivMgrMDAdmin::revokePMTbl failed, error reading metadata");
      cliRC = -1;
      break;
    }

    // Revoke any privileges assigned to the table
    PrivMgrPrivileges privInterface;
    privInterface.setObjectUID(objectUID);
    if (privInterface.revokeObjectPriv() == STATUS_ERROR) {
      if (inRecovery) continue;

      cliRC = -1;
      break;
    }

    if (inRecovery) {
      if (injectError("UPGRADE_PRIVMGR_INJECT_10", "undo revokePMTbl.")) {
        cliRC = -1;
        break;
      }
    }

    else {
      if (injectError("UPGRADE_PRIVMGR_INJECT_2", "revokePMTbl.")) {
        cliRC = -1;
        break;
      }
    }
  }

  if (cmpDDL.endXnIfStartedHere(cliInterface, xnWasStartedHere, cliRC) < 0) {
    cliInterface->retrieveSQLDiagnostics(pDiags_);
    return -1;
  }

  if (cliRC < 0) return -1;

  return cliRC;
}

// ----------------------------------------------------------------------------
// method:  upgradePMTbl
//
// State machine for privilege manager upgrades
//
// Parameters:
//   cliInterface - class that manages CLI requests
//   CmpDDLwithStatusInfo - upgrade details
// ----------------------------------------------------------------------------
short PrivMgrMDAdmin::upgradePMTbl(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui) {
  int cliRC = 0;

  while (1)  // exit via return stmt in switch
  {
    switch (mdui->subStep()) {
      case 0: {
        mdui->setMsg("Upgrade PrivMgr Metadata: Started");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 1: {
        mdui->setMsg("  Start: Check For Old PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 2: {
        // Error out if old tables exist, left from a previous failed upgrade
        cliRC = checkForOldPMTbl(cliInterface);
        if (cliRC != 0) return ERR_DONE;

        if (injectError("UPGRADE_PRIVMGR_INJECT_1", "checkForOldPMTbls.")) {
          return ERR_DONE;
        }

        mdui->setMsg("  End:   Check For Old PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 3: {
        mdui->setMsg("  Start: Revoke Privileges From Original PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 4: {
        // revoke privileges from original tables
        if (revokePMTbl(cliInterface, FALSE /*orig table*/, FALSE /*not in recovery*/))
          return ERR_DONE;  // fails - privs not revoked remove old tables

        mdui->setMsg("  End:   Revoke Privileges from Original PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 5: {
        mdui->setMsg("  Start: Rename Original PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 6: {
        // rename original table to old table
        if (alterRenamePMTbl(cliInterface, TRUE /*target is old table*/))
          return ERR_RESTORE_ORIG_TBLS;  // error, no old data exists so just return

        mdui->setMsg("  End:   Rename Original PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 7: {
        mdui->setMsg("  Start: Create New PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 8: {
        // create new tables
        if (createPMTbl(cliInterface)) return ERR_RESTORE_ORIG_TBLS;  // error, bring back the orig and drop old tables

        mdui->setMsg("  End:   Create New PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 9: {
        mdui->setMsg("  Start: Grant Privileges To New PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 10: {
        // create new or updated tables
        if (grantPMTbl(cliInterface, FALSE))
          return ERR_DROP_NEW_TBLS;  // error, bring back the original and drop the old

        mdui->setMsg("  End:   Grant Privileges To New PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 11: {
        mdui->setMsg("  Start: Copy Old PrivMgr Metadata Contents ");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 12: {
        // copy old contents into new repository
        if (copyPMOldToNew(cliInterface))
          return ERR_REVOKE_PRIVS;  // error, need to drop new repository then undo rename

        mdui->setMsg("  End:   Copy Old PrivMgr Metadata Contents ");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

        // TDB:  add code to remove any dropped metadata tables here

      case 13: {
        mdui->setMsg("Upgrade PrivMgr Metadata: Done except for cleaning up");
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

// ----------------------------------------------------------------------------
// method: upgradePMTblComplete
//
// Completes the upgrade of PrivMgr metadata by managing views and by dropping
// the OLD metadata information.
//
// Parameters:
//   cliInterface - class that manages CLI requests
//   CmpDDLwithStatusInfo - Upgrade details
// ----------------------------------------------------------------------------
short PrivMgrMDAdmin::upgradePMTblComplete(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui) {
  int cliRC = 0;

  while (1)  // exit via return stmt in switch
  {
    switch (mdui->subStep()) {
      case 0: {
        mdui->setMsg("Upgrade PrivMgr Metadata: Check for views");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 1: {
        // If there were views on the old privmgr tables, they are
        // still there, by virtue of the fact that we did "SKIP VIEW CHECK"
        // on the ALTER TABLE RENAME. Now we drop the views and capture
        // their view definitions (which will contain the old table name,
        // not the renamed table name) and log the view text so that the
        // user can recreate these views later if he/she wishes.
        // In the future, perhaps, we will migrate these views automatically.

        NABoolean someViewSaved = FALSE;
        // dropAndLogPMViews(cliInterface,someViewSaved /* out */);

        mdui->setMsg("Upgrade PrivMgr Metadata: View check done");
        mdui->subStep()++;
        if (!someViewSaved) mdui->subStep()++;  // skip view text saved message step
        return 0;
      } break;

      case 2: {
        // This state is here in case we want to report to the user that
        // there was a view that was dropped. But we saved the view text
        // in the logs.
        mdui->setMsg(
            "One or more views on a PrivMgr Metadata table were dropped. "
            "View text was saved in the logs. Look for 'CREATE VIEW' to find it.");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 3: {
        mdui->setMsg("Upgrade PrivMgr Metadata: Drop Old Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 4: {
        // drop old repository; ignore errors
        dropPMTbl(cliInterface, TRUE /*old repos*/, FALSE /*not in recovery*/);

        mdui->setMsg("Upgrade PrivMgr Metadata: Drop Old Metadata done");
        mdui->setEndStep(TRUE);
        mdui->setSubstep(0);

        return 0;
      } break;

      default:
        return -1;
    }
  }  // while

  return 0;
}

// ----------------------------------------------------------------------------
// method: upgradePMTblUndo
//
// resets a failed PrivMgr metadata upgrade
//
// Parameters:
//   cliInterface - class that manages CLI requests
//   CmpDDLwithStatusInfo - Upgrade details
// ----------------------------------------------------------------------------
short PrivMgrMDAdmin::upgradePMTblUndo(ExeCliInterface *cliInterface, CmpDDLwithStatusInfo *mdui) {
  int cliRC = 0;

  while (1)  // exit via return stmt in switch
  {
    switch (mdui->subStep()) {
      // error return codes from upgradePMTbl are mapped to the recovery substep
      case 0:  // corresponds to (-1) ERR_REVOKE_PRIVS      -> set substep to 6
      case 1:  // corresponds to (-2) ERR_DROP_NEW_TBLS     -> set substep to 8
      case 2:  // corresponds to (-3) ERR_RESTORE_ORIG_TBLS -> set substep to 10
      case 3:  // corresponds to (-4) ERR_GRANT_PRIVS       -> set substep to 12
      case 4:  // corresponds to (-5) ERR_DONE              -> set substep to 14
      {
        mdui->setMsg("Upgrade PrivMgr Metadata Failed: Restoring Old Metadata");
        mdui->setSubstep(2 * mdui->subStep() + 6);  // go to appropriate case
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      // corresponds to (-6) ERR_RESTORE_FAILED
      case 5:  // corresponds to (-5) ERR_DONE
      {
        // Completed undo operation
        mdui->setMsg("Upgrade PrivMgr Metadata Failed: Restore attempt failed, contact EsgynDB support");
        mdui->setSubstep(0);
        mdui->setEndStep(TRUE);

        return 0;
      } break;

      // corresponds to (-1) ERR_REVOKE_PRIVS
      case 6: {
        mdui->setMsg(" Start: Revoke Privileges From New PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 7: {
        // revoke privileges from the new repository
        if (revokePMTbl(cliInterface, FALSE, TRUE /* inRecovery*/) != 0) {
          mdui->setSubstep(ERR_RESTORE_FAILED);
          return 0;
        }

        cliInterface->clearGlobalDiags();
        mdui->setMsg(" End:   Revoke Privileges From New PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      // corresponds to (-2) ERR_DROP_NEW_TBLS
      case 8: {
        mdui->setMsg(" Start: Drop New PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 9: {
        // drop the new repository
        if (dropPMTbl(cliInterface, FALSE, TRUE /*inRecovery*/) != 0) {
          mdui->setSubstep(ERR_RESTORE_FAILED);
          return 0;
        }

        cliInterface->clearGlobalDiags();
        mdui->setMsg(" End:   Drop New PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 10:  // corresponds to (-3) ERR_RESTORE_ORIG_TBLS
      {
        mdui->setMsg(" Start: Restore Original PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 11: {
        // restore the original tables -> copy old to original
        if (alterRenamePMTbl(cliInterface, FALSE /*new repos*/) != 0) {
          mdui->setSubstep(ERR_RESTORE_FAILED);
          return 0;
        }

        cliInterface->clearGlobalDiags();
        mdui->setMsg(" End:   Restore Original PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 12:  // corresponds to (-4) ERR_GRANT_PRIVS
      {
        mdui->setMsg(" Start: Restore Privileges On Original PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 13: {
        // grant back owner privileges
        if (grantPMTbl(cliInterface, TRUE) != 0) {
          mdui->setSubstep(ERR_RESTORE_FAILED);
          return 0;
        }

        cliInterface->clearGlobalDiags();
        mdui->setMsg(" End:   Restore Privileges On Original PrivMgr Metadata");
        mdui->subStep()++;
        mdui->setEndStep(FALSE);

        return 0;
      } break;

      case 14:  // corresponds to (-5) ERR_RESTORE_FAILED
      {
        // Completed undo operation
        mdui->setMsg("Upgrade PrivMgr Metadata Failed: Metadata Restored");
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
