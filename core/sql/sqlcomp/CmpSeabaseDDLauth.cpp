
#define SQLPARSERGLOBALS_FLAGS
#include "parser/SqlParserGlobalsCmn.h"

#include <algorithm>

#include "sqlcomp/CmpSeabaseDDLauth.h"
#include "sqlcomp/CmpSeabaseDDL.h"
#include "sqlcomp/CmpSeabaseTenant.h"
#include "parser/StmtDDLRegisterUser.h"
#include "parser/StmtDDLTenant.h"
#include "parser/StmtDDLAlterUser.h"
#include "parser/StmtDDLCreateRole.h"
#include "parser/StmtDDLUserGroup.h"
#include "parser/ElemDDLGrantee.h"
#include "arkcmp/CompException.h"
#include "parser/ElemDDLGroup.h"
#include "cli/Context.h"
#include "dbsecurity/dbUserAuth.h"
#include "common/ComUser.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"
#include "export/NAStringDef.h"
#include "exp/ExpHbaseInterface.h"
#include "sqlcomp/PrivMgrCommands.h"
#include "sqlcomp/PrivMgrRoles.h"
#include "sqlcomp/PrivMgrComponentPrivileges.h"
#include "sqlcomp/PrivMgrSchemaPrivileges.h"
#include "sqlcomp/PrivMgrObjects.h"
#include "common/ComRtUtils.h"
#include "executor/TenantHelper_JNI.h"
#include "sqlcomp/CompilerSwitchDDL.h"
#include "common/sq_license.h"

#include <sys/types.h>
#include <pwd.h>

#define RESERVED_AUTH_NAME_PREFIX "DB__"
#define DEFAULT_AUTH_CONFIG       0

#define MAX_PASSWORD_LENGTH 128

inline static bool validateExternalGroupname(const char *externalGroupName, const char *configurationName,
                                             Int16 &foundConfigurationNumber);

inline static bool validateExternalUsername(const char *externalUsername, const char *configurationName,
                                            Int16 &foundConfigurationNumber);

#define CHECK_LICENSE                                                                                 \
  do {                                                                                                \
    if (COM_LOCAL_AUTH == currentAuthType_ && !GetCliGlobals()->isLicenseModuleOpen(LM_LOCAL_AUTH)) { \
      *CmpCommon::diags() << DgSqlCode(-CAT_NO_LICENSE_ERROR) << DgString0("LOCAL-AUTH");             \
      return;                                                                                         \
    }                                                                                                 \
  } while (0)

// copy from core/conn/security_dll/native/common/utils.h
static int is_little_endian(void) {
  union {
    long l;
    unsigned char uc[sizeof(long)];
  } u;

  u.l = 1;
  return u.uc[0];
}

static int swapByteOrderOfS(unsigned int us) {
  return ((us) << 24) | (((us) << 8) & 0x00ff0000) | (((us) >> 8) & 0x0000ff00) | ((us) >> 24);
}

#define IS_ALLOWED_GRANT_ROLE_TO_GROUP(flag)                                                           \
  {                                                                                                    \
    if (CmpCommon::getDefault(ALLOW_ROLE_GRANTS_TO_GROUPS) == DF_ON ||                                 \
        CmpCommon::getDefault(ALLOW_ROLE_GRANTS_TO_GROUPS) == DF_SYSTEM && !getenv("SQLMX_REGRESS")) { \
      flag = true;                                                                                     \
    }                                                                                                  \
  }

// ****************************************************************************
// Class AuthConflictList methods
// ****************************************************************************

// ----------------------------------------------------------------------------
// method:  createInList
//
// This returns a comma separated string of all the source authIDs (users and
// roles) that have conflicts.
// -----------------------------------------------------------------------------
NAString AuthConflictList::createInList() {
  char intStr[20];
  NAString IDList;
  for (int i = 0; i < entries(); i++) {
    if (i > 0) IDList += ',';
    IDList += str_itoa(operator[](i).getSourceID(), intStr);
  }
  return IDList;
}

// ----------------------------------------------------------------------------
// method:  findEntry
//
// Returns an index into the list that matches the passed in conflict.
// Returns NULL_COLL_INDEX if conflict is not in the list.
// ----------------------------------------------------------------------------
CollIndex AuthConflictList::findEntry(const AuthConflict &conflict) {
  for (CollIndex i = 0; i < entries(); i++) {
    AuthConflict other = operator[](i);
    if (other == conflict) return i;
  }
  return NULL_COLL_INDEX;
}

// ----------------------------------------------------------------------------
// method:  generateConflicts
//
// Reads the xxxxxx_privileges tables and generates a list of all the authIDs
// where the auth names match but the authIDs do not.
//
// There is no need to include schema_owner and object_owner from the OBJECTS
// table - all schema_owner's and object_owner's exist in the OBJECT_PRIVILEGES
// table.
//
// Result: < 0 (error); 0 good
// ----------------------------------------------------------------------------
short AuthConflictList::generateConflicts(ExeCliInterface *cliInterface) {
  int bufSize = strlen(TRAFODION_SYSCAT_LIT) * 12 + strlen(SEABASE_PRIVMGR_SCHEMA) * 6 + strlen(SEABASE_MD_SCHEMA) * 6 +
                strlen(PRIVMGR_OBJECT_PRIVILEGES) * 2 + strlen(PRIVMGR_COLUMN_PRIVILEGES) * 2 +
                strlen(PRIVMGR_SCHEMA_PRIVILEGES) * 2 + strlen(SEABASE_AUTHS) * 6 + (18 * 100);
  char queryBuf[bufSize];

  Queue *tableQueue = NULL;

  str_sprintf(queryBuf,
              "select distinct grantee_id as sourceID, auth_id as targetID, auth_db_name, "
              "'grantee_id', 'OBJECT_PRIVILEGES' from %s.\"%s\".%s a, %s.\"%s\".%s o "
              "where grantee_name =  auth_db_name and grantee_id <> auth_id "
              "union (select distinct grantor_id as sourceID, auth_id as targetID, auth_db_name, "
              "'grantor_id', 'OBJECT_PRIVILEGES' from %s.\"%s\".%s a, %s.\"%s\".%s o "
              "where grantor_name =  auth_db_name and grantor_id <> auth_id ) "
              "union (select distinct grantee_id as sourceID, auth_id as targetID, auth_db_name, "
              "'grantee_id', 'COLUMN_PRIVILEGES' from %s.\"%s\".%s a, %s.\"%s\".%s o "
              "where grantee_name =  auth_db_name and grantee_id <> auth_id ) "
              "union (select distinct grantor_id as sourceID, auth_id as targetID, auth_db_name, "
              "'grantor_id', 'COLUMN_PRIVILEGES' from %s.\"%s\".%s a, %s.\"%s\".%s o "
              "where grantor_name =  auth_db_name and grantor_id <> auth_id ) "
              "union (select distinct grantee_id as sourceID, auth_id as targetID, auth_db_name, "
              "'grantee_id', 'SCHEMA_PRIVILEGES' from %s.\"%s\".%s a, %s.\"%s\".%s o "
              "where grantee_name =  auth_db_name and grantee_id <> auth_id ) "
              "union (select distinct grantor_id as sourceID, auth_id as targetID, auth_db_name, "
              "'grantor_id', 'SCHEMA_PRIVILEGES' from %s.\"%s\".%s a, %s.\"%s\".%s o "
              "where grantor_name =  auth_db_name and grantor_id <> auth_id ) order by 1",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_AUTHS, TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA,
              PRIVMGR_OBJECT_PRIVILEGES, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_AUTHS, TRAFODION_SYSCAT_LIT,
              SEABASE_PRIVMGR_SCHEMA, PRIVMGR_OBJECT_PRIVILEGES, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_AUTHS,
              TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_COLUMN_PRIVILEGES, TRAFODION_SYSCAT_LIT,
              SEABASE_MD_SCHEMA, SEABASE_AUTHS, TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA, PRIVMGR_COLUMN_PRIVILEGES,
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_AUTHS, TRAFODION_SYSCAT_LIT, SEABASE_PRIVMGR_SCHEMA,
              PRIVMGR_SCHEMA_PRIVILEGES, TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_AUTHS, TRAFODION_SYSCAT_LIT,
              SEABASE_PRIVMGR_SCHEMA, PRIVMGR_SCHEMA_PRIVILEGES);

  int cliRC = cliInterface->fetchAllRows(tableQueue, queryBuf, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // Create a AuthConflict for every unique (sourceID, targetID, authName, mdTable, mdCol) found
  tableQueue->position();
  for (int i = 0; i < tableQueue->numEntries(); i++) {
    OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();

    int sourceID(*(int *)pCliRow->get(0));
    int targetID(*(int *)pCliRow->get(1));
    NAString authName((char *)pCliRow->get(2));
    NAString mdColumn((char *)pCliRow->get(3));
    NAString mdTable((char *)pCliRow->get(4));

    AuthConflict conflict(sourceID, authName, targetID, mdTable, mdColumn);
    this->insert(conflict);
  }
  return 0;
}

// ----------------------------------------------------------------------------
// method:  getIndexForSourceID
//
// This method returns the first entry (index) where the passed in sourceID
// matches the sourceID_ in the list.
// Returns NULL_COLL_INDEX if sourceID is not in the list.
//
// There could be several entries where the passed in sourceID matches the
// sourceID_ in the list (one for each table/column combination), but for each
// of these entries; the sourceID, targetID, and authName are always the same.
// ----------------------------------------------------------------------------
CollIndex AuthConflictList::getIndexForSourceID(int sourceID) {
  for (CollIndex i = 0; i < entries(); i++) {
    AuthConflict conflict = operator[](i);
    if (conflict.getSourceID() == sourceID) return i;
  }
  return NULL_COLL_INDEX;
}

// ----------------------------------------------------------------------------
// method:  updateObjects
//
// Updates the objects table to adjust the existing schema_owner and/or
// object_owner to match target auth_id assigned to the user/role
//
// This code scans the OBJECTS table looking for entries where either the
// schema_owner or object_owner matches a known AuthConflict. If found, the
// entry is updated to reflect the target ID (the AUTHS table contains the
// correct ID).
//
// Result: < 0 (error); 0 good
// ----------------------------------------------------------------------------
short AuthConflictList::updateObjects(ExeCliInterface *cliInterface) {
  NAString sourceIDList = createInList();

  int bufSize = strlen(TRAFODION_SYSCAT_LIT) + strlen(SEABASE_MD_SCHEMA) + strlen(SEABASE_OBJECTS) +
                (sourceIDList.length() * 2) + 200;
  char queryBuf[bufSize];

  Queue *tableQueue = NULL;

  // Get list of objects that need to be updated
  // -> the list of object_uids where either the schema_owner and/or the object_owner
  //    is in the sourceIDList
  str_sprintf(queryBuf,
              "select object_uid, schema_owner, object_owner from %s.\"%s\".%s "
              "where (schema_owner in (%s) or object_owner in (%s))",
              TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA, SEABASE_OBJECTS, sourceIDList.data(), sourceIDList.data());

  int cliRC = cliInterface->fetchAllRows(tableQueue, queryBuf, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  tableQueue->position();
  long rowCount = 0;

  // For each row in the objects table, update those where the schema_owner
  // and/or object_owner needs adjustment
  //   column 0 - value of object_uid
  //   column 1 - value of schema_owner
  //   column 2 - value of object_owner
  for (int i = 0; i < tableQueue->numEntries(); i++) {
    OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();

    long objectUID(*(long *)pCliRow->get(0));

    int schOwner(*(int *)pCliRow->get(1));
    CollIndex schIndex = getIndexForSourceID(schOwner);
    bool updateSchOwner = (schIndex == NULL_COLL_INDEX) ? false : true;

    int objOwner(*(int *)pCliRow->get(2));
    int objIndex = getIndexForSourceID(objOwner);
    bool updateObjOwner = (objIndex == NULL_COLL_INDEX) ? false : true;

    NAString setClause;
    char intStr[20];

    if (updateSchOwner && updateObjOwner) {
      int targetID = operator[](schIndex).getTargetID();
      setClause = "set schema_owner = ";
      setClause += str_itoa(targetID, intStr);
      targetID = operator[](objIndex).getTargetID();
      setClause += ", object_owner = ";
      setClause += str_itoa(targetID, intStr);
    }

    else if (updateSchOwner) {
      int targetID = operator[](schIndex).getTargetID();
      setClause = "set schema_owner = ";
      setClause += str_itoa(targetID, intStr);
    }

    else if (updateObjOwner) {
      int targetID = operator[](objIndex).getTargetID();
      setClause += "set object_owner = ";
      setClause += str_itoa(targetID, intStr);
    }

    else
      continue;

    str_sprintf(queryBuf, "update %s.\"%s\".%s %s where object_uid = %ld", TRAFODION_SYSCAT_LIT, SEABASE_MD_SCHEMA,
                SEABASE_OBJECTS, setClause.data(), objectUID);
    cliRC = cliInterface->executeImmediate(queryBuf, NULL, NULL, TRUE, &rowCount);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    // A conflict is identified by the sourceID, targetID, authName, and mdTable
    // and mdColumn in the table that was updated.
    //
    // If this conflict is already in the list, increment the number of rows
    // updated. If this conflict is not in the list, insert it.
    //
    // At the end, the list of changes and the number of rows affected is
    // logged.
    if (updateSchOwner) {
      AuthConflict schConflict(schOwner, operator[](schIndex).getAuthName(), operator[](schIndex).getTargetID(),
                               SEABASE_OBJECTS, "schema_owner");

      CollIndex conflictIndex = findEntry(schConflict);
      if (conflictIndex == NULL_COLL_INDEX) {
        schConflict.setRowCount(rowCount);
        insert(schConflict);
      } else {
        AuthConflict &conflict = operator[](conflictIndex);
        conflict.setRowCount(conflict.getRowCount() + rowCount);
      }
    }

    if (updateObjOwner) {
      AuthConflict objConflict(objOwner, operator[](objIndex).getAuthName(), operator[](objIndex).getTargetID(),
                               SEABASE_OBJECTS, "object_owner");

      CollIndex conflictIndex = findEntry(objConflict);
      if (conflictIndex == NULL_COLL_INDEX) {
        objConflict.setRowCount(rowCount);
        insert(objConflict);
      } else {
        AuthConflict &conflict = operator[](conflictIndex);
        conflict.setRowCount(conflict.getRowCount() + rowCount);
      }
    }
  }
  return 0;
}

// ----------------------------------------------------------------------------
// method:  updatePrivs
//
// Updates the xxxxxx_privilege table to adjust the existing grantee_id or
// grantor_id to match target auth_id assigned the grantee_name/grantor_name.
//
// Result: < 0 (error); 0 good
// ----------------------------------------------------------------------------
short AuthConflictList::updatePrivs(ExeCliInterface *cliInterface, const char *schName, const char *objName) {
  int bufSize = strlen(TRAFODION_SYSCAT_LIT) + strlen(schName) + strlen(objName) + MAX_AUTHID_AS_STRING_LEN +
                MAX_DBUSERNAME_LEN + 200;
  char queryBuf[bufSize];

  long rowCount = 0;

  // For each conflict, update grantee_id/grantor_id for xxxxxx_privileges table
  // -> update object_privileges set grantee_id = 33344 where grantee_name = 'USER1'
  //
  // If the update violates a unique constraint, we firstly update it to a invalid
  // value, continue to do other update. After that, we update it to the right value
  // again.
  int32_t diagsMark = CmpCommon::diags()->mark();
  std::vector<int> uniqueConflictList;
  int invalidUID = LOWER_INVALID_ID;
  for (int i = 0; i < entries(); i++) {
    AuthConflict &conflict = operator[](i);
    if (strcmp(conflict.getMDTable().data(), objName) == 0) {
      str_sprintf(queryBuf, "update %s.\"%s\".%s set %s = %d where %s = '%s'", TRAFODION_SYSCAT_LIT, schName, objName,
                  conflict.getMDColumn().data(), conflict.getTargetID(),
                  (conflict.getMDColumn() == "grantee_id") ? "grantee_name" : "grantor_name",
                  conflict.getAuthName().data());

      int cliRC = cliInterface->executeImmediate(queryBuf, NULL, NULL, TRUE, &rowCount);
      if (cliRC < 0) {
        // First, ignore error. Maybe check the error number, we should only ignore
        // 8102, indicates violating a unique constraint
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        CmpCommon::diags()->rewind(diagsMark, TRUE);

        str_sprintf(queryBuf, "update %s.\"%s\".%s set %s = %d where %s = '%s'", TRAFODION_SYSCAT_LIT, schName, objName,
                    conflict.getMDColumn().data(), invalidUID--,
                    (conflict.getMDColumn() == "grantee_id") ? "grantee_name" : "grantor_name",
                    conflict.getAuthName().data());
        int cliRC1 = cliInterface->executeImmediate(queryBuf, NULL, NULL, TRUE, &rowCount);
        if (cliRC1 < 0) {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }

        uniqueConflictList.push_back(i);
        continue;
      }

      conflict.setRowCount(conflict.getRowCount() + rowCount);
    }
  }

  if (uniqueConflictList.size() != 0) {
    for (int i = 0; i < uniqueConflictList.size(); i++) {
      AuthConflict &conflict = operator[](uniqueConflictList[i]);
      if (strcmp(conflict.getMDTable().data(), objName) == 0) {
        str_sprintf(queryBuf, "update %s.\"%s\".%s set %s = %d where %s = '%s'", TRAFODION_SYSCAT_LIT, schName, objName,
                    conflict.getMDColumn().data(), conflict.getTargetID(),
                    (conflict.getMDColumn() == "grantee_id") ? "grantee_name" : "grantor_name",
                    conflict.getAuthName().data());

        int cliRC = cliInterface->executeImmediate(queryBuf, NULL, NULL, TRUE, &rowCount);
        if (cliRC < 0) {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          return -1;
        }

        conflict.setRowCount(conflict.getRowCount() + rowCount);
      }
    }
  }

  return 0;
}

// ****************************************************************************
// Class CmpSeabaseDDLauth methods
// ****************************************************************************

// ----------------------------------------------------------------------------
// Constructors
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::CmpSeabaseDDLauth(const NAString &systemCatalog, const NAString &MDSchema)
    : authID_(NA_UserIdDefault),
      authType_(COM_UNKNOWN_ID_CLASS),
      authCreator_(NA_UserIdDefault),
      authCreateTime_(0),
      authRedefTime_(0),
      authValid_(true),
      authIsAdmin_(false),
      authInTenant_(false),
      authConfig_(DEFAULT_AUTH_CONFIG),
      authAutoCreated_(false),
      authGroupList_(NULL),
      systemCatalog_(systemCatalog),
      MDSchema_(MDSchema) {
  currentAuthType_ = CmpCommon::getAuthenticationType();
}

CmpSeabaseDDLauth::CmpSeabaseDDLauth()
    : authID_(NA_UserIdDefault),
      authType_(COM_UNKNOWN_ID_CLASS),
      authCreator_(NA_UserIdDefault),
      authCreateTime_(0),
      authRedefTime_(0),
      authValid_(true),
      authIsAdmin_(false),
      authInTenant_(false),
      authConfig_(DEFAULT_AUTH_CONFIG),
      authAutoCreated_(false),
      authGroupList_(NULL),
      systemCatalog_("TRAFODION"),
      MDSchema_("TRAFODION.\"_MD_\"")

{
  currentAuthType_ = CmpCommon::getAuthenticationType();
}

CmpSeabaseDDLauth::~CmpSeabaseDDLauth() {
  if (authGroupList_) NADELETE(authGroupList_, TenantGroupInfoList, authGroupList_->getHeap());
  authGroupList_ = NULL;
}

// ----------------------------------------------------------------------------
// public method:  authExists
//
// Input:
//   authName - name to look up
//   isExternal -
//       true - the auth name is the external name (auth_ext_name)
//       false - the auth name is the database name (auth_db_name)
//
// Output:
//   Returns true if authorization row exists in the metadata
//   Returns false if authorization row does not exist in the metadata or an
//      unexpected error occurs
//
//  The diags area contains an error if any unexpected errors occurred.
//  Callers should check the diags area when false is returned
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::authExists(const NAString &authName, bool isExternal) {
  // Read the auths table to get a count of rows for the authName
  long rowCount = 0;
  try {
    NAString whereClause("where ");
    whereClause += (isExternal) ? "auth_ext_name = '" : "auth_db_name = '";
    whereClause += authName;
    whereClause += "'";
    rowCount = selectCount(whereClause);
  }

  catch (...) {
    // If there is no error in the diags area, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLauth::authExists for authName");
    return false;
  }
  return (rowCount > 0) ? true : false;
}

// ----------------------------------------------------------------------------
// method:  describe
//
// This method is not valid for the base class
//
// Input:  none
//
// Output:  populates diag area, throws exception.
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::describe(const NAString &authName, NAString &authText) {
  SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLauth::describe");

  UserException excp(NULL, 0);

  throw excp;

  return 0;
}

// -----------------------------------------------------------------------------
// method: getAdminRole
//
// returns adminRoleID that has been assigned to the passed in authID
//
// returns:
//    STATUS_NOTFOUND = could not find admin role ID
//    STATUS_GOOD     - found admin role ID
//    STATUS_ERROR    - unexpected error (ComDiags set up with error)
// -----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::getAdminRole(const int authID, int &adminRoleID) {
  // If there is no current tenant, just return
  int tenantID = ComTenant::getCurrentTenantID();
  if (tenantID == NA_UserIdDefault) return STATUS_NOTFOUND;

  adminRoleID = NA_UserIdDefault;
  std::vector<int> adminRoles;
  CmpSeabaseDDLauth::AuthStatus authStatus = STATUS_GOOD;
  authStatus = getAdminRoles(adminRoles);
  if (authStatus != STATUS_GOOD) return authStatus;
  std::vector<int> myRoles;
  std::vector<int> myGrantees;
  authStatus = getRoleIDs(authID, myRoles, myGrantees);

  // if myRoles contain an admin role - return STATUS_GOOD
  // otherwise return STATUS_NOTFOUND
  for (size_t i = 0; i < adminRoles.size(); ++i) {
    if (std::find(myRoles.begin(), myRoles.end(), adminRoles[i]) != myRoles.end()) {
      adminRoleID = adminRoles[i];
      TenantInfo roleForTenant(STMTHEAP);
      if (TenantInfo::getTenantInfo(adminRoleID, roleForTenant) == 0) {
        if (roleForTenant.getTenantID() == tenantID) return STATUS_GOOD;
      }
    }
  }
  return STATUS_NOTFOUND;
}

// ----------------------------------------------------------------------------
// method:  getAdminRoles
//
// Returns the list of role IDs for all admin roles.
// System role DB__SYSTEMTENANT is not managed by an admin role. So its role
// is not returned in this list.
//
// AuthStatus returns the result of the operation
//   STATUS_GOOD - the list is returned in roleIDs
//   STATUS_NOTFOUND - no roles were found
//   STATUS_ERROR - unexpected error, ComDiags contains the error
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::getAdminRoles(std::vector<int> &roleIDs) {
  long authFlags = 0;
  authFlags |= SEABASE_IS_ADMIN_ROLE;

  char buf[200];
  str_sprintf(buf,
              "where flags = %ld and auth_type = 'R'"
              " and auth_creator <> %d",
              authFlags, getAuthID());
  NAString whereClause(buf);
  return selectAllWhere(whereClause, roleIDs);
}

// ----------------------------------------------------------------------------
// method:  getAuthsForCreator
//
// Returns lists of users and roles created by the passed in authCreatlr
//
// AuthStatus returns the result of the operation
//   STATUS_GOOD - the lists are returned
//   STATUS_NOTFOUND - no users or roles were found
//   STATUS_ERROR - unexpected error, ComDiags contains the error
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::getAuthsForCreator(int authCreator,
                                                                    std::vector<std::string> &userNames,
                                                                    std::vector<std::string> &roleNames) {
  long authFlags = 0;
  authFlags |= SEABASE_IS_TENANT_AUTH;

  char buf[200];
  str_sprintf(buf,
              "where flags = %ld and auth_type in ('R', 'U')"
              "  and auth_creator = %d",
              authFlags, authCreator);
  NAString whereClause(buf);
  std::vector<std::string> tenantNames;
  return selectAuthNames(whereClause, userNames, roleNames, tenantNames);
}

// ----------------------------------------------------------------------------
// public method: getAuthDetails
//
// Populates the CmpSeabaseDDLauth class containing auth details for the
// requested authName
//
// Input:
//    authName - the database or external auth name
//    isExternal -
//       true - the auth name is the external name (auth_ext_name)
//       false - the auth name is the database name (auth_db_name)
//
// Output:
//    Returned parameter (AuthStatus):
//       STATUS_GOOD: authorization details are populated:
//       STATUS_NOTFOUND: authorization details were not found
//       STATUS_WARNING: (not 100) warning was returned, diags area populated
//       STATUS_ERROR: error was returned, diags area populated
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::getAuthDetails(const char *pAuthName, bool isExternal) {
  // If the authname is a special PUBLIC authorization ID, set it up
  std::string authName = pAuthName;
  if (authName == PUBLIC_AUTH_NAME) {
    setAuthCreator(SUPER_USER);
    setAuthCreateTime(0);
    setAuthDbName(PUBLIC_AUTH_NAME);
    setAuthExtName(PUBLIC_AUTH_NAME);
    setAuthID(PUBLIC_USER);
    setAuthRedefTime(0);
    setAuthType(COM_ROLE_CLASS);
    setAuthValid(false);
    setAuthIsAdmin(false);
    setAuthInTenant(false);
    setAuthConfig(DEFAULT_AUTH_CONFIG);
    return STATUS_GOOD;
  }

  try {
    NAString whereClause("where ");
    whereClause += (isExternal) ? "auth_ext_name = '" : "auth_db_name = '";
    whereClause += pAuthName;
    whereClause += "'";
    return selectExactRow(whereClause);
  }

  catch (...) {
    // If there is no error in the diags area, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLauth::getAuthDetails for authName");
    return STATUS_ERROR;
  }
}

// ----------------------------------------------------------------------------
// public method:  getAuthDetails
//
// Create the CmpSeabaseDDLauth class containing auth details for the
// request authID
//
// Input:
//    authID - the database authorization ID to search for
//
//  Output:
//    A returned parameter:
//       STATUS_GOOD: authorization details are populated:
//       STATUS_NOTFOUND: authorization details were not found
//       STATUS_WARNING: (not 100) warning was returned, diags area populated
//       STATUS_ERROR: error was returned, diags area populated
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::getAuthDetails(int authID) {
  try {
    char buf[500];
    str_sprintf(buf, "where auth_id = %d ", authID);
    NAString whereClause(buf);
    return selectExactRow(whereClause);
  }

  catch (...) {
    // If there is no error in the diags area, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLauth::getAuthDetails for authID");

    return STATUS_ERROR;
  }
}

// ----------------------------------------------------------------------------
// public method:  getRoleIDs
//
// Return the list of roles that granted to the passed in authID
//
// Input:
//    authID - the database authorization ID to search for
//
//  Output:
//    A returned parameter:
//       STATUS_GOOD: list of roles returned
//       STATUS_NOTFOUND: no roles were granted
//       STATUS_ERROR: error was returned, diags area populated
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::getRoleIDs(const int authID, std::vector<int32_t> &roleIDs,
                                                            std::vector<int32_t> &grantees, bool fetchGroupRoles) {
  NAString privMgrMDLoc;
  CONCAT_CATSCH(privMgrMDLoc, systemCatalog_.data(), SEABASE_PRIVMGR_SCHEMA);

  PrivMgrRoles role(std::string(MDSchema_.data()), std::string(privMgrMDLoc.data()), CmpCommon::diags());
  std::vector<std::string> roleNames;
  std::vector<int32_t> grantDepths;

  CompilerSwitchDDL switcher;
  short isUsingMeta = switcher.switchCompiler(CmpContextInfo::CMPCONTEXT_TYPE_META);

  // fetchGroupRoles is only valid for user
  PrivStatus privStatus =
      role.fetchRolesForAuth(authID, roleNames, roleIDs, grantDepths, grantees, isUserID(authID) && fetchGroupRoles);

  if (isUsingMeta == 0) switcher.switchBackCompiler();

  if (privStatus == PrivStatus::STATUS_ERROR) return STATUS_ERROR;

  // assert (roleIDs.size() == grantees.size());
  return STATUS_GOOD;
}

#if 0
// ----------------------------------------------------------------------------
// method:  getDependentAuths
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus 
CmpSeabaseDDLauth::getDependentAuths(
  const int authID,
  std::vector<int> &authList)
{
  NAString whereClause ("where auth_type in ('R', 'U') and auth_creator = ");
  std::string roleID = PrivMgr::authIDToString(authID);
  whereClause += roleID.c_str(); 
  return selectAllWhere(whereClause, authList);
}
#endif

// ----------------------------------------------------------------------------
// public method:  getObjectName
//
// Returns the first object name from the list of passed in objectUIDs.
//
// Input:
//    objectUIDs - list of objectUIDs
//
//  Output:
//    returns the fully qualified object name
//    returns NULL string if no objects were found
// ----------------------------------------------------------------------------
NAString CmpSeabaseDDLauth::getObjectName(const std::vector<int64_t> objectUIDs) {
  char longBuf[sizeof(int64_t) * 8 + 1];
  bool isFirst = true;
  NAString objectList;
  NAString objectName;

  if (objectUIDs.size() == 0) return objectName;

  // convert objectUIDs into an "in" clause list
  for (int i = 0; i < objectUIDs.size(); i++) {
    if (isFirst)
      objectList = "(";
    else
      objectList += ", ";
    isFirst = false;
    sprintf(longBuf, "%ld", objectUIDs[i]);
    objectList += longBuf;
  }
  objectList += ")";

  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  char buf[1000 + objectList.length()];
  str_sprintf(buf,
              "select [first 1] trim(catalog_name) || '.' || "
              "trim(schema_name) || '.' || trim(object_name) "
              " from %s.\"%s\".%s where object_uid in %s",
              sysCat.data(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS, objectList.data());

  ExeCliInterface cliInterface(STMTHEAP);
  int cliRC = cliInterface.fetchRowsPrologue(buf, true /*no exec*/);
  if (cliRC != 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }

  cliRC = cliInterface.clearExecFetchClose(NULL, 0);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }

  // return an empty string
  if (cliRC == 100) return objectName;

  // get the objectname
  char *ptr = NULL;
  int len = 0;

  // object name returned
  cliInterface.getPtrAndLen(1, ptr, len);
  NAString returnedName(ptr, len);
  return returnedName;
}

// -----------------------------------------------------------------------------
// public method: getNumberTenants
//
// Returns the number of registered tenants
// If unexpected error occurs -1 is returned and ComDiags is set up.
//
// -----------------------------------------------------------------------------
long CmpSeabaseDDLauth::getNumberTenants() {
  try {
    NAString whereClause(" WHERE AUTH_TYPE = 'T' ");
    return selectCount(whereClause);
  } catch (...) {
    // At this time, an error should be in the diags area.
    // If there is no error, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLuser::getting number tenants failed");
    return -1;
  }
}

// ----------------------------------------------------------------------------
// public method:  getTenantGroupList
//
// Returns the list of groups assigned for the requested tenant
//
// Input:
//    tenantName
//    heap to allocate space
//
//  Output:
//    groups assigned to the tenant
//    empty list if no groups are assigned the tenant
//    NULL if error (ComDiags contains details)
// ----------------------------------------------------------------------------
TenantGroupInfoList *CmpSeabaseDDLtenant::getTenantGroupList(TenantInfo &tenantInfo, NAHeap *heap) {
  if (!isTenantID(getAuthID())) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(getAuthDbName().data())
                        << DgString1("tenant");
    return NULL;
  }

  TenantGroupInfoList *authGroupList = getAuthGroupList();
  if (authGroupList) return authGroupList;

  authGroupList = new (heap) TenantGroupInfoList(heap);
  const TenantUsageList *usages = tenantInfo.getUsageList();

  if (usages) {
    // TDB: Performance enhancement, run a single query to get group info by
    //      joining between AUTHS and TENANT_USAGE
    for (CollIndex i = 0; i < usages->entries(); i++) {
      TenantUsage usage = (*tenantInfo.getUsageList())[i];
      if (usage.isGroupUsage()) {
        CmpSeabaseDDLgroup groupInfo;
        groupInfo.getAuthDetails((int)usage.getUsageID());

        TenantGroupInfo *tenantGroup = new (heap)
            TenantGroupInfo(getAuthID(), groupInfo.getAuthExtName(), groupInfo.getAuthID(), groupInfo.getAuthConfig());
        authGroupList->insert(tenantGroup);
      }
    }
  }
  setAuthGroupList(authGroupList);
  return authGroupList;
}

// ----------------------------------------------------------------------------
// method:  getUniqueAuthID
//
// Return an unused auth ID between the requested ranges
// Input:
//   minValue - the lowest value
//   maxValue - the highest value
//
// Output: unique ID to use
//   exception is generated if unable to generate a unique value
// ----------------------------------------------------------------------------
int CmpSeabaseDDLauth::getUniqueAuthID(const int minValue, const int maxValue) {
  int newUserID = 0;
  int currentMaxUserID = 0;
  char buf[300];
  int len = 0;
  // If root auth does not exist, use it.
  if (isUserGroup() || isTenant()) {
    int len = (MAX_AUTHID_AS_STRING_LEN * 2) + 200;
    char buf[len];
    snprintf(buf, len, "where auth_id = %d", minValue);
    NAString whereClause(buf);
    if (selectCount(whereClause) == 0) return minValue;
  }

  // get cqd
  int cliRC = 0;
  long metadataValue = 0;
  bool nullTerminate = false;
  int specialID = SUPER_USER;

  ExeCliInterface cliInterface(STMTHEAP);
  if (isUser()) {
    memset(buf, 0, 300);
    len = snprintf(buf, 300, "SELECT sub_id FROM %s.%s where text_uid = %d and text_type = %d;", MDSchema_.data(),
                   SEABASE_TEXT, specialID, COM_USER_PASSWORD);
    assert(len <= 300);

    cliRC = cliInterface.executeImmediate(buf, (char *)&metadataValue, &len, nullTerminate);
    currentMaxUserID = (cliRC == 0) ? (int)metadataValue : 0;
    if (0 == currentMaxUserID) {
      currentMaxUserID = getMaxUserIdInUsed(&cliInterface);
    }
  }
  if (CmpCommon::getDefault(UNIQUE_USERID) == DF_ON && isUser()) {
    newUserID = currentMaxUserID ? currentMaxUserID + 1 : 0;
  }

  if (CmpCommon::getDefault(UNIQUE_USERID) != DF_ON || newUserID == 0) {
    memset(buf, 0, 300);
    len = snprintf(buf, 300,
                   "SELECT [FIRST 1] auth_id FROM (SELECT auth_id, "
                   "LEAD(auth_id) OVER (ORDER BY auth_id) L FROM %s.%s ) "
                   "WHERE (L - auth_id > 1 or L is null) and auth_id >= %d ",
                   MDSchema_.data(), SEABASE_AUTHS, minValue);
    assert(len <= 300);

    len = 0;
    long metadataValue = 0;
    bool nullTerminate = false;

    cliRC = cliInterface.executeImmediate(buf, (char *)&metadataValue, &len, nullTerminate);
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return 0;
    }

    newUserID = (int)metadataValue;
    if (newUserID == 0)
      newUserID = minValue;
    else
      newUserID++;
  }

  if (isUser() && CmpCommon::getDefault(UNIQUE_USERID) == DF_ON && currentMaxUserID && newUserID > currentMaxUserID &&
      newUserID < MAX_USERID) {
    memset(buf, 0, 300);
    len = snprintf(buf, 300, "update %s.%s set sub_id = %d where text_uid = %d and text_type = %d; ", MDSchema_.data(),
                   SEABASE_TEXT, newUserID, specialID, COM_USER_PASSWORD);
    assert(len <= 300);
    cliInterface.executeImmediate(buf);
  }

  // We have lots of available ID's.  Don't expect to run out of ID's for awhile
  if (cliRC == 100 || (metadataValue > maxValue)) {
    SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLauth::getUniqueAuthID failed, ran out of available IDs");
    UserException excp(NULL, 0);
    throw excp;
  }

  // There is a bug where grants are not being removed from component privileges
  // when a user is dropped.  So if this authID still shows up as a component
  // privilege grantee go ahead a cleanup the inconsistency.
  std::string privMDLoc(CmpSeabaseDDL::getSystemCatalogStatic().data());
  privMDLoc += std::string(".\"") + std::string(SEABASE_PRIVMGR_SCHEMA) + std::string("\"");

  PrivMgrComponentPrivileges componentPrivs(privMDLoc, CmpCommon::diags());
  if (componentPrivs.isAuthIDGrantedPrivs(newUserID)) {
    if (!componentPrivs.dropAllForGrantee(newUserID)) {
      *CmpCommon::diags() << DgSqlCode(CAT_WARN_USED_AUTHID) << DgInt0(newUserID);

      int newMinValue = newUserID + 1;
      newUserID = getUniqueAuthID(newUserID + 1, maxValue);
    }
  }

  return newUserID;
}

// ----------------------------------------------------------------------------
// method:  isAdminAuthID
//
// Checks to see if the passed in authCreator is actually an admin role
//  true - is an admin role
//  false - not an admin role
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::isAdminAuthID(const int authCreator) {
  std::vector<int> adminRoles;
  CmpSeabaseDDLauth::AuthStatus authStatus = getAdminRoles(adminRoles);
  if (authStatus != AuthStatus::STATUS_GOOD) return false;
  if (std::find(adminRoles.begin(), adminRoles.end(), authCreator) != adminRoles.end()) return true;
  return false;
}

// ----------------------------------------------------------------------------
// method: isAuthNameReserved
//
// Checks to see if proposed name is reserved
//
// Input: authorization name
//
// Output:
//   true - name is reserved
//   false - name is not reserved
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::isAuthNameReserved(const NAString &authName) {
  bool result;
  result = authName.length() >= strlen(RESERVED_AUTH_NAME_PREFIX) &&
               authName.operator()(0, strlen(RESERVED_AUTH_NAME_PREFIX)) == RESERVED_AUTH_NAME_PREFIX ||
           authName == SYSTEM_AUTH_NAME || authName == PUBLIC_AUTH_NAME || authName == "NONE";

  return result;
}

// ----------------------------------------------------------------------------
// method: isAuthNameValid
//
// checks to see if the name contains valid character
//
// Input:
//    NAString - authName -- name string to check
//
// Returns:  true if valid, false otherwise
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::isAuthNameValid(const NAString &authName) {
  string validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_@./";
  string strToScan = authName.data();
  size_t found = strToScan.find_first_not_of(validChars);
  if (found == string::npos) return true;
  return false;
}

// ----------------------------------------------------------------------------
// method: isPasswordValid
//
// checks to see if the password contains valid character and rule
//
// Input:
//    NAString - password -- password string to check
//
// Returns:  true if valid, false otherwise
// ----------------------------------------------------------------------------

#define CHECK_FOR_ITEM(counter, item)         \
  {                                           \
    if (0xff != item) {                       \
      if ((0xee == item) && (0 != counter)) { \
        errorFlag = false;                    \
      } else if ((0xee != item)) {            \
        if ((0 == item) && (0 == counter)) {  \
          errorFlag = false;                  \
        } else if (counter < item) {          \
          errorFlag = false;                  \
        }                                     \
      }                                       \
    }                                         \
  }

#define HAS_THIS_CHAR_TYPE(counter, flag) \
  {                                       \
    if (counter > 0) {                    \
      flag++;                             \
    }                                     \
  }

bool CmpSeabaseDDLauth::isPasswordValid(const NAString &password) {
  size_t i = 0;
  size_t UpperCounter = 0;
  size_t LowerCounter = 0;
  size_t DigitCounter = 0;
  size_t SymbolCounter = 0;
  size_t len = password.length();

  if (COM_LDAP_AUTH == currentAuthType_) {
    return true;
  }

  // default password for local auth
  if (0 == strcmp(AUTH_DEFAULT_WORD, password.data())) {
    return true;
  }

  ComPwdPolicySyntax syntax = CmpCommon::getPasswordCheckSyntax();

  // check for invaild char
  string validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_@./";
  string strToScan = password.data();
  size_t found = strToScan.find_first_not_of(validChars);
  if (found != string::npos) return false;

  // Not check for password quality
  if (syntax.max_password_length == 0xff) {
    // limit length of password
    return len > MAX_PASSWORD_LENGTH ? false : true;
  }

  // check length for password
  if (len < syntax.min_password_length || len > syntax.max_password_length) {
    return false;
  }

  // compute number of chars
  for (i = 0; i < len; i++) {
    if (strToScan[i] >= 'A' && strToScan[i] <= 'Z') {
      UpperCounter++;
    } else if (strToScan[i] >= 'a' && strToScan[i] <= 'z') {
      LowerCounter++;
    } else if (strToScan[i] >= '0' && strToScan[i] <= '9') {
      DigitCounter++;
    } else
      SymbolCounter++;
  }

  // check number
  // the minimum password quality check
  if (syntax.digital_chars_number == 0xff && syntax.lower_chars_number == 0xff && syntax.symbol_chars_number == 0xff &&
      syntax.upper_chars_number == 0xff) {
    // must contain 3 different types of character.
    int pwdCharType = 0;
    HAS_THIS_CHAR_TYPE(UpperCounter, pwdCharType);
    HAS_THIS_CHAR_TYPE(LowerCounter, pwdCharType);
    HAS_THIS_CHAR_TYPE(DigitCounter, pwdCharType);
    HAS_THIS_CHAR_TYPE(SymbolCounter, pwdCharType);
    return pwdCharType >= 3 ? true : false;
  }
  // the complex password quality check
  else {
    bool errorFlag = true;
    CHECK_FOR_ITEM(UpperCounter, syntax.upper_chars_number);
    CHECK_FOR_ITEM(LowerCounter, syntax.lower_chars_number);
    CHECK_FOR_ITEM(DigitCounter, syntax.digital_chars_number);
    CHECK_FOR_ITEM(SymbolCounter, syntax.symbol_chars_number);
    return errorFlag;
  }
}

// ----------------------------------------------------------------------------
// method: isRoleID
//
// Determines if an authID is in the role ID range
//
// Input:
//    int - authID -- numeric ID to check
//
// Returns:  true if authID is a role ID, false otherwise
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::isRoleID(int authID) { return (authID >= MIN_ROLEID && authID <= MAX_ROLEID); }

// ----------------------------------------------------------------------------
// method: isUserID
//
// Determines if an authID is in the user ID range
//
// Input:
//    int - authID -- numeric ID to check
//
// Returns:  true if authID is a user ID, false otherwise
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::isUserID(int authID) { return (authID >= MIN_USERID && authID <= MAX_USERID); }

// ----------------------------------------------------------------------------
// method: isTenantID
//
// Determines if an authID is in the user ID range
//
// Input:
//    int - authID -- numeric ID to check
//
// Returns:  true if authID is a user ID, false otherwise
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::isTenantID(int authID) { return (authID >= MIN_TENANTID && authID <= MAX_TENANTID); }

// ----------------------------------------------------------------------------
// method: isUserGroup
//
// Determines if an authID is in the user group ID range
//
// Input:
//    int - authID -- numeric ID to check
//
// Returns:  true if authID is a user ID, false otherwise
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::isUserGroupID(int authID) { return (authID >= MIN_USERGROUP && authID <= MAX_USERGROUP); }

// ----------------------------------------------------------------------------
// method: isSystemAuth
//
// Checks the list of authorization IDs to see if the passed in authName is a
//   system auth. This replaces checks for reserved names.
//
// isSpecialAuth indicates a system auth but it is not defined in the metadata
//
// Returns:
//    true - is a system auth
//    false - is not a system auth
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::isSystemAuth(const ComIdClass authType, const NAString &authName, bool &isSpecialAuth) {
  bool isSystem = false;
  switch (authType) {
    case COM_TENANT_CLASS: {
      int32_t numberTenants = sizeof(systemTenants) / sizeof(SystemAuthsStruct);
      for (int32_t i = 0; i < numberTenants; i++) {
        const SystemAuthsStruct &tenantDefinition = systemTenants[i];
        if (tenantDefinition.authName == authName) {
          isSystem = true;
          isSpecialAuth = tenantDefinition.isSpecialAuth;
          break;
        }
      }
      break;
    }

    case COM_ROLE_CLASS: {
      int32_t numberRoles = sizeof(systemRoles) / sizeof(SystemAuthsStruct);
      for (int32_t i = 0; i < numberRoles; i++) {
        const SystemAuthsStruct &roleDefinition = systemRoles[i];
        if (roleDefinition.authName == authName) {
          isSystem = true;
          isSpecialAuth = roleDefinition.isSpecialAuth;
          break;
        }
      }
      break;
    }

    case COM_USER_CLASS: {
      // Verify name is a standard name
      std::string authNameStr(authName.data());
      size_t prefixLength = strlen(RESERVED_AUTH_NAME_PREFIX);
      if (authNameStr.size() <= prefixLength || authNameStr.compare(0, prefixLength, RESERVED_AUTH_NAME_PREFIX) == 0)
        isSystem = true;
      break;
    }

    case COM_USER_GROUP_CLASS: {
      // At this time, there are no system groups
      isSystem = false;
      break;
    }
    default: {
      // should never get here - assert?
      isSystem = false;
    }
  }
  return isSystem;
}

// ----------------------------------------------------------------------------
// protected method: createStandardAuth
//
// Inserts a standard user or role in the Trafodion metadata
// The authType needs to be set up before calling
//
// Input:
//    authName
//    authID
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::createStandardAuth(const std::string authName, const int32_t authID) {
  // check to see if authName is a system object
  bool isSpecialAuth = false;
  bool isSystem = isSystemAuth(getAuthType(), NAString(authName.c_str()), isSpecialAuth);

  // since this is being called by internal code, should not be trying to
  // create non system object (isSystemAuth) or object that should not be
  // registered in the metadata (isSpecialAuth), return internal error
  if (!isSystem || isSpecialAuth) {
    NAString errorMsg("Invalid system authorization identifier for ");
    errorMsg += getAuthType() == COM_TENANT_CLASS ? "tenant " : getAuthType() == COM_ROLE_CLASS ? "role " : "user ";
    errorMsg += authName.c_str();
    SEABASEDDL_INTERNAL_ERROR(errorMsg.data());
    return false;
  }

  setAuthDbName(authName.c_str());
  setAuthExtName(authName.c_str());
  setAuthValid(true);  // assume a valid authorization ID
  setAuthIsAdmin(false);
  setAuthInTenant(false);
  setAuthConfig(DEFAULT_AUTH_CONFIG);

  long createTime = NA_JulianTimestamp();
  setAuthCreateTime(createTime);
  setAuthRedefTime(createTime);  // make redef time the same as create time

  // Make sure authorization ID has not already been registered
  if (authExists(getAuthDbName(), false)) return false;

  if (isUser()) {
    int rc = 0;
    int cryptlen = 0;
    // base64 maybe make encrypted data longer
    char dePassword[2 * (MAX_PASSWORD_LENGTH + EACH_ENCRYPTION_LENGTH) + 1] = {0};

    rc = ComEncryption::encrypt_ConvertBase64_Data(
        ComEncryption::AES_128_CBC, (unsigned char *)(AUTH_DEFAULT_WORD), strlen(AUTH_DEFAULT_WORD),
        (unsigned char *)AUTH_PASSWORD_KEY, (unsigned char *)AUTH_PASSWORD_IV, (unsigned char *)dePassword, cryptlen);
    if (rc) {
      SEABASEDDL_INTERNAL_ERROR("encryption user password error in CmpSeabaseDDLuser::createStandardAuth");
      return false;
    }

    setAuthPassword(dePassword);
  }

  try {
    int minAuthID = 0;
    int maxAuthID = 0;
    switch (getAuthType()) {
      case COM_USER_CLASS:
        minAuthID = MIN_USERID;
        maxAuthID = MAX_USERID;
        break;
      case COM_ROLE_CLASS:
        minAuthID = MIN_ROLEID;
        maxAuthID = MAX_ROLEID_RANGE1;
        break;
      case COM_USER_GROUP_CLASS:
        minAuthID = MIN_USERGROUP;
        maxAuthID = MAX_USERGROUP;
        break;
      case COM_TENANT_CLASS:
        minAuthID = MIN_TENANTID;
        maxAuthID = MAX_TENANTID;
        break;
      default:
        SEABASEDDL_INTERNAL_ERROR("Switch statement in CmpSeabaseDDLuser::insertRow invalid authType");
        UserException excp(NULL, 0);
        throw excp;
    }

    int newAuthID = (authID == NA_UserIdDefault) ? getUniqueAuthID(minAuthID, maxAuthID) : authID;

    setAuthID(newAuthID);
    setAuthCreator(ComUser::getRootUserID());

    // Add the role to AUTHS table
    insertRow();
  }

  catch (...) {
    // At this time, an error should be in the diags area.
    // If there is no error, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("Unexpected error in CmpSeabaseDDLuser::createStandardAuth");
  }
  return true;
}

//-----------------------------------------------------------------------------
// Methods that perform metadata access
//
// All methods return a UserException if an unexpected error occurs
//-----------------------------------------------------------------------------

// Delete a row from the AUTHS table based on the auth name
void CmpSeabaseDDLauth::deleteRow(const NAString &authName) {
  NAString systemCatalog = CmpSeabaseDDL::getSystemCatalogStatic();
  char buf[1000];
  ExeCliInterface cliInterface(STMTHEAP);

  // delete password
  str_sprintf(buf,
              "delete from %s.\"%s\".%s where text_uid in (select \
                    auth_id from %s.\"%s\".%s where auth_db_name = '%s') and text_type = %d; ",
              systemCatalog.data(), SEABASE_MD_SCHEMA, SEABASE_TEXT, systemCatalog.data(), SEABASE_MD_SCHEMA,
              SEABASE_AUTHS, authName.data(), COM_USER_PASSWORD);

  int cliRC = cliInterface.executeImmediate(buf);

  if (cliRC > -1) {
    str_sprintf(buf, "delete from %s.\"%s\".%s where auth_db_name = '%s'", systemCatalog.data(), SEABASE_MD_SCHEMA,
                SEABASE_AUTHS, authName.data());
    cliRC = cliInterface.executeImmediate(buf);
  }

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }

  // Existence of the row is already checked, throw an assertion the row does not exist
  // With optimistic locking, this might occur if a concurrent session is deleting the
  // same row and gets in first
  // CMPASSERT (cliRC == 100);

  // Not sure if it is possible to get a warning from a delete and
  // what it means if one is returned for now, it is ignored.
}

short CmpSeabaseDDLauth::updateSeabaseXDCDDLTable() {
  if (ddlQuery_ == NULL) {
    QRLogger::log(CAT_SQL_EXE, LL_WARN, "XDC DDL Query is null, nothing to update");
    return 0;
  }

  if (CmpCommon::getDefault(DONOT_WRITE_XDC_DDL) == DF_ON) {
    return 0;
  }
  ExeCliInterface cliInterface(STMTHEAP);
  NAString quotedSchName;
  ToQuotedString(quotedSchName, NAString(XDC_AUTH_SCHEMA), FALSE);
  NAString quotedObjName;
  ToQuotedString(quotedObjName, NAString(XDC_AUTH_TABLE), FALSE);

  int cliRC;

  int dataLen = ddlLength_;
  char *dataToInsert = ddlQuery_;
  int maxLen = 10000;
  char queryBuf[1000];
  int numRows = ((dataLen - 1) / maxLen) + 1;
  int currPos = 0;
  int currDataLen = 0;

  for (int i = 0; i < numRows; i++) {
    if (i < numRows - 1)
      currDataLen = maxLen;
    else
      currDataLen = dataLen - currPos;

    str_sprintf(queryBuf,
                "upsert into %s.\"%s\".%s values ('%s', '%s', '%s', '%s', %d, cast(? as char(%d bytes) character set "
                "utf8 not null))",
                TRAFODION_SYSCAT_LIT, SEABASE_XDC_MD_SCHEMA, XDC_DDL_TABLE, TRAFODION_SYSCAT_LIT, quotedSchName.data(),
                quotedObjName.data(), COM_XDC_AUTH_OBJ_TYPE_LIT, i, currDataLen);
    cliRC = cliInterface.executeImmediateCEFC(queryBuf, &dataToInsert[currPos], currDataLen, NULL, NULL, NULL);

    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return -1;
    }

    currPos += maxLen;
  }

  return 0;
}
// Insert a row into the AUTHS table
void CmpSeabaseDDLauth::insertRow() {
  char buf[1000];
  ExeCliInterface cliInterface(STMTHEAP);

  NAString authType;
  switch (getAuthType()) {
    case COM_ROLE_CLASS:
      authType = COM_ROLE_CLASS_LIT;
      break;
    case COM_USER_CLASS:
      authType = COM_USER_CLASS_LIT;
      break;
    case COM_USER_GROUP_CLASS:
      authType = COM_USER_GROUP_CLASS_LIT;
      break;
    case COM_TENANT_CLASS:
      authType = COM_TENANT_CLASS_LIT;
      break;
    default:
      SEABASEDDL_INTERNAL_ERROR("Switch statement in CmpSeabaseDDLuser::insertRow invalid authType");
      UserException excp(NULL, 0);
      throw excp;
  }

  NAString authValid = isAuthValid() ? "Y" : "N";

  // add config number first
  // config resides in bits 3 - 6
  long authFlags = 0;
  if (getAuthConfig() != DEFAULT_AUTH_CONFIG) {
    Int16 config = getAuthConfig();
    config = config << 2;  // shift over bits 1 and 2
    authFlags = config;
  }

  // Add remaining flags
  if (isAuthAdmin()) authFlags |= SEABASE_IS_ADMIN_ROLE;
  if (isAuthInTenant()) authFlags |= SEABASE_IS_TENANT_AUTH;
  if (isAutoCreated()) authFlags |= SEABASE_IS_AUTO_CREATED;

  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  str_sprintf(buf, "insert into %s.\"%s\".%s values (%d, '%s', '%s', '%s', %d, '%s', %ld, %ld, %ld)", sysCat.data(),
              SEABASE_MD_SCHEMA, SEABASE_AUTHS, getAuthID(), getAuthDbName().data(), getAuthExtName().data(),
              authType.data(), getAuthCreator(), authValid.data(), getAuthCreateTime(), getAuthRedefTime(), authFlags);

  int cliRC = cliInterface.executeImmediate(buf);

  // add password into SEABASE_TEXT table
  if (cliRC > -1 && authType == COM_USER_CLASS_LIT) {
    // replace insert to upsert in case of mantis 20229
    str_sprintf(buf, "upsert into  %s.\"%s\".%s values (%d, %d, 0, 0, %ld, '%s')", sysCat.data(), SEABASE_MD_SCHEMA,
                SEABASE_TEXT, getAuthID(), COM_USER_PASSWORD, getAuthCreateTime(), getAuthPassword().data());

    cliRC = cliInterface.executeImmediate(buf);
  }

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }

  // Not sure if it is possible to get a warning from an insert and
  // what it means if one is returned, for now it is ignored.
}

// update a row in AUTHS table based on the passed in setClause
void CmpSeabaseDDLauth::updateRow(NAString &setClause) {
  char buf[1000];
  ExeCliInterface cliInterface(STMTHEAP);

  // Always change the redefinition timestamp
  if (setClause.length() > 0 && strcmp(setClause.data(), "set ") != 0)
    setClause += ", ";
  else
    setClause = "set ";

  long redefTime = NA_JulianTimestamp();
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  str_sprintf(buf, "update %s.\"%s\".%s %s auth_redef_time = %ld where auth_id = %d", sysCat.data(), SEABASE_MD_SCHEMA,
              SEABASE_AUTHS, setClause.data(), redefTime, getAuthID());

  int cliRC = cliInterface.executeImmediate(buf);

  // update password
  if (cliRC > -1) {
    NAString authPassword = getAuthPassword();
    if (authPassword && !authPassword.isNull()) {
      str_sprintf(buf, "update  %s.\"%s\".%s set text = '%s', flags = %ld where text_uid = %d and text_type = %d",
                  sysCat.data(), SEABASE_MD_SCHEMA, SEABASE_TEXT, getAuthPassword().data(), redefTime, getAuthID(),
                  COM_USER_PASSWORD);
    }
    cliRC = cliInterface.executeImmediate(buf);
  }

  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }

  // Existence of the row is already checked, throw an assertion the row does not exist
  // With optimistic locking, this might occur if a concurrent session is deleting the
  // same row and gets in first
  CMPASSERT(cliRC == 100);

  // Not sure if it is possible to get a warning from an update and
  // what it means if one is returned, for now it is ignored.
}

static int getSeqnumFromText(int userid) {
  char buf[1000];
  memset(buf, 0, 1000);
  ExeCliInterface cliInterface(STMTHEAP);
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();

  str_sprintf(buf, "select SEQ_NUM from %s.\"%s\".%s where TEXT_UID = %d and text_type = %d", sysCat.data(),
              SEABASE_MD_SCHEMA, SEABASE_TEXT, userid, COM_USER_PASSWORD);

  Queue *cntQueue = NULL;
  int cliRC = cliInterface.fetchAllRows(cntQueue, buf, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }

  if (cliRC == 100 || cntQueue->numEntries() == 0) {
    memset(buf, 0, 1000);
    str_sprintf(buf, "insert into  %s.\"%s\".%s values (%d, %d, 0, 0, 0, '')", sysCat.data(), SEABASE_MD_SCHEMA,
                SEABASE_TEXT, userid, COM_USER_PASSWORD);

    cliRC = cliInterface.executeImmediate(buf);
    if (cliRC < 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      UserException excp(NULL, 0);
      throw excp;
    }
    return 0;
  }

  cntQueue->position();
  OutputInfo *pCliRow = (OutputInfo *)cntQueue->getNext();

  return *(int *)pCliRow->get(0);
}

int CmpSeabaseDDLauth::getErrPwdCnt(int userid, Int16 &errcnt) {
  int val = getSeqnumFromText(userid);
  if (0 != val) {
    Int16 *ptr = (Int16 *)&val;
    if (is_little_endian()) {
      errcnt = *(ptr + 1);
    } else {
      // little -> big
      Int8 *ptr1 = (Int8 *)ptr;
      errcnt = (*ptr1 << 8) | (*(ptr1 + 1) >> 8);
    }
  } else {
    errcnt = 0;
  }
  return 0;
}

int CmpSeabaseDDLauth::updateErrPwdCnt(int userid, Int16 errcnt, bool reset) {
  char buf[1000];
  memset(buf, 0, 1000);
  int val, val2;
  bool isLittle = is_little_endian();

  val = getSeqnumFromText(userid);
  val2 = val;

  errcnt = reset ? 0 : errcnt;

  if (isLittle) {
    ((Int16 *)&val)[1] = errcnt;
  } else {
    ((Int16 *)&val)[0] = errcnt;
  }

  if (val2 == val) {
    // not change
    return 0;
  }

  if (!isLittle) {
    val = swapByteOrderOfS((unsigned int)val);
  }

  ExeCliInterface cliInterface(STMTHEAP);
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  str_sprintf(buf, "update  %s.\"%s\".%s set SEQ_NUM = %d where text_uid = %d and text_type = %d", sysCat.data(),
              SEABASE_MD_SCHEMA, SEABASE_TEXT, val, userid, COM_USER_PASSWORD);
  int cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }

  return cliRC;
}

// change the redefintion time stamp in auths table
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::updateRedefTime() {
  char buf[1000];
  ExeCliInterface cliInterface(STMTHEAP);

  long redefTime = NA_JulianTimestamp();
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  str_sprintf(buf, "update %s.\"%s\".%s set auth_redef_time = %ld where auth_id = %d", sysCat.data(), SEABASE_MD_SCHEMA,
              SEABASE_AUTHS, redefTime, getAuthID());

  int cliRC = cliInterface.executeImmediate(buf);

  if (cliRC < 0)
    return STATUS_ERROR;
  else if (cliRC == 100)
    return STATUS_NOTFOUND;
  return STATUS_GOOD;
}

// -----------------------------------------------------------------------------
// method: selectAuthIDs
//
// Returns a list of authIDs that match the passed in where clause.
// -----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::selectAuthIDs(const NAString &whereClause,
                                                               std::set<int32_t> &authIDs) {
  char buf[whereClause.length() + 500];
  snprintf(buf, sizeof(buf), "select auth_id from %s.%s %s ", MDSchema_.data(), SEABASE_AUTHS, whereClause.data());

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  Queue *queue = NULL;

  // set pointer in diags area
  int32_t diagsMark = CmpCommon::diags()->mark();

  int cliRC = cliInterface.fetchAllRows(queue, (char *)buf, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return STATUS_ERROR;
  }

  if (cliRC == 100)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  queue->position();
  int authID;
  char type[3];
  for (int idx = 0; idx < queue->numEntries(); idx++) {
    OutputInfo *pCliRow = (OutputInfo *)queue->getNext();
    authID = (*(int *)pCliRow->get(0));
    authIDs.insert(authID);
  }
  return STATUS_GOOD;
}

// -----------------------------------------------------------------------------
// method: selectAuthIDAndAuthName
//
// Returns a list of pair for authID with authName that match the passed in where clause.
// -----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::selectAuthIDAndNames(
    const NAString &whereClause, std::map<const char *, int32_t, Char_Compare> &nameAndIDs, bool consistentCheck) {
  char buf[whereClause.length() + 500];
  snprintf(buf, sizeof(buf), "select distinct auth_id,auth_db_name from %s.%s %s ", MDSchema_.data(), SEABASE_AUTHS,
           whereClause.data());

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  Queue *queue = NULL;

  // set pointer in diags area
  int32_t diagsMark = CmpCommon::diags()->mark();

  int cliRC = cliInterface.fetchAllRows(queue, (char *)buf, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return STATUS_ERROR;
  }

  if (cliRC == 100)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  queue->position();
  int authID;
  char *authName;
  for (int idx = 0; idx < queue->numEntries(); idx++) {
    OutputInfo *pCliRow = (OutputInfo *)queue->getNext();
    authID = (*(int *)pCliRow->get(0));
    authName = (char *)pCliRow->get(1);
    if (consistentCheck) {
      if (nameAndIDs.count(authName) > 0) {
        nameAndIDs[authName] = authID;
      }
    } else {
      char *tmp = new (STMTHEAP) char[strlen(authName) + 1];
      strcpy(tmp, authName);
      nameAndIDs.insert(std::pair<const char *, int32_t>(tmp, authID));
    }
  }
  return STATUS_GOOD;
}

// select auth name  lists based on the passed in whereClause
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::selectAuthNames(const NAString &whereClause,
                                                                 std::vector<std::string> &userNames,
                                                                 std::vector<std::string> &roleNames,
                                                                 std::vector<std::string> &tenantNames) {
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  int stmtSize = whereClause.length() + 500;
  char buf[stmtSize];
  str_sprintf(buf, "select auth_db_name, auth_type from %s.%s %s", MDSchema_.data(), SEABASE_AUTHS, whereClause.data());

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  Queue *queue = NULL;

  // set pointer in diags area
  int32_t diagsMark = CmpCommon::diags()->mark();

  int32_t cliRC = cliInterface.fetchAllRows(queue, (char *)buf, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return STATUS_ERROR;
  }

  if (cliRC == 100)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  queue->position();
  char type[3];
  for (int idx = 0; idx < queue->numEntries(); idx++) {
    OutputInfo *pCliRow = (OutputInfo *)queue->getNext();
    std::string authName = (char *)pCliRow->get(0);
    char *authType = (char *)pCliRow->get(1);

    if (authType[0] == 'U')
      userNames.push_back(authName);
    else if (authType[0] = 'R')
      roleNames.push_back(authName);
    else if (authType[0] = 'T')
      tenantNames.push_back(authName);
    // Ignore any auth types that are not recognizable
  }
  return STATUS_GOOD;
}

// select all based on the passed in whereClause
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::selectAllWhere(const NAString &whereClause,
                                                                std::vector<int> &roleIDs) {
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  int stmtSize = whereClause.length() + 500;
  char buf[stmtSize];
  str_sprintf(buf, "select auth_id from %s.%s %s", MDSchema_.data(), SEABASE_AUTHS, whereClause.data());

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  Queue *queue = NULL;

  // set pointer in diags area
  int32_t diagsMark = CmpCommon::diags()->mark();

  int32_t cliRC = cliInterface.fetchAllRows(queue, (char *)buf, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return STATUS_ERROR;
  }

  if (cliRC == 100)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  queue->position();
  for (int idx = 0; idx < queue->numEntries(); idx++) {
    char *ptr = NULL;
    int len = 0;
    OutputInfo *pCliRow = (OutputInfo *)queue->getNext();
    pCliRow->get(0, ptr, len);
    int roleID = *(reinterpret_cast<int32_t *>(ptr));
    roleIDs.push_back(roleID);
  }
  return STATUS_GOOD;
}

// select exact based on the passed in whereClause
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::selectExactRow(const NAString &whereClause) {
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  char buf[1000];
  str_sprintf(buf,
              "select auth_id, auth_db_name, auth_ext_name, auth_type, "
              "auth_creator, auth_is_valid, auth_create_time, auth_redef_time, "
              "flags from %s.%s %s ",
              MDSchema_.data(), SEABASE_AUTHS, whereClause.data());
  NAString cmd(buf);

  ExeCliInterface cliInterface(STMTHEAP);

  int cliRC = cliInterface.fetchRowsPrologue(cmd.data(), true /*no exec*/);
  if (cliRC != 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    DDLException excp(cliRC, NULL, 0);
    throw excp;
  }

  cliRC = cliInterface.clearExecFetchClose(NULL, 0);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    DDLException excp(cliRC, NULL, 0);
    throw excp;
  }

  if (cliRC == 100)  // did not find the row
  {
    cliInterface.clearGlobalDiags();
    return STATUS_NOTFOUND;
  }

  // Set the return status
  CmpSeabaseDDLauth::AuthStatus authStatus = (cliRC == 0) ? STATUS_GOOD : STATUS_WARNING;

  // Populate the class
  char *ptr = NULL;
  int len = 0;
  char type[6];

  // value 1:  auth_id (int32)
  cliInterface.getPtrAndLen(1, ptr, len);
  setAuthID(*(int *)ptr);

  // value 2: auth_db_name (NAString)
  cliInterface.getPtrAndLen(2, ptr, len);
  NAString dbName(ptr, len);
  setAuthDbName(dbName);

  // value 3: auth_ext_name (NAString)
  cliInterface.getPtrAndLen(3, ptr, len);
  NAString extName(ptr, len);
  setAuthExtName(extName);

  // value 4: auth_type (char)
  // str_cpy_and_null params: *tgt, *src, len, endchar, blank, null term
  cliInterface.getPtrAndLen(4, ptr, len);
  str_cpy_and_null(type, ptr, len, '\0', ' ', true);
  if (type[0] == 'U')
    setAuthType(COM_USER_CLASS);
  else if (type[0] == 'G')
    setAuthType(COM_USER_GROUP_CLASS);
  else if (type[0] == 'R')
    setAuthType(COM_ROLE_CLASS);
  else if (type[0] == 'T')
    setAuthType(COM_TENANT_CLASS);
  else
    setAuthType(COM_UNKNOWN_ID_CLASS);

  // value 5: auth_creator (int32)
  cliInterface.getPtrAndLen(5, ptr, len);
  setAuthCreator(*(int *)ptr);

  // value 6: auth_is_valid (char)
  cliInterface.getPtrAndLen(6, ptr, len);
  str_cpy_and_null(type, ptr, len, '\0', ' ', true);
  if (type[0] == 'Y')
    setAuthValid(true);
  else
    setAuthValid(false);

  // value 7: auth_create_time (int64)
  cliInterface.getPtrAndLen(7, ptr, len);
  long intValue = *(long *)ptr;
  setAuthCreateTime((ComTimestamp)intValue);

  // value 8: auth_redef_time (int64)
  cliInterface.getPtrAndLen(8, ptr, len);
  intValue = *(long *)ptr;
  setAuthRedefTime((ComTimestamp)intValue);

  // value 9: flags (int64)
  cliInterface.getPtrAndLen(9, ptr, len);
  intValue = *(long *)ptr;
  setAuthIsAdmin(intValue & SEABASE_IS_ADMIN_ROLE);
  setAuthInTenant(intValue & SEABASE_IS_TENANT_AUTH);
  setAutoCreated(intValue & SEABASE_IS_AUTO_CREATED);

  long mask = 60;             // (3C hex) config number stored in bits 3 - 6
  intValue = intValue & mask;  // turn off all other bits
  intValue = intValue >> 2;    // shift over bits 1 & 2
  setAuthConfig((Int16)intValue);

  cliInterface.fetchRowsEpilogue(NULL, true);
  if (isUser()) {
    memset(buf, 0, sizeof(buf));
    str_sprintf(buf, "select text, flags from %s.%s where text_uid = %d and text_type = %d ", MDSchema_.data(),
                SEABASE_TEXT, getAuthID(), COM_USER_PASSWORD);
    NAString cmd(buf);
    ExeCliInterface subCliInterface(STMTHEAP);
    cliRC = subCliInterface.fetchRowsPrologue(cmd.data(), true);
    if (cliRC != 0) {
      subCliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      DDLException excp(cliRC, NULL, 0);
      throw excp;
    }

    cliRC = subCliInterface.clearExecFetchClose(NULL, 0);
    if (cliRC < 0) {
      subCliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      DDLException excp(cliRC, NULL, 0);
      throw excp;
    }

    if (cliRC == 100)  // did not find the row
    {
      setAuthPassword(AUTH_DEFAULT_WORD);
      setPasswdModTime(NA_JulianTimestamp());
      subCliInterface.clearGlobalDiags();
    } else {
      // value 1: userpassword (char[])
      subCliInterface.getPtrAndLen(1, ptr, len);
      NAString password(ptr, len);
      setAuthPassword(password);

      // value 2: passwordLifeTime_ (int64)
      subCliInterface.getPtrAndLen(2, ptr, len);
      intValue = *(long *)ptr;
      setPasswdModTime((ComTimestamp)intValue);
      subCliInterface.fetchRowsEpilogue(NULL, true);
    }
  }
  return authStatus;
}

// selectCount - returns the number of rows based on the where clause
long CmpSeabaseDDLauth::selectCount(const NAString &whereClause) {
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  char buf[1000];
  str_sprintf(buf, "select count(*) from %s.%s %s ", MDSchema_.data(), SEABASE_AUTHS, whereClause.data());

  int len = 0;
  long rowCount = 0;
  ExeCliInterface cliInterface(STMTHEAP);
  int cliRC = cliInterface.executeImmediate(buf, (char *)&rowCount, &len, FALSE);

  // If unexpected error occurred, return an exception
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    DDLException excp(cliRC, NULL, 0);
    throw excp;
  }

  return rowCount;
}

// selectMaxAuthID - gets the last used auth ID
int CmpSeabaseDDLauth::selectMaxAuthID(const NAString &whereClause) {
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  char buf[400];
  str_sprintf(buf, "select nvl(max (auth_id),0) from %s.%s %s", MDSchema_.data(), SEABASE_AUTHS, whereClause.data());

  int len = 0;
  long maxValue = 0;
  bool nullTerminate = false;
  ExeCliInterface cliInterface(STMTHEAP);
  int cliRC = cliInterface.executeImmediate(buf, (char *)&maxValue, &len, nullTerminate);
  if (cliRC != 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }

  return static_cast<int>(maxValue);
}

// ----------------------------------------------------------------------------
// method: verifyAuthority
//
// makes sure user has privilege to perform the operation
//
// Input: none
//
// Output:
//   true - authority granted
//   false - no authority or unexpected error
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLauth::verifyAuthority(const SQLOperation operation) {
  // If authorization is not enabled, just return with no error
  if (!CmpCommon::context()->isAuthorizationEnabled()) return true;

  if (ComUser::isRootUserID()) return true;

  int32_t currentUser = ComUser::getCurrentUser();

  NAString systemCatalog = CmpSeabaseDDL::getSystemCatalogStatic();
  std::string privMDLoc(systemCatalog.data());

  privMDLoc += std::string(".\"") + std::string(SEABASE_PRIVMGR_SCHEMA) + std::string("\"");

  PrivMgrComponentPrivileges componentPrivileges(privMDLoc, CmpCommon::diags());

  // See if non-root user has authority to manage users.
  if (componentPrivileges.hasSQLPriv(currentUser, operation, true)) {
    return true;
  }

  return false;
}

// ****************************************************************************
// Class CmpSeabaseDDLuser methods
// ****************************************************************************

// ----------------------------------------------------------------------------
// Constructor
// ----------------------------------------------------------------------------
CmpSeabaseDDLuser::CmpSeabaseDDLuser(const NAString &systemCatalog, const NAString &MDSchema)
    : CmpSeabaseDDLauth(systemCatalog, MDSchema)

{}

CmpSeabaseDDLuser::CmpSeabaseDDLuser()
    : CmpSeabaseDDLauth()

{}

// ----------------------------------------------------------------------------
// public method: getUserDetails
//
// Create the CmpSeabaseDDLuser class containing user details for the
// requested userID
//
// Input:
//    userID - the database authorization ID to search for
//
//  Output:
//    Returned parameter:
//       STATUS_GOOD: authorization details are populated:
//       STATUS_NOTFOUND: authorization details were not found
//       STATUS_WARNING: (not 100) warning was returned, diags area populated
//       STATUS_ERROR: error was returned, diags area populated
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLuser::getUserDetails(int userID) {
  CmpSeabaseDDLauth::AuthStatus retcode = getAuthDetails(userID);
  if (retcode == STATUS_GOOD && !isUser()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_A_USER) << DgString0(getAuthDbName().data());
    retcode = STATUS_ERROR;
  }
  return retcode;
}

// ----------------------------------------------------------------------------
// public method: getUserDetails
//
// Create the CmpSeabaseDDLuser class containing user details for the
// requested username
//
// Input:
//    userName - the database username to retrieve details for
//    isExternal -
//       true - the username is the external name (auth_ext_name)
//       false - the username is the database name (auth_db_name)
//
//  Output:
//    Returned parameter:
//       STATUS_GOOD: authorization details are populated:
//       STATUS_NOTFOUND: authorization details were not found
//       STATUS_WARNING: (not 100) warning was returned, diags area populated
//       STATUS_ERROR: error was returned, diags area populated
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLuser::getUserDetails(const char *pUserName, bool isExternal) {
  CmpSeabaseDDLauth::AuthStatus retcode = getAuthDetails(pUserName, isExternal);
  if (retcode == STATUS_GOOD && !isUser()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_A_USER) << DgString0(getAuthDbName().data());
    retcode = STATUS_ERROR;
  }
  return retcode;
}

// ----------------------------------------------------------------------------
// static method: getUserGroups
//
// Sets up the CmpSeabaseDDLauth class describing the users attributes.
// Calls getUserGroups with just the group list to do the real work.
//
// returns:
//    -1 - unexpected error (ComDiags is setup)
//     0 - operation successful
//   100 - user not registered
// -----------------------------------------------------------------------------
int CmpSeabaseDDLauth::getUserGroups(const char *userName, std::vector<int32_t> &groupIDs) {
  NAString user(userName);
  CmpSeabaseDDLauth authInfo;
  CmpSeabaseDDLauth::AuthStatus retcode = authInfo.getAuthDetails(user, false);
  if (retcode == STATUS_ERROR) return -1;
  if (retcode == STATUS_NOTFOUND) return 100;

  CmpSeabaseDDLauth::AuthStatus authStatus = authInfo.getUserGroups(groupIDs);
  if (authStatus == STATUS_ERROR) return -1;
  return 0;
}

// -----------------------------------------------------------------------------
// method: getUserGroups
//
// Return the list of groups assigned to the user that are registered in the
// metadata.
//
// If authorization enabled, call DBSecurity to get list from AD/LDAP
// Otherwise, get the list from cqd USER_GROUPNAMES
//
// returns:
//     0 - operation successful
//    -1 - unexpected error (ComDiags is setup)
// -----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::getUserGroups(std::vector<int32_t> &groupIDs) {
  std::vector<char *> nameList;
  NAString whereClause("where auth_db_name in ('");
  bool groupsFound = false;
  // if CQD DISABLE_EXTERNAL_AUTH_CHECKS is on,get group list for user from default value
  if (COM_LDAP_AUTH == currentAuthType_ && Get_SqlParser_Flags(DISABLE_EXTERNAL_AUTH_CHECKS)) {
    // user groupnames is a list of space separated names
    std::string groupList = CmpCommon::getDefaultString(USER_GROUPNAMES).data();
    if (groupList.size() == 0) return STATUS_NOTFOUND;

    size_t pos = 0;
    char delimiter(' ');
    bool addComma = false;
    while ((pos = groupList.find(delimiter)) != string::npos) {
      NAString group = groupList.substr(0, pos).c_str();
      group.toUpper();
      // Only include group names that contain valid characters
      if (isAuthNameValid(group)) {
        groupsFound = true;
        if (addComma) whereClause += "', '";
        addComma = true;
        whereClause += group.data();
      }
      groupList.erase(0, pos + 1);
    }
    // add last entry
    NAString lastGroup = groupList.substr(0, groupList.length()).c_str();
    lastGroup.toUpper();
    if (isAuthNameValid(lastGroup)) {
      groupsFound = true;
      if (addComma) whereClause += "', '";
      whereClause += lastGroup.data();
    }
  }

  else {
    switch (currentAuthType_) {
      case COM_LDAP_AUTH: {
        DBUserAuth *userSession = DBUserAuth::GetInstance();
        std::set<std::string> groupList;
        DBUserAuth::CheckUserResult chkRslt =
            userSession->getGroupListForUser(getAuthDbName(), getAuthExtName(), ComUser::getCurrentExternalUsername(),
                                             getAuthID(), getAuthConfig(), groupList);

        switch (chkRslt) {
          case DBUserAuth::GroupListFound:
            break;
          case DBUserAuth::NoGroupsDefined:
            return STATUS_NOTFOUND;
            break;

          // valid configuration section specified
          case DBUserAuth::InvalidConfig:
            *CmpCommon::diags() << DgSqlCode(-CAT_LDAP_DEFAULTCONFIG) << DgString0("unknown");
            return STATUS_ERROR;
            break;

          // Problem looking up the username.  Could be a bad configuration, a
          // problem at the identity store, or communicating with the identity store.
          case DBUserAuth::ErrorDuringCheck:
          default:
            *CmpCommon::diags() << DgSqlCode(-CAT_LDAP_COMM_ERROR);
            return STATUS_ERROR;
            break;
        }
        if (groupList.size() == 0) return STATUS_NOTFOUND;
        // Get the authIDs associated with the entries in groupList. This weeds out any
        // group is that is not registered in metadata.
        for (std::set<std::string>::iterator it = groupList.begin(); it != groupList.end(); ++it) {
          std::string nextValue = *it;
          NAString group = nextValue.c_str();
          group.toUpper();
          // Only include group names that contain valid characters
          if (isAuthNameValid(group)) {
            groupsFound = true;
            if (it != groupList.begin()) whereClause += "', '";
            whereClause += group.data();
          }
        }
      } break;
      case COM_NOT_AUTH:  // is ok
      case COM_NO_AUTH:
      case COM_LOCAL_AUTH:
      // Current version only supports No LDAP and Local authentication methods
      default: {
        // not found is ok
        CmpSeabaseDDLuser userInfo;
        AuthStatus ret = userInfo.getUserDetails(getAuthID());
        if (STATUS_GOOD != ret) {
          return ret;
        }
        // when getAllJoinedUserGroup returns fails,the list is empty.
        if (CmpSeabaseDDLauth::STATUS_ERROR == userInfo.getAllJoinedUserGroup(groupIDs)) {
          return STATUS_ERROR;
        }
        if (groupIDs.empty()) {
          return STATUS_NOTFOUND;
        }
        // return is ok
        return STATUS_GOOD;
      } break;
    }
  }

  // case LDAP and CQD is on will go here
  whereClause += "')";

  if (!groupsFound) return STATUS_NOTFOUND;

  // get groupIDs
  std::vector<int32_t> authIDs;
  AuthStatus authStatus = selectAllWhere(whereClause, authIDs);
  if (authStatus == STATUS_ERROR) return STATUS_ERROR;

  if (authIDs.size() == 0) return STATUS_GOOD;

  // If the name specified in the list is not a group but matches
  // another authID, just ignore.  For example, there could be an AD/LDAP group
  // called "DB__ADMIN" which matches the Esgyn user called DB__ADMIN.
  for (size_t j = 0; j < authIDs.size(); j++) {
    int groupID = authIDs[j];
    if (groupID >= MIN_USERGROUP && groupID <= MAX_USERGROUP) groupIDs.push_back(groupID);
  }

  return STATUS_GOOD;
}

// ----------------------------------------------------------------------------
// Public method: registerUser
//
// registers a user in the Trafodion metadata
//
// Input:  parse tree containing a definition of the user
// Output: the global diags area is set up with the result
// ----------------------------------------------------------------------------
void CmpSeabaseDDLuser::registerUser(StmtDDLRegisterUser *pNode) {
  // See if the user has been granted an ADMIN role
  int adminRole = NA_UserIdDefault;
  bool hasAdminRole = false;
  if (msg_license_multitenancy_enabled()) {
    AuthStatus authStatus = getAdminRole(ComUser::getCurrentUser(), adminRole);
    if (authStatus == STATUS_ERROR) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_AUTH_DETAILS)
                          << DgString0(ComUser::getCurrentUsername());
      return;
    }
    hasAdminRole = (authStatus == STATUS_GOOD);
  }

  CHECK_LICENSE;

  // If the user has been granted an ADMIN role, allow the create even if
  // user failed authorization check
  if (!verifyAuthority(SQLOperation::MANAGE_USERS) && !hasAdminRole) {
    // No authority.  We're outta here.
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return;
  }

  const NAString *userPwd = &(pNode->getAuthPassword());
  if (COM_LDAP_AUTH == currentAuthType_ && NOT pNode->isSetupWithDefaultPassword()) {
    // not supported,just warning
    *CmpCommon::diags() << DgSqlCode(CAT_IGNORE_SETUP_PASSWORD_WHEN_LOCAL_AUTH);
    userPwd = (const NAString *)(new (STMTHEAP) NAString(AUTH_DEFAULT_WORD));
  }
  if (userPwd != NULL && 0 == userPwd->length()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_MUST_INPUT_PASSWORD);
    return;
  }

  int authCreator = hasAdminRole ? adminRole : ComUser::getCurrentUser();

  int retcode = registerUserInternal(pNode->getExternalUserName(), pNode->getDbUserName(), pNode->getConfig().data(),
                                       false, authCreator, *userPwd);
  if (retcode) {
    NAString userName =
        (COM_LDAP_AUTH == CmpCommon::getAuthenticationType()) ? pNode->getExternalUserName() : pNode->getDbUserName();
    *CmpCommon::diags() << DgSqlCode(-CAT_REGISTER_USER_FAIL) << DgString0(userName);
  }
  if (userPwd != &(pNode->getAuthPassword())) {
    NADELETEBASIC(userPwd, STMTHEAP);
  }
}

// ----------------------------------------------------------------------------
// method: registerUserInternal
//
// registers a user in the Trafodion metadata
//
// Input:  Attributes for a user
// Output: the specified diags area is set up with the result
//
// returns:
//    0  - successful
//   <0  - error that occurred during processing
//
// This method does not verify access.  It assumes that privileges have been
// checked prior to calling.
// ----------------------------------------------------------------------------
int CmpSeabaseDDLuser::registerUserInternal(const NAString &extUserName, const NAString &dbUserName,
                                              const char *config, bool autoCreated, int authCreator,
                                              const NAString &authPassword) {
  // If this is an auth register user request, don't allow, return an error
  if (autoCreated && !(COM_LDAP_AUTH == currentAuthType_ && CmpCommon::getDefault(TRAF_AUTO_REGISTER_USER) == DF_ON)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTO_REGISTER_USER) << DgString0(dbUserName.data());
    return -CAT_AUTO_REGISTER_USER;
  }

  // Set up a global try/catch loop to catch unexpected errors
  try {
    // Verify that the specified user name is not reserved
    setAuthDbName(dbUserName);
    if (isAuthNameReserved(dbUserName)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_AUTH_NAME_RESERVED) << DgString0(dbUserName.data());
      return -CAT_AUTH_NAME_RESERVED;
    }

    // Verify that the name does not include unsupported special characters
    if (!isAuthNameValid(getAuthDbName())) {
      *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_CHARS_IN_AUTH_NAME) << DgString0(dbUserName.data());
      return -CAT_INVALID_CHARS_IN_AUTH_NAME;
    }

    // Make sure external user has not already been registered
    setAuthExtName(extUserName);
    // Verify that the external name does not include unsupported special characters
    if (!isAuthNameValid(getAuthExtName())) {
      *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_CHARS_IN_AUTH_NAME) << DgString0(extUserName.data());
      return -CAT_INVALID_CHARS_IN_AUTH_NAME;
    }

    // set up class members from parse node
    setAuthType(COM_USER_CLASS);  // we are a user
    setAuthValid(true);           // assume a valid user

    long createTime = NA_JulianTimestamp();
    setAuthCreateTime(createTime);
    setAuthRedefTime(createTime);  // make redef time the same as create time

    // Make sure db user has not already been registered
    if (authExists(getAuthDbName(), false)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_AUTHID_ALREADY_EXISTS) << DgString0("Authorization identifier")
                          << DgString1(getAuthDbName().data());
      return -CAT_AUTHID_ALREADY_EXISTS;
    }

    if (authExists(getAuthExtName(), true)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_LDAP_USER_ALREADY_EXISTS) << DgString0(getAuthExtName().data());
      return -CAT_LDAP_USER_ALREADY_EXISTS;
    }
    if (COM_LDAP_AUTH == currentAuthType_) {
      Int16 foundConfigurationNumber = DBUserAuth::DefaultConfiguration;

      // Verify that the external user exists in configured identity store
      if (!validateExternalUsername(extUserName.data(), config, foundConfigurationNumber))
        return -CAT_LDAP_AUTH_NOT_FOUND;
      if (foundConfigurationNumber == DBUserAuth::DefaultConfiguration)
        setAuthConfig(DEFAULT_AUTH_CONFIG);
      else
        setAuthConfig((Int16)foundConfigurationNumber);
    } else {
      if (config != NULL && config[0] != 0) {
        *CmpCommon::diags() << DgSqlCode(CAT_IGNORE_SUFFIX_CONFIG_WHEN_LOCAL_AUTH);
      }
      setAuthConfig(DEFAULT_AUTH_CONFIG);
    }
    // if running sentry mode and using Linux groups, make sure DB user is a
    // Linux user. If not return a warning.
    NABoolean usingSentryUserApi = FALSE;
    if (CmpCommon::getDefault(PRIV_SENTRY_USE_USERNAME_API) == DF_ON) usingSentryUserApi = TRUE;
    if (NATable::usingSentry() && NATable::usingLinuxGroups() && !usingSentryUserApi) {
      NAString linuxName = getAuthDbName();
      linuxName.toLower();
      struct passwd *pwName = getpwnam(linuxName.data());
      if (pwName == NULL) {
        *CmpCommon::diags() << DgSqlCode(-CAT_SENTRY_NO_LINUX_UID) << DgString0(getAuthDbName().data())
                            << DgString1(getAuthExtName().data());
        return -CAT_SENTRY_NO_LINUX_UID;
      }
    }

    // Verify that the password does not include unsupported special characters
    // will add a error number about password, password length not grante
    if (!isPasswordValid(authPassword)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_CHARS_IN_AUTH_PASSWORD) << DgString0(authPassword.data());
      return -CAT_INVALID_CHARS_IN_AUTH_PASSWORD;
    }

    int rc = 0;
    int cryptlen = 0;
    char dePassword[2 * (MAX_PASSWORD_LENGTH + EACH_ENCRYPTION_LENGTH) + 1] = {0};
    // base64 maybe make encrypted data longer

    rc = ComEncryption::encrypt_ConvertBase64_Data(
        ComEncryption::AES_128_CBC, (unsigned char *)authPassword.data(), authPassword.length(),
        (unsigned char *)AUTH_PASSWORD_KEY, (unsigned char *)AUTH_PASSWORD_IV, (unsigned char *)dePassword, cryptlen);

    if (rc) {
      *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_CHARS_IN_AUTH_PASSWORD) << DgString0(authPassword.data());
      return -CAT_INVALID_CHARS_IN_AUTH_PASSWORD;
    }

    setAuthPassword(dePassword);

    // Get a unique auth ID number
    int userID = getUniqueAuthID(MIN_USERID, MAX_USERID);
    assert(isUserID(userID));
    setAuthID(userID);

    setAuthCreator(authCreator);
    setAuthInTenant(isRoleID(authCreator));

    // Only roles can have the admin distinction
    setAuthIsAdmin(false);

    setAutoCreated(autoCreated);
    updateSeabaseXDCDDLTable();

    // Add the user to AUTHS table
    insertRow();
#if 0    
    if (pNode->isSchemaSpecified())
    {
       ExeCliInterface cliInterface(STMTHEAP);
       char buf [1000];
       NAString csStmt;
                      COM_SCHEMA_CLASS_PRIVATE = 3,
                      COM_SCHEMA_CLASS_SHARED = 4,
                      COM_SCHEMA_CLASS_DEFAULT = 5};
       
       csStmt = "CREATE ";
       switch (pNode->getSchemaClass())
       {
          case COM_SCHEMA_CLASS_SHARED:
             csStmt += "SHARED ";
             break;
          case COM_SCHEMA_CLASS_DEFAULT:
          case COM_SCHEMA_CLASS_PRIVATE:
             csStmt += "PRIVATE ";
             break;
          default:
          {
             SEABASEDDL_INTERNAL_ERROR("Unknown schema class in registerUser");
             return -1;
          }
       }
           
       csStmt += pNode->getSchemaName()->getCatalogName();
       csStmt +- ".";  
       csStmt += pNode->getSchemaName()->getSchemaName();
       
       str_sprintf(buf, "CREATE %s SCHEMA %s \"%s\".\"%s\".\"%s\" cascade",
                   (char*)catName.data(), (char*)schName.data(), objName);
       
       cliRC = cliInterface.executeImmediate(buf);
    }
#endif
  } catch (...) {
    // At this time, an error should be in the diags area.
    // If there is no error, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("Unexpected error in CmpSeabaseDDLuser::registerUser");
    return (int)CmpCommon::diags()->mainSQLCODE();
  }
  return 0;
}

// ----------------------------------------------------------------------------
// public method:  unregisterUser
//
// This method removes a user from the database
//
// Input:  parse tree containing a definition of the user
// Output: the global diags area is set up with the result
// ----------------------------------------------------------------------------
void CmpSeabaseDDLuser::unregisterUser(StmtDDLRegisterUser *pNode) {
  int adminRole = NA_UserIdDefault;
  bool hasAdminRole = false;
  if (msg_license_multitenancy_enabled()) {
    AuthStatus authStatus = getAdminRole(ComUser::getCurrentUser(), adminRole);
    if (authStatus == STATUS_ERROR) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_AUTH_DETAILS)
                          << DgString0(ComUser::getCurrentUsername());
      return;
    }
    hasAdminRole = (authStatus == STATUS_GOOD);
  }

  CHECK_LICENSE;

  // If the user has been granted an ADMIN role, allow the unregister even if
  // user failed authorization check
  bool hasPriv = verifyAuthority(SQLOperation::MANAGE_USERS);
  if (!hasPriv && !hasAdminRole) {
    // No authority.  We're outta here.
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return;
  }

  // CASCADE option not yet supported
  if (pNode->getDropBehavior() == COM_CASCADE_DROP_BEHAVIOR) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ONLY_SUPPORTING_RESTRICT_DROP_BEHAVIOR);
    return;
  }

  // Verify that the specified user name is not reserved
  if (isAuthNameReserved(pNode->getDbUserName())) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTH_NAME_RESERVED) << DgString0(pNode->getDbUserName().data());
    return;
  }

  // read user details from the AUTHS table
  const NAString dbUserName(pNode->getDbUserName());
  CmpSeabaseDDLauth::AuthStatus retcode = getUserDetails(dbUserName);
  if (retcode == STATUS_ERROR) return;
  if (retcode == STATUS_NOTFOUND) {
    *CmpCommon::diags() << DgSqlCode(-CAT_USER_NOT_EXIST) << DgString0(dbUserName.data());
    return;
  }

  // If performing based on the adminRole, make sure the adminRole
  // matches the authCreator
  if (!hasPriv && adminRole != getAuthCreator()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return;
  }

  try {
    // See if the user has been granted an ADMIN role
    NAString whereClause(" WHERE AUTH_TYPE = 'R' AND AUTH_CREATOR = ");

    char authIDString[20];

    sprintf(authIDString, "%d", getAuthID());
    whereClause += authIDString;

    if (selectCount(whereClause) > 0) {
      *CmpCommon::diags() << DgSqlCode(-CAT_NO_UNREG_USER_OWNS_ROLES);
      return;
    }

    NAString privMgrMDLoc;
    CONCAT_CATSCH(privMgrMDLoc, systemCatalog_.data(), SEABASE_PRIVMGR_SCHEMA);

    // User does not own any roles, but may have been granted roles.
    if (CmpCommon::context()->isAuthorizationEnabled()) {
      PrivMgrRoles role(std::string(MDSchema_.data()), std::string(privMgrMDLoc.data()), CmpCommon::diags());
      std::vector<std::string> roleNames;
      std::vector<int32_t> roleDepths;
      std::vector<int32_t> roleIDs;
      std::vector<int32_t> grantees;
      // don't fetch roles which are inherit from user group.
      if (role.fetchRolesForAuth(getAuthID(), roleNames, roleIDs, roleDepths, grantees, false) ==
          PrivStatus::STATUS_ERROR)
        return;
      if (roleNames.size() > 0) {
        NAString roleList;
        int num = (roleNames.size() <= 5) ? roleNames.size() : 5;
        for (size_t i = 0; i < num; i++) {
          if (i > 0) roleList += ", ";
          roleList += roleNames[i].c_str();
        }
        if (roleNames.size() >= 5) roleList += "...";
        *CmpCommon::diags() << DgSqlCode(-CAT_NO_UNREG_USER_GRANTED_ROLES) << DgString0("user")
                            << DgString1(dbUserName.data()) << DgString2(roleList.data());
        return;
      }
    }

    // Does user own any objects?
    NAString whereClause2(" WHERE OBJECT_OWNER = ");

    NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
    char buf[1000];
    str_sprintf(buf, "SELECT COUNT(*) FROM %s.\"%s\".%s %s %d", sysCat.data(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                whereClause2.data(), getAuthID());

    int len = 0;
    long rowCount = 0;
    ExeCliInterface cliInterface(STMTHEAP);
    int cliRC = cliInterface.executeImmediate(buf, (char *)&rowCount, &len, FALSE);
    if (cliRC != 0) {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      UserException excp(NULL, 0);
      throw excp;
    }

    if (rowCount > 0) {
      *CmpCommon::diags() << DgSqlCode(-CAT_NO_UNREG_USER_OWNS_OBJECT) << DgString0("objects");
      return;
    }

    // Does user own any resources
    if (msg_license_multitenancy_enabled()) {
      NAString resourceNames;
      long rowCount = CmpSeabaseDDL::authIDOwnsResources(getAuthID(), resourceNames);
      if (rowCount < 0) return;
      if (rowCount > 0) {
        *CmpCommon::diags() << DgSqlCode(-CAT_NO_UNREG_USER_OWNS_OBJECT) << DgString0("resources");
        return;
      }
    }

    // User granted any privs?
    PrivMgr privMgr(std::string(privMgrMDLoc), CmpCommon::diags());
    std::vector<PrivClass> privClasses;

    privClasses.push_back(PrivClass::ALL);

    // remove any component privileges granted to this user
    if (CmpCommon::context()->isAuthorizationEnabled()) {
      std::vector<int64_t> objectUIDs;
      bool hasComponentPriv = false;
      if (privMgr.isAuthIDGrantedPrivs(getAuthID(), privClasses, objectUIDs, hasComponentPriv)) {
        // the call to isAuthIDGrantedPrivs may have returned an error reading metadata
        if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) > 0) {
          SEABASEDDL_INTERNAL_ERROR("Unable to retrieve dependent information CmpSeabaseDDLuser::unregisterUser");
          return;
        }

        NAString privGrantee;
        if (hasComponentPriv)
          privGrantee = "one or components";
        else
          privGrantee = getObjectName(objectUIDs);
        if (privGrantee.length() > 0) {
          *CmpCommon::diags() << DgSqlCode(-CAT_NO_UNREG_USER_HAS_PRIVS) << DgString0("User")
                              << DgString1(dbUserName.data()) << DgString2(privGrantee.data());

          return;
        }
      }
      PrivMgrComponentPrivileges componentPrivileges(privMgrMDLoc.data(), CmpCommon::diags());
      std::string componentUIDString = "1";
      if (!componentPrivileges.dropAllForGrantee(getAuthID())) {
        UserException excp(NULL, 0);
        throw excp;
      }
    }

    // delete the row
    deleteRow(getAuthDbName());

    // exit all joined groups
    exitAllJoinedUserGroup();
  } catch (...) {
    // At this time, an error should be in the diags area.
    // If there is no error, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("Switch statement in CmpSeabaseDDLuser::unregisterUser");
  }
}

// ----------------------------------------------------------------------------
// public method:  alterUser
//
// This method changes a user definition
//
// Input:  parse tree containing a definition of the user change
// Output: the global diags area is set up with the result
// ----------------------------------------------------------------------------
void CmpSeabaseDDLuser::alterUser(StmtDDLAlterUser *pNode) {
  try {
    int adminRole = NA_UserIdDefault;
    bool hasAdminRole = false;
    bool isAuthorization =
        CmpCommon::context()->isAuthorizationEnabled() && CmpCommon::context()->isAuthorizationReady();
    if (msg_license_multitenancy_enabled()) {
      AuthStatus authStatus = getAdminRole(ComUser::getCurrentUser(), adminRole);
      if (authStatus == STATUS_ERROR) {
        *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_AUTH_DETAILS)
                            << DgString0(ComUser::getCurrentUsername());
        return;
      }
      hasAdminRole = (authStatus == STATUS_GOOD);
      // TODO: remove this check in the feature
      hasAdminRole &= ComUser::isManagerUser();
    }

    CHECK_LICENSE;

    // If the user has been granted an ADMIN role, allow the alter even if
    // user failed authorization check
    StmtDDLAlterUser::AlterUserCmdSubType cmdSubType = pNode->getAlterUserCmdSubType();

    // change password:   MANAGE_USERS or MANAGE_SECURE
    // unlock user:       MANAGE_USERS or MANAGE_SECURE
    // offline user:      MANAGE_USERS
    // set external name: MANAGE_USERS
    // add/remove group:  MANAGE_USERS + MANAGE_GROUPS

    // pre-check
    bool hasPriv = verifyAuthority(SQLOperation::MANAGE_USERS);
    switch (cmdSubType) {
      case StmtDDLAlterUser::SET_IS_VALID_USER:
      case StmtDDLAlterUser::SET_EXTERNAL_NAME:
        break;
      case StmtDDLAlterUser::SET_USER_UNLOCK:
      case StmtDDLAlterUser::SET_USER_PASSWORD:
        hasPriv |= verifyAuthority(SQLOperation::MANAGE_SECURE);
        break;
      case StmtDDLAlterUser::SET_USER_EXIT_GROUP:
      case StmtDDLAlterUser::SET_USER_JOIN_GROUP:
        hasPriv &= verifyAuthority(SQLOperation::MANAGE_GROUPS);
        break;
      default:
        // error on CAT_UNSUPPORTED_COMMAND_ERROR
        // hasPriv = true;
        break;
    }

    // change password is ok
    if ((!hasPriv && !hasAdminRole) && StmtDDLAlterUser::SET_USER_PASSWORD != cmdSubType) {
      // No authority.  We're outta here.
      *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
      return;
    }

    // read user details from the AUTHS table
    const NAString dbUserName(pNode->getDatabaseUsername());
    CmpSeabaseDDLauth::AuthStatus retcode = getUserDetails(dbUserName);
    if (retcode == STATUS_ERROR) return;
    if (retcode == STATUS_NOTFOUND) {
      *CmpCommon::diags() << DgSqlCode(-CAT_USER_NOT_EXIST) << DgString0(dbUserName.data());
      return;
    }

    // If has admin role, verify that user creator is the same as the admin role
    if ((!hasPriv && adminRole != getAuthCreator() && StmtDDLAlterUser::SET_USER_PASSWORD != cmdSubType) &&
        isAuthorization) {
      // No authority.  We're outta here.
      *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
      return;
    }

    // Process the requested operation
    NAString setClause("set ");
    setAuthDbName(pNode->getDatabaseUsername());
    switch (pNode->getAlterUserCmdSubType()) {
      case StmtDDLAlterUser::SET_EXTERNAL_NAME: {
        long authFlags = 0;
        NAString setList;
        // If authExtName not set to specified name, make sure name is distinct
        if (getAuthExtName() != pNode->getExternalUsername()) {
          if (authExists(pNode->getExternalUsername(), true)) {
            *CmpCommon::diags() << DgSqlCode(-CAT_LDAP_USER_ALREADY_EXISTS)
                                << DgString0(pNode->getExternalUsername().data());
            return;
          }

          setAuthExtName(pNode->getExternalUsername());
          setList += "auth_ext_name = '";
          setList += getAuthExtName();
          setList += "'";

          // check if user exists in LDAP
          if (COM_LDAP_AUTH == currentAuthType_) {
            Int16 foundConfigurationNumber = DBUserAuth::DefaultConfiguration;
            const char *configSectionName = (pNode->getConfig().isNull() ? NULL : pNode->getConfig().data());

            // Verify that the external user exists in configured identity store
            if (!validateExternalUsername((pNode->getAlterUserCmdSubType() == StmtDDLAlterUser::SET_USER_PASSWORD
                                               ? getAuthExtName().data()
                                               : pNode->getExternalUsername().data()),
                                          configSectionName, foundConfigurationNumber))
              return;

            // If configuration changed, authFlags
            if (foundConfigurationNumber != getAuthConfig()) {
              if (foundConfigurationNumber == DBUserAuth::DefaultConfiguration)
                setAuthConfig(DEFAULT_AUTH_CONFIG);
              else
                setAuthConfig((Int16)foundConfigurationNumber);
            }

            //          if (getAuthConfig() != DEFAULT_AUTH_CONFIG)
            //          {
            Int16 config = getAuthConfig();
            config = config << 2;  // shift over bits 1 and 2
            authFlags = config;
            //         }
          }

          // Add remaining flags
          if (isAuthAdmin()) authFlags |= SEABASE_IS_ADMIN_ROLE;
          if (isAuthInTenant()) authFlags |= SEABASE_IS_TENANT_AUTH;
          if (isAutoCreated()) authFlags |= SEABASE_IS_AUTO_CREATED;

          setList += (setList.isNull()) ? " flags = " : ", flags = ";
          setList += (to_string((long long int)authFlags)).c_str();
          setClause += setList;
        }
        if (setList.isNull()) return;
      } break;
      case StmtDDLAlterUser::SET_USER_PASSWORD: {
        if (0 == pNode->getAuthPassword().length()) {
          *CmpCommon::diags() << DgSqlCode(-CAT_MUST_INPUT_PASSWORD);
          return;
        }
        // only allowed to change password for self
        // role MANAGE_USERS can change others password
        if (0 != strcasecmp(ComUser::getCurrentUsername(), getAuthDbName().data())) {
          //#define ADMIN_USER_ID 33332
          //#define ROOT_USER_ID  33333
          if (ComUser::getCurrentUser() > SUPER_USER) {
            if (isAuthorization && (hasPriv || hasAdminRole)) {
              // user has privilege for manage users.
            } else if (!isAuthorization) {
              // lost the status of authorization,just do it
            } else {
              *CmpCommon::diags() << DgSqlCode(-CAT_NOT_ALLOWED_CHANGE_OTHERS_PASSWORD);
              return;
            }
          }
        }

        if (COM_LDAP_AUTH == currentAuthType_) {
          *CmpCommon::diags() << DgSqlCode(-CAT_NOT_ALLOWED_OPERATION_ON_LDAP_AUTH);
          return;
        }

        // Verify that the password does not include unsupported special characters
        // will add a error number about password
        if (!isPasswordValid(pNode->getAuthPassword())) {
          *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_CHARS_IN_AUTH_PASSWORD)
                              << DgString0(pNode->getAuthPassword().data());
          return;
        }

        int rc = 0;
        int cryptlen = 0;
        // base64 maybe make encrypted data longer
        char dePassword[2 * (MAX_PASSWORD_LENGTH + EACH_ENCRYPTION_LENGTH) + 1] = {0};

        rc = ComEncryption::encrypt_ConvertBase64_Data(
            ComEncryption::AES_128_CBC, (unsigned char *)(pNode->getAuthPassword().data()),
            pNode->getAuthPassword().length(), (unsigned char *)AUTH_PASSWORD_KEY, (unsigned char *)AUTH_PASSWORD_IV,
            (unsigned char *)dePassword, cryptlen);
        if (rc) {
          *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_CHARS_IN_AUTH_PASSWORD)
                              << DgString0(pNode->getAuthPassword().data());
          return;
        }

        setAuthPassword(dePassword);
        updateErrPwdCnt(getAuthID(), 0, false);
        updateGracePwdCnt(getAuthID(), 0);
      } break;
      case StmtDDLAlterUser::SET_USER_UNLOCK: {
        // only admin can go here
        switch (currentAuthType_) {
          case COM_LDAP_AUTH: {
            *CmpCommon::diags() << DgSqlCode(-CAT_NOT_ALLOWED_OPERATION_ON_LDAP_AUTH);
            return;
          } break;
          case COM_NOT_AUTH:  // is ok
          case COM_NO_AUTH:
          case COM_LOCAL_AUTH:
          default: {
            // only unlock for expired password or password retry count
            // user password is expired
            {
              long lifetime = (long)CmpCommon::getDefaultNumeric(PASSWORD_LIFE_TIME);
              if (lifetime > 0) {
                // 60 * 60 * 1000 * 1000 usecs
                lifetime = (getPasswdModTime() + lifetime * 24 * 3600000000LL - NA_JulianTimestamp());
                if (lifetime < 1) {
                  setPasswdModTime(NA_JulianTimestamp());
                  reflashPasswordModTime();
                }
              }
            }
            // user password retry count is over limit.
            {
              Int16 count;
              getErrPwdCnt(getAuthID(), count);
              if (count >= CmpCommon::getDefaultNumeric(PASSWORD_ERROR_COUNT)) {
                updateErrPwdCnt(getAuthID(), 0, false);
              }
            }
            // user grace login count is over limit
            {
              Int16 count;
              getGracePwdCnt(getAuthID(), count);
              if (count >= CmpCommon::getDefaultNumeric(TRAF_PASSWORD_GRACE_COUNTER)) {
                updateGracePwdCnt(getAuthID(), 0);
              }
            }
            // don`t call updateRow(setClause);
            return;
          } break;
        }
      } break;
      case StmtDDLAlterUser::SET_IS_VALID_USER: {
        if (isAuthNameReserved(getAuthDbName())) {
          *CmpCommon::diags() << DgSqlCode(-CAT_AUTH_NAME_RESERVED) << DgString0(getAuthDbName().data());
          return;
        }

        setAuthValid(pNode->isValidUser());
        setClause += (isAuthValid()) ? "auth_is_valid = 'Y'" : "auth_is_valid = 'N'";
      } break;
      case StmtDDLAlterUser::SET_USER_EXIT_GROUP:
      case StmtDDLAlterUser::SET_USER_JOIN_GROUP: {
        if (COM_LDAP_AUTH == currentAuthType_) {
          *CmpCommon::diags() << DgSqlCode(-CAT_NOT_ALLOWED_OPERATION_ON_LDAP_AUTH);
          return;
        }
        StmtDDLAlterUser::AlterUserCmdSubType optionType = pNode->getAlterUserCmdSubType();
        std::set<int32_t> joinedGroups;
        {
          std::vector<int32_t> joinedGroupList;
          CmpSeabaseDDLauth::AuthStatus userJoinedStatus = getAllJoinedUserGroup(joinedGroupList);
          if (CmpSeabaseDDLauth::STATUS_GOOD != userJoinedStatus &&
              CmpSeabaseDDLauth::STATUS_NOTFOUND != userJoinedStatus) {
            // CmpSeabaseDDLauth::STATUS_NOTFOUND -> error CAT_USER_IS_NOT_MEMBER_OF_GROUP
            return;
          }
          // vector -> set
          if (!joinedGroupList.empty()) {
            joinedGroups.insert(joinedGroupList.begin(), joinedGroupList.end());
          }
        }
        // don't forget free key of map
        std::map<const char *, int32_t, Char_Compare> authNameAndID;
        // get pair for authID and authName for usergroup
        if (TRUE == getAuthNameAndAuthID(pNode->getUserGroupList(), authNameAndID)) {
          // check all of authid is valid.
          bool isErrorOccurred = false;  // for free key of map
          for (std::map<const char *, int32_t, Char_Compare>::iterator itor = authNameAndID.begin();
               itor != authNameAndID.end();) {
            const char *authName = itor->first;
            int32_t authID = itor->second;
            if (0 == authID) {
              *CmpCommon::diags() << DgSqlCode(-CAT_GROUP_NOT_EXIST) << DgString0(authName);
              isErrorOccurred = true;
              break;
            }
            if (!isUserGroupID(authID)) {
              *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_A_GROUP) << DgString0(authName);
              isErrorOccurred = true;
              break;
            }
            if (joinedGroups.count(authID) == 0) {
              if (StmtDDLAlterUser::SET_USER_EXIT_GROUP == optionType) {
                *CmpCommon::diags() << DgSqlCode(-CAT_USER_IS_NOT_MEMBER_OF_GROUP) << DgString0(getAuthDbName().data())
                                    << DgString1(authName);
                isErrorOccurred = true;
                break;
              }
            } else {
              if (StmtDDLAlterUser::SET_USER_JOIN_GROUP == optionType) {
                // free key
                NADELETEBASICARRAY(authName, STMTHEAP);
                itor = authNameAndID.erase(itor);
                continue;
              }
            }
            itor++;
          }
          int (*func)(int, int) = (StmtDDLAlterUser::SET_USER_JOIN_GROUP == pNode->getAlterUserCmdSubType())
                                            ? &CmpSeabaseDDLauth::addGroupMember
                                            : &CmpSeabaseDDLauth::removeGroupMember;
          bool isAllowedGrantRoleToGroup = false;
          IS_ALLOWED_GRANT_ROLE_TO_GROUP(isAllowedGrantRoleToGroup);
          std::vector<int32_t> groupIDs;
          for (std::map<const char *, int32_t, Char_Compare>::iterator itor = authNameAndID.begin();
               itor != authNameAndID.end(); itor++) {
            if (!isErrorOccurred) {
              func(itor->second, getAuthID());
              if (isAllowedGrantRoleToGroup) {
                groupIDs.push_back(itor->second);
              }
            }
            NADELETEBASICARRAY(itor->first, STMTHEAP);
          }
          authNameAndID.clear();

          // refresh the query cache
          if (!isErrorOccurred && !groupIDs.empty()) {
            // don't use ContextCli::authQuery, which will be polluted the context cache
            std::set<int32_t> groupRoleIDs;  // is a set
            std::vector<int32_t> userRoleIDs;
            std::vector<int32_t> granteeIDs;  // i don't care
            std::vector<int32_t> roleIDs;     // tmp
            std::vector<int32_t> authIDs;
            // get all of role from group
            for (int i = 0; i < groupIDs.size(); i++) {
              roleIDs.clear();
              granteeIDs.clear();
              if (CmpSeabaseDDLauth::STATUS_GOOD == this->getRoleIDs(groupIDs[i], roleIDs, granteeIDs)) {
                // there's duplicate value check on getRoleIDs(), too slow
                groupRoleIDs.insert(roleIDs.begin(), roleIDs.end());
              } else {
                // groupRoleIDs.clear();
                // break;
              }
            }
            // get all of role from user
            if (!groupRoleIDs.empty()) {
              granteeIDs.clear();
              // need roles from group inheritance
              this->getRoleIDs(getAuthID(), userRoleIDs, granteeIDs);
            }

            authIDs.clear();
            authIDs.push_back(getAuthID());

            roleIDs.clear();

            if (!groupRoleIDs.empty() && !userRoleIDs.empty()) {
              // set -> vector
              std::vector<int32_t> gRoleIDs_(groupRoleIDs.begin(), groupRoleIDs.end());
              CmpCommon::getDifferenceWithTwoVector(gRoleIDs_, userRoleIDs, roleIDs);
            } else if (!groupRoleIDs.empty() && userRoleIDs.empty()) {
              roleIDs.assign(groupRoleIDs.begin(), groupRoleIDs.end());
            }

            if (!roleIDs.empty()) {
              CmpCommon::letsQuerycacheToInvalidated(authIDs, roleIDs);
            }
          }
          // don`t call updateRow(setClause);
          return;
        }
      } break;
      default: {
        *CmpCommon::diags() << DgSqlCode(-CAT_UNSUPPORTED_COMMAND_ERROR);
        return;
      }
    }
    updateRow(setClause);
  } catch (...) {
    // At this time, an error should be in the diags area.
    // If there is no error, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("Switch statement in CmpSeabaseDDLuser::alterUser");
  }
}

// ----------------------------------------------------------------------------
// method: registerStandardUser
//
// Creates a standard user ie. (DB__ROOT) in the Trafodion metadata
//
// Input:
//    authName
//    authID
// ----------------------------------------------------------------------------
void CmpSeabaseDDLuser::registerStandardUser(const std::string authName, const int32_t authID) {
  setAuthType(COM_USER_CLASS);  // we are a user
  createStandardAuth(authName, authID);
}

// -----------------------------------------------------------------------------
// Function: validateExternalGroupname
//
//  Determines if an external group mame is valid.
//
//  Parameters:
//
//   <externalGroupName> is the group name to be validated.
//
//   <configurationName> specifies which configuration to use to validate the
//                       group name; a configuration designates one or more
//                       identity stores and their parameters.  A value of
//                       "default" indicates the "default" configuration should
//                       be used.
//
//   <foundConfigurationNumber> passes back the configuration used to validate
//                              the username.
// -----------------------------------------------------------------------------
inline static bool validateExternalGroupname(const char *externalGroupName, const char *configurationName,
                                             Int16 &foundConfigurationNumber)

{
  // return true;
  // During initialization external checking needs to be disabled to setup
  // standard database users.
  if (Get_SqlParser_Flags(DISABLE_EXTERNAL_AUTH_CHECKS)) {
    foundConfigurationNumber = DBUserAuth::DefaultConfiguration;
    return true;
  }

  // Verify that the external username is defined in the identity store.
  DBUserAuth::CheckUserResult chkUserRslt = DBUserAuth::UserExists;

  Int16 configurationNumber = DBUserAuth::DefaultConfiguration;
  chkUserRslt =
      DBUserAuth::CheckExternalGroupnameDefined(externalGroupName, configurationName, foundConfigurationNumber);
  // Group name was found!
  if (chkUserRslt == DBUserAuth::UserExists) return true;

  switch (chkUserRslt) {
    case DBUserAuth::UserDoesNotExist:
      *CmpCommon::diags() << DgSqlCode(-CAT_LDAP_AUTH_NOT_FOUND) << DgString0("User group")
                          << DgString1(externalGroupName);
      break;

    // valid configuration section specified
    case DBUserAuth::InvalidConfig:
      *CmpCommon::diags() << DgSqlCode(-CAT_LDAP_DEFAULTCONFIG) << DgString0(configurationName);
      break;

    // Problem looking up the username.  Could be a bad configuration, a
    // problem at the identity store, or communicating with the identity store.
    case DBUserAuth::ErrorDuringCheck:
    default:
      *CmpCommon::diags() << DgSqlCode(-CAT_LDAP_COMM_ERROR);
      break;
  }

  return false;
}
//---------------------- End of validateExternalGroupname -----------------------

// -----------------------------------------------------------------------------
// Function: validateExternalUsername
//
//  Determines if an external username is valid.
//
//  Parameters:
//
//    <externalUsername>   is the username to be validated.
//
//    <configurationName>   specifies which configuration to use to validate the
//                          username; a configuration designates one or more
//                          identity stores and their parameters.  A value of
//                          "default" indicates the "default" configuration should
//                          be used.
//
//    <foundConfigurationNumber> passes back the configuration used to validate
//                               the username.
// -----------------------------------------------------------------------------
inline static bool validateExternalUsername(const char *externalUsername, const char *configurationName,
                                            Int16 &foundConfigurationNumber)

{
  // During initialization external checking needs to be disabled to setup
  // standard database users.
  if (Get_SqlParser_Flags(DISABLE_EXTERNAL_AUTH_CHECKS)) {
    foundConfigurationNumber = DBUserAuth::DefaultConfiguration;
    return true;
  }

  // Verify that the external username is defined in the identity store.
  DBUserAuth::CheckUserResult chkUserRslt = DBUserAuth::UserDoesNotExist;

  Int16 configurationNumber = DBUserAuth::DefaultConfiguration;
  chkUserRslt = DBUserAuth::CheckExternalUsernameDefined(externalUsername, configurationName,
                                                         // configurationNumber,
                                                         foundConfigurationNumber);

  // Username was found!
  if (chkUserRslt == DBUserAuth::UserExists) return true;

  switch (chkUserRslt) {
    case DBUserAuth::UserDoesNotExist:
      *CmpCommon::diags() << DgSqlCode(-CAT_LDAP_AUTH_NOT_FOUND) << DgString0("User") << DgString1(externalUsername);
      break;

    // valid configuration section specified
    case DBUserAuth::InvalidConfig:
      *CmpCommon::diags() << DgSqlCode(-CAT_LDAP_DEFAULTCONFIG) << DgString0(configurationName);
      break;

    // Problem looking up the username.  Could be a bad configuration, a
    // problem at the identity store, or communicating with the identity store.
    case DBUserAuth::ErrorDuringCheck:
    default:
      *CmpCommon::diags() << DgSqlCode(-CAT_LDAP_COMM_ERROR);
      break;
  }

  return false;
}
//---------------------- End of validateExternalUsername -----------------------

// -----------------------------------------------------------------------------
// public method:  describe
//
// This method returns the showddl text for the requested user in string format
//
// Input:
//   authName - name of user to describe
//
// Input/Output:
//   authText - the REGISTER USER text
//
// returns result:
//    true -  successful
//    false - failed (ComDiags area will be set up with appropriate error)
//-----------------------------------------------------------------------------
bool CmpSeabaseDDLuser::describe(const NAString &authName, NAString &authText) {
  bool hasAdminRole = false;
  if (authName != ComUser::getCurrentUsername()) {
    if (msg_license_multitenancy_enabled()) {
      int adminRole = NA_UserIdDefault;
      AuthStatus authStatus = getAdminRole(ComUser::getCurrentUser(), adminRole);
      if (authStatus == STATUS_ERROR) return false;
      hasAdminRole = (authStatus == STATUS_GOOD);
    }

    if (!verifyAuthority(SQLOperation::SHOW) && !hasAdminRole) {
      // No authority.  We're outta here.
      *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
      return false;
    }

    // If sentry is enabled, display groups associated with the user
    NABoolean usingSentryUserApi = FALSE;
    if (CmpCommon::getDefault(PRIV_SENTRY_USE_USERNAME_API) == DF_ON) usingSentryUserApi == TRUE;

    if (NATable::usingSentry() && !usingSentryUserApi) {
      std::set<std::string> myGroups;
      if (NATable::usingLinuxGroups()) getLinuxGroups(getAuthDbName().data(), myGroups);
      if (myGroups.size() > 0) {
        authText += "-- groups: ";
        for (std::set<std::string>::iterator it = myGroups.begin(); it != myGroups.end(); ++it) {
          std::string group = *it;
          authText += group.c_str();
          authText += " ";
        }
        if (myGroups.size() > 0) authText += '\n';
      }
    }
  }

  CmpSeabaseDDLauth::AuthStatus retcode = getUserDetails(authName.data());
  // If the user was not found, set up an error
  if (retcode == STATUS_NOTFOUND) {
    *CmpCommon::diags() << DgSqlCode(-CAT_USER_NOT_EXIST) << DgString0(authName.data());
    return false;
  }

  // If an error was detected, return
  if (retcode == STATUS_ERROR) return false;

  if (!isUser()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(authName.data()) << DgString1("user");
    UserException excp(NULL, 0);
    return false;
  }

  // Generate output text
  authText = (COM_LDAP_AUTH == currentAuthType_) ? "REGISTER USER \"" : "CREATE USER \"";
  authText += getAuthExtName();
  authText += '"';
  if (getAuthExtName() != getAuthDbName()) {
    authText += " AS \"";
    authText += getAuthDbName();
    authText += '"';
  }
  authText += ";\n";

  if (!isAuthValid()) {
    authText += "ALTER USER \"";
    authText += getAuthDbName();
    authText += "\" SET OFFLINE;\n";
  }
  return true;
}
//------------------------------ End of describe -------------------------------

// ****************************************************************************
// Class CmpSeabaseDDLrole methods
// ****************************************************************************

// ----------------------------------------------------------------------------
// Constructors
// ----------------------------------------------------------------------------
CmpSeabaseDDLrole::CmpSeabaseDDLrole(const NAString &systemCatalog, const NAString &MDSchema)
    : CmpSeabaseDDLauth(systemCatalog, MDSchema) {}

CmpSeabaseDDLrole::CmpSeabaseDDLrole(const NAString &systemCatalog) : CmpSeabaseDDLauth() {
  systemCatalog_ = systemCatalog;

  CONCAT_CATSCH(MDSchema_, systemCatalog.data(), SEABASE_MD_SCHEMA);
}

CmpSeabaseDDLrole::CmpSeabaseDDLrole() : CmpSeabaseDDLauth() {}

// ----------------------------------------------------------------------------
// Public method: createRole
//
// Creates a role in the Trafodion metadata
//
// Input:  parse tree containing a definition of the role
// Output: the global diags area is set up with the result
// ----------------------------------------------------------------------------
void CmpSeabaseDDLrole::createRole(StmtDDLCreateRole *pNode) {
  // Don't allow roles to be created unless authorization is enabled
  if (!CmpCommon::context()->isAuthorizationEnabled()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTHORIZATION_NOT_ENABLED);
    return;
  }

  // See if the user has been granted an ADMIN role
  int adminRole = NA_UserIdDefault;
  bool hasAdminRole = false;
  if (msg_license_multitenancy_enabled()) {
    AuthStatus authStatus = getAdminRole(ComUser::getCurrentUser(), adminRole);
    if (authStatus == STATUS_ERROR) {
      *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_AUTH_DETAILS)
                          << DgString0(ComUser::getCurrentUsername());
      return;
    }

    hasAdminRole = (authStatus == STATUS_GOOD);
  }

  // If the user does not have privilege, allow the create if
  //   the user has been granted an admin role and
  //   the role being created is not an admin role
  if (!verifyAuthority(SQLOperation::MANAGE_ROLES)) {
    if (!hasAdminRole || pNode->isAdminRole()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
      return;
    }
  }

  // Set up a global try/catch loop to catch unexpected errors
  try {
    // Verify that the specified role name is not reserved
    setAuthDbName(pNode->getRoleName());
    if (isAuthNameReserved(getAuthDbName())) {
      *CmpCommon::diags() << DgSqlCode(-CAT_AUTH_NAME_RESERVED) << DgString0(getAuthDbName().data());
      return;
    }

    // Verify that the name does not include unsupported special characters
    if (!isAuthNameValid(getAuthDbName())) {
      *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_CHARS_IN_AUTH_NAME) << DgString0(getAuthDbName().data());
      return;
    }

    // Make sure role has not already been registered
    if (authExists(getAuthDbName(), false)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_AUTHID_ALREADY_EXISTS) << DgString0("Authorization identifier")
                          << DgString1(getAuthDbName().data());
      return;
    }

    // unexpected error occurred - ?
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) > 0) return;

    // if tenant specified, verify tenant details
    int32_t tenantID = NA_UserIdDefault;
    const NAString tenantName = pNode->getTenantName();
    TenantInfo tenantInfo(STMTHEAP);
    if (!tenantName.isNull()) {
      // See if tenant exists
      CmpSeabaseDDLtenant tenant;
      AuthStatus authStatus = tenant.getTenantDetails(tenantName, tenantInfo);
      if (authStatus == STATUS_ERROR) {
        *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_AUTH_DETAILS) << DgString0(tenantName.data());
        return;
      }
      if (authStatus == AuthStatus::STATUS_NOTFOUND) {
        *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(getAuthDbName().data())
                            << DgString1("tenant");
        return;
      }

      // See if tenant has already been granted an admin role
      if (tenantInfo.getAdminRoleID() > NA_UserIdDefault) {
        char adminRoleName[MAX_DBUSERNAME_LEN + 1];
        int32_t length;
        ComUser::getAuthNameFromAuthID(tenantInfo.getAdminRoleID(), adminRoleName, MAX_DBUSERNAME_LEN, length, FALSE,
                                       CmpCommon::diags());
        *CmpCommon::diags() << DgSqlCode(-CAT_AUTH_HAS_ADMIN_ROLE) << DgString0(tenantName.data())
                            << DgString1(adminRoleName);
        return;
      }
      tenantID = tenant.getAuthID();
    }

    // set up class members from parse node
    setAuthType(COM_ROLE_CLASS);  // we are a role
    setAuthValid(true);           // assume a valid role

    long createTime = NA_JulianTimestamp();
    setAuthCreateTime(createTime);
    setAuthRedefTime(createTime);  // make redef time the same as create time

    // Get a unique role ID number
    int roleID = getUniqueAuthID(MIN_ROLEID, MAX_ROLEID);  // TODO: add role support
    assert(isRoleID(roleID));
    setAuthID(roleID);

    // If the WITH ADMIN clause was specified, then create the role on behalf of the
    // authorization ID specified in this clause.
    // Need to translate the creator name to its authID
    std::string granteeName;
    int granteeID;
    if (pNode->getOwner() == NULL) {
      // get effective user from the Context
      setAuthCreator(ComUser::getCurrentUser());
      granteeName = ComUser::getCurrentUsername();
    } else {
      const NAString creatorName = pNode->getOwner()->getAuthorizationIdentifier();
      int authID = NA_UserIdDefault;
      Int16 result = ComUser::getUserIDFromUserName(creatorName.data(), authID, CmpCommon::diags());
      if (result != 0) {
        *CmpCommon::diags() << DgSqlCode(-CAT_USER_NOT_EXIST) << DgString0(creatorName.data());
        return;
      }

      // TODO: verify creator can create roles
      setAuthCreator(authID);
      granteeName = creatorName.data();
    }
    granteeID = getAuthCreator();

    // If the user has been granted an ADMIN role, make the ADMIN role
    // the creator
    if (hasAdminRole && !pNode->isAdminRole()) {
      if (!isRoleID(adminRole)) SEABASEDDL_INTERNAL_ERROR("Unable to create role, invalid role ID detected");
      setAuthCreator(adminRole);
    }

    // For roles, external and database names are the same.
    setAuthExtName(getAuthDbName());

    setAuthIsAdmin(pNode->isAdminRole());
    if (hasAdminRole && !pNode->isAdminRole())
      setAuthInTenant(true);
    else
      setAuthInTenant(false);
    setAutoCreated(false);

    // Add the role to AUTHS table
    insertRow();

    // Grant this role to the creator of the role if authorization is enabled.
    NAString privMgrMDLoc;

    CONCAT_CATSCH(privMgrMDLoc, systemCatalog_.data(), SEABASE_PRIVMGR_SCHEMA);

    PrivMgrRoles roles(std::string(MDSchema_.data()), std::string(privMgrMDLoc), CmpCommon::diags());

    PrivStatus privStatus =
        roles.grantRoleToCreator(roleID, getAuthDbName().data(), SYSTEM_USER, SYSTEM_AUTH_NAME, granteeID, granteeName);
    if (privStatus != PrivStatus::STATUS_GOOD) {
      SEABASEDDL_INTERNAL_ERROR("Unable to grant role to role administrator");
      return;
    }

    // if tenant specified, set tenant information
    if (!tenantName.isNull()) {
      // grant admin role to tenant
      tenantInfo.setAdminRoleID(roleID);
      tenantInfo.updateTenantInfo(true, NULL);
    }
  }

  catch (...) {
    // At this time, an error should be in the diags area.
    // If there is no error, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("Switch statement in CmpSeabaseDDLrole::createRole");
  }
}

// ----------------------------------------------------------------------------
// method: createAdminRole
//
// creates an admin role for a tenant.
// ----------------------------------------------------------------------------
int CmpSeabaseDDLrole::createAdminRole(const NAString &roleName, const NAString &tenantName, const int roleOwner,
                                         const char *roleOwnerName) {
  bool enabled = msg_license_multitenancy_enabled();
  if (!enabled) {
    *CmpCommon::diags() << DgSqlCode(-4222) << DgString0("multi-tenant");
    UserException excp(NULL, 0);
    throw excp;
  }

  // Verify that the specified role name is not reserved
  bool isSpecialAuth = false;
  if (isSystemAuth(COM_ROLE_CLASS, roleName, isSpecialAuth)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTH_NAME_RESERVED) << DgString0(roleName.data());
    UserException excp(NULL, 0);
    throw excp;
  }

  // Verify that the name does not include unsupported special characters
  if (!isAuthNameValid(roleName)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_CHARS_IN_AUTH_NAME) << DgString0(roleName.data());
    UserException excp(NULL, 0);
    throw excp;
  }

  // Make sure role has not already been created
  if (authExists(roleName, false)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTHID_ALREADY_EXISTS) << DgString0("Authorization identifier")
                        << DgString1(roleName.data());
    UserException excp(NULL, 0);
    throw excp;
  }

  // set up class members
  setAuthDbName(roleName);
  setAuthExtName(roleName);
  setAuthType(COM_ROLE_CLASS);  // we are a role
  setAuthValid(true);           // assume a valid role
  setAuthCreator(roleOwner);
  setAuthIsAdmin(true);

  long createTime = NA_JulianTimestamp();
  setAuthCreateTime(createTime);
  setAuthRedefTime(createTime);  // make redef time the same as create time

  // Get a unique role ID number
  int roleID = getUniqueAuthID(MIN_ROLEID, MAX_ROLEID);  // TODO: add role support
  setAuthID(roleID);

  // Add the role to AUTHS table
  insertRow();

  // Grant this role to the creator of the role
  NAString privMgrMDLoc;
  CONCAT_CATSCH(privMgrMDLoc, systemCatalog_.data(), SEABASE_PRIVMGR_SCHEMA);
  PrivMgrRoles roles(std::string(MDSchema_.data()), std::string(privMgrMDLoc), CmpCommon::diags());

  PrivStatus privStatus = roles.grantRoleToCreator(roleID, std::string(roleName), SYSTEM_USER, SYSTEM_AUTH_NAME,
                                                   roleOwner, std::string(roleOwnerName));
  if (privStatus != PrivStatus::STATUS_GOOD) {
    SEABASEDDL_INTERNAL_ERROR("Unable to grant role to role administrator");
    UserException excp(NULL, 0);
    throw excp;
  }

  return roleID;
}

// ----------------------------------------------------------------------------
// Public method: createStandardRole
//
// Creates a standard role (ie. DB__nameROLE) in the Trafodion metadata
//
// Input:
//    authName
//    authID
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLrole::createStandardRole(const std::string roleName, const int32_t roleID) {
  setAuthType(COM_ROLE_CLASS);  // we are a role
  return createStandardAuth(roleName, roleID);
}

// -----------------------------------------------------------------------------
// public method:  describe
//
// This method returns the showddl text for the requested role in string format
//
// Input:
//   roleName - name of role to describe
//
// Input/Output:
//   roleText - the CREATE ROLE (and GRANT ROLE) text
//
// returns result:
//    true -  successful
//    false - failed (ComDiags area will be set up with appropriate error)
//-----------------------------------------------------------------------------
bool CmpSeabaseDDLrole::describe(const NAString &roleName, NAString &roleText) {
  CmpSeabaseDDLauth::AuthStatus retcode = getRoleDetails(roleName.data());

  // Error was detected, ComDiags should be set up with problem
  if (retcode == STATUS_ERROR) return false;

  // If the role was not found, set up an error
  if (retcode == STATUS_NOTFOUND) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_NOT_EXIST) << DgString0(roleName.data());
    return false;
  }

  // Can current user perform request
  //   User has been granted role
  //   User has authority
  //   User has been granted admin role (tenants only)
  bool resetRoleList = false;
  bool userIsAuthorized = ComUser::currentUserHasRole(getAuthID(), NULL /*don't update diags*/, resetRoleList);
  if (!userIsAuthorized) userIsAuthorized = verifyAuthority(SQLOperation::SHOW);

  if (!userIsAuthorized) {
    if (msg_license_multitenancy_enabled()) {
      int adminRole = NA_UserIdDefault;
      AuthStatus authStatus = getAdminRole(ComUser::getCurrentUser(), adminRole);
      if (authStatus == STATUS_ERROR) return false;
      if (authStatus == STATUS_GOOD) userIsAuthorized = true;
    }
  }

  if (!userIsAuthorized) {
    // No authority.  We're outta here.
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return false;
  }

  if (!isRole()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(roleName.data()) << DgString1("role");
    return false;
  }

  // Generate output text
  roleText = "CREATE ";
  roleText += "ROLE \"";
  roleText += getAuthDbName();
  roleText += "\"";

  // If the role owner is not DB__ROOT, list the user who administers the role.
  if (getAuthCreator() != ComUser::getRootUserID()) {
    roleText += " WITH ADMIN \"";

    char creatorName[MAX_DBUSERNAME_LEN + 1];
    int32_t length = 0;

    // just display what getAuthNameFromAuthID returns
    ComUser::getAuthNameFromAuthID(getAuthCreator(), creatorName, sizeof(creatorName), length, TRUE,
                                   NULL /*don't update diags*/);
    roleText += creatorName;
    roleText += "\"";
  }

  // See if authorization is enabled.  If so, need to list any grants of this
  // role.  Otherwise, we are outta here.
  if (!CmpCommon::context()->isAuthorizationEnabled()) {
    roleText += ";\n";
    return true;
  }

  NAString privMgrMDLoc;
  CONCAT_CATSCH(privMgrMDLoc, systemCatalog_.data(), SEABASE_PRIVMGR_SCHEMA);
  PrivMgrRoles roles(std::string(MDSchema_.data()), std::string(privMgrMDLoc), CmpCommon::diags());

  // Get name of tenant
  std::string tenantName;
  bool reportTenant = false;
  bool isDetached = false;
  if (isAuthAdmin()) {
    TenantInfo tenantForRole(STMTHEAP);
    if (TenantInfo::getTenantInfo(getAuthID(), tenantForRole) == 0) {
      char tempName[MAX_DBUSERNAME_LEN + 1];
      int32_t length = 0;

      // just display what getAuthNameFromAuthID returns
      ComUser::getAuthNameFromAuthID(tenantForRole.getTenantID(), tempName, sizeof(tempName), length, TRUE,
                                     NULL /*don't update diags*/);

      tenantName = tempName;
      reportTenant = true;
    }
  }
  roleText += ";\n";

  std::vector<std::string> granteeNames;
  std::vector<int32_t> granteeIDs;
  std::vector<int32_t> grantDepths;
  std::vector<int32_t> grantorIDs;

  PrivStatus privStatus =
      roles.fetchGranteesForRole(getAuthID(), granteeNames, granteeIDs, grantorIDs, grantDepths, true);

  // If nobody was granted this role, nothing to do.
  if (privStatus == PrivStatus::STATUS_NOTFOUND || granteeNames.size() == 0) return true;

  if (privStatus == PrivStatus::STATUS_ERROR) SEABASEDDL_INTERNAL_ERROR("Could not fetch users/groups granted role.");

  // If CQD to display privilege grants is off, return now
  if ((CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_OFF) ||
      ((CmpCommon::getDefault(SHOWDDL_DISPLAY_PRIVILEGE_GRANTS) == DF_SYSTEM) && getenv("SQLMX_REGRESS")))
    return true;

  // Report on each grantee.
  for (size_t r = 0; r < granteeNames.size(); r++) {
    // If the grantor is system, we want to show the grant, but exclude
    // it from executing in a playback script.
    if (grantorIDs[r] == ComUser::getSystemUserID()) {
      if (isTenantID(granteeIDs[r])) {
        tenantName = granteeNames[r];
        reportTenant = true;
        isDetached = true;
        continue;
      }
      roleText += "-- ";
    }
    roleText += "GRANT ROLE \"";
    roleText += getAuthDbName();
    roleText += "\" TO \"";
    roleText += granteeNames[r].c_str();
    roleText += "\"";
    // Grant depth is either zero or non-zero for now.  If non-zero,
    // report WITH ADMIN OPTION.
    if (grantDepths[r] != 0) roleText += " WITH ADMIN OPTION";

    // If the grantor is not DB__ROOT or _SYSTEM, list the grantor.
    if (grantorIDs[r] != ComUser::getRootUserID() && grantorIDs[r] != ComUser::getSystemUserID()) {
      roleText += " GRANTED BY \"";
      char grantorName[MAX_DBUSERNAME_LEN + 1];
      int32_t length = 0;

      // Ignore errors, just print what getAuthNameFromAuthID returns
      ComUser::getAuthNameFromAuthID(grantorIDs[r], grantorName, sizeof(grantorName), length, TRUE,
                                     NULL /*don't update diags*/);
      roleText += grantorName;
      roleText += "\"";
    }
    roleText += ";\n";
  }
  if (reportTenant) {
    if (isDetached)
      roleText += "-- Detached admin role for tenant ";
    else
      roleText += "-- Admin role for tenant ";
    roleText += tenantName.c_str();
    roleText += '\n';
  }
  return true;
}
//------------------------------ End of describe -------------------------------

// ----------------------------------------------------------------------------
// Method: dropAdminRole
//
// removes admin role and its connection to its tenant.
// ----------------------------------------------------------------------------
int CmpSeabaseDDLrole::dropAdminRole(const NAString &roleName, const int tenantID) {
  // read role details from the AUTHS table
  CmpSeabaseDDLauth::AuthStatus retcode = getRoleDetails(roleName);
  if (retcode == STATUS_ERROR) return -1;

  if (retcode == STATUS_NOTFOUND) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_NOT_EXIST) << DgString0(roleName.data());
    return -1;
  }

  NAString privMgrMDLoc;
  CONCAT_CATSCH(privMgrMDLoc, systemCatalog_.data(), SEABASE_PRIVMGR_SCHEMA);
  PrivMgrRoles role(std::string(MDSchema_.data()), std::string(privMgrMDLoc), CmpCommon::diags());

  // First, see if role has been granted
  if (role.revokeAllForGrantor(getAuthCreator(), getAuthID(), false, 0, PrivDropBehavior::RESTRICT) ==
      PrivStatus::STATUS_ERROR) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_IS_GRANTED_NO_DROP);
    return -1;
  }

  // remove any component privileges granted to this user
  PrivMgrComponentPrivileges componentPrivileges(privMgrMDLoc.data(), CmpCommon::diags());
  if (!componentPrivileges.dropAllForGrantee(getAuthID())) return -1;

  // Now see if the role has been granted any privileges.
  PrivMgr privMgr(std::string(privMgrMDLoc), CmpCommon::diags());
  std::vector<PrivClass> privClasses;

  privClasses.push_back(PrivClass::ALL);
  std::vector<int64_t> objectUIDs;
  bool hasComponentPriv = false;

  if (privMgr.isAuthIDGrantedPrivs(getAuthID(), privClasses, objectUIDs, hasComponentPriv)) {
    NAString privGrantee;
    if (hasComponentPriv)
      privGrantee = "one or components";
    else
      // If the objectName is empty, then it is no longer defined,
      // continue.
      privGrantee = getObjectName(objectUIDs);

    if (privGrantee.length() > 0) {
      *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_HAS_PRIVS_NO_DROP) << DgString0(roleName.data())
                          << DgString1(privGrantee.data());
      return -1;
    }
  }

  // Role has not been granted and no privileges have been granted to
  // the role.  Remove the system grants.  Every role has a grant from
  // system (_SYSTEM) to the role creator.  It may have a grant from
  // system (_SYSTEM) to a detached role.  Remove both.
  if (role.revokeRoleFromCreator(getAuthID(), getAuthCreator(), true) != PrivStatus::STATUS_GOOD) {
    SEABASEDDL_INTERNAL_ERROR("Unable to revoke role from role creator");
    return -1;
  }
  try {
    // delete the row
    deleteRow(roleName);
  } catch (...) {
    // At this time, an error should be in the diags area.
    // If there is no error, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLrole::dropAdminRole");
  }
  return 0;
}

// ----------------------------------------------------------------------------
// Public method: dropRole
//
// Drops a role from the Trafodion metadata
//
// Input:  parse tree containing a definition of the role
// Output: the global diags area is set up with the result
// ----------------------------------------------------------------------------
void CmpSeabaseDDLrole::dropRole(StmtDDLCreateRole *pNode)

{
  // Set up a global try/catch loop to catch unexpected errors
  try {
    const NAString roleName(pNode->getRoleName());
    // Verify that the specified user name is not reserved
    setAuthDbName(roleName);
    if (isAuthNameReserved(getAuthDbName())) {
      *CmpCommon::diags() << DgSqlCode(-CAT_AUTH_NAME_RESERVED) << DgString0(roleName.data());
      return;
    }

    // read role details from the AUTHS table
    CmpSeabaseDDLauth::AuthStatus retcode = getRoleDetails(roleName);
    if (retcode == STATUS_ERROR) return;

    if (retcode == STATUS_NOTFOUND) {
      *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_NOT_EXIST) << DgString0(roleName.data());
      return;
    }

    // If this is an admin role, return an error.  Admin roles are dropped
    // when the tenant is unregistered
    if (isAuthAdmin()) {
      TenantInfo roleForTenant(STMTHEAP);
      int cliRC = TenantInfo::getTenantInfo(getAuthID(), roleForTenant);

      // cliRC of 1 means tenant no longer exists so go ahead and drop
      if (cliRC != 1) {
        char tenantName[MAX_DBUSERNAME_LEN + 1];
        int32_t length;

        // just display what getAuthNameFromAuthID returns
        ComUser::getAuthNameFromAuthID(roleForTenant.getTenantID(), tenantName, MAX_DBUSERNAME_LEN, length, TRUE,
                                       NULL /* don't update diags*/);

        *CmpCommon::diags() << DgSqlCode(-CAT_CANNOT_DROP_ADMIN_ROLE) << DgString0(roleName.data())
                            << DgString1(tenantName);
        return;
      }
    }

    // See if the user has been granted an ADMIN role
    int adminRole = NA_UserIdDefault;
    bool hasAdminRole = false;
    if (msg_license_multitenancy_enabled()) {
      AuthStatus authStatus = getAdminRole(ComUser::getCurrentUser(), adminRole);
      if (authStatus == STATUS_ERROR) {
        *CmpCommon::diags() << DgSqlCode(-CAT_UNABLE_TO_RETRIEVE_AUTH_DETAILS)
                            << DgString0(ComUser::getCurrentUsername());
        return;
      }

      hasAdminRole = (authStatus == STATUS_GOOD);
    }

    // check privileges
    if (ComUser::getCurrentUser() != getAuthCreator()) {
      // If the user does not have privilege, allow the drop if
      //   the user has been granted an admin role and
      //   the role being dropped is not an admin role and
      //   the authCreator of the role being dropped matches the admin role
      if (verifyAuthority(SQLOperation::MANAGE_ROLES) == false) {
        if (!hasAdminRole || pNode->isAdminRole() || adminRole != getAuthCreator()) {
          *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
          return;
        }
      }
    }

    NAString privMgrMDLoc;

    CONCAT_CATSCH(privMgrMDLoc, systemCatalog_.data(), SEABASE_PRIVMGR_SCHEMA);

    PrivMgrRoles role(std::string(MDSchema_.data()), std::string(privMgrMDLoc), CmpCommon::diags());

    // If authorization is not enabled and a role has been defined, skip
    // looking for dependencies and just remove the role from auths.
    if (CmpCommon::context()->isAuthorizationEnabled()) {
      // TODO: Could support a CASCADE option that would clean up
      // grants and dependent objects.

      // First, see if role has been granted
      std::string existingGrantees;
      bool roleIsGranted = role.isGranted(getAuthID(), true, existingGrantees);
      if (roleIsGranted) {
        *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_IS_GRANTED_NO_DROP) << DgString0(existingGrantees.c_str());
        return;
      }

      // remove any component privileges granted to this role
      PrivMgrComponentPrivileges componentPrivileges(privMgrMDLoc.data(), CmpCommon::diags());
      if (!componentPrivileges.dropAllForGrantee(getAuthID())) return;

      // Now see if the role has been granted any privileges.
      // TODO: could allow priv grants if no dependent objects.
      PrivMgr privMgr(std::string(privMgrMDLoc), CmpCommon::diags());
      std::vector<PrivClass> privClasses;

      privClasses.push_back(PrivClass::ALL);
      std::vector<int64_t> objectUIDs;
      bool hasComponentPriv = false;

      if (privMgr.isAuthIDGrantedPrivs(getAuthID(), privClasses, objectUIDs, hasComponentPriv)) {
        NAString privGrantee;
        if (hasComponentPriv)
          privGrantee = "one or components";
        else
          // If the objectName is empty, then it is no longer defined,
          // continue.
          privGrantee = getObjectName(objectUIDs);

        if (privGrantee.length() > 0) {
          *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_HAS_PRIVS_NO_DROP) << DgString0(roleName.data())
                              << DgString1(privGrantee.data());
          return;
        }
      }

      // Does role own any resources
      if (msg_license_multitenancy_enabled()) {
        NAString resourceNames;
        long rowCount = CmpSeabaseDDL::authIDOwnsResources(getAuthID(), resourceNames);
        if (rowCount < 0) return;
        if (rowCount > 0) {
          *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_OWNS_RESOURCES) << DgString0(roleName.data())
                              << DgString1(resourceNames.data());
          return;
        }
      }

      // Role has not been granted and no privileges have been granted to
      // the role.  Remove the system grants.  Every role has a grant from
      // system (_SYSTEM) to the role creator.  It may have a grant from
      // system (_SYSTEM) to a detached role.  Remove both.
      PrivStatus privStatus = role.revokeRoleFromCreator(getAuthID(), getAuthCreator(), true /*remove all*/);

      if (privStatus != PrivStatus::STATUS_GOOD) {
        SEABASEDDL_INTERNAL_ERROR("Unable to revoke role from role creator");
        return;
      }

      // revoke admin role from tenant
      if (isAuthAdmin()) {
        std::string tenantName;
        TenantInfo tenantForRole(STMTHEAP);
        if (TenantInfo::getTenantInfo(getAuthID(), tenantForRole) == 0) {
          tenantForRole.setAdminRoleID(NA_UserIdDefault);
          if (tenantForRole.updateTenantInfo(true, NULL) == -1) {
            SEABASEDDL_INTERNAL_ERROR("Unable to revoke role administrator from tenant");
            return;
          }
        }
      }
    }

    // delete the row
    deleteRow(roleName);
  } catch (...) {
    // At this time, an error should be in the diags area.
    // If there is no error, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("Switch statement in CmpSeabaseDDLrole::dropRole");
  }
}

// ----------------------------------------------------------------------------
// Public method: dropStandardRole
//
// Drops a standard role (ie. DB__nameROLE) from the Trafodion metadata
//
// Input:  role name
// ----------------------------------------------------------------------------
void CmpSeabaseDDLrole::dropStandardRole(const std::string roleName)

{
  // Verify name is a standard name

  size_t prefixLength = strlen(RESERVED_AUTH_NAME_PREFIX);

  if (roleName.size() <= prefixLength || roleName.compare(0, prefixLength, RESERVED_AUTH_NAME_PREFIX) != 0) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_NOT_EXIST) << DgString0(roleName.c_str());
    return;
  }
  // delete the row
  deleteRow(roleName.c_str());
}

// ----------------------------------------------------------------------------
// public method: getRoleDetails
//
// Create the CmpSeabaseDDLuser class containing user details for the
// requested username
//
// Input:
//    userName - the database username to retrieve details for
//    isExternal -
//       true - the username is the external name (auth_ext_name)
//       false - the username is the database name (auth_db_name)
//
//  Output:
//    Returned parameter:
//       STATUS_GOOD: authorization details are populated:
//       STATUS_NOTFOUND: authorization details were not found
//       STATUS_WARNING: (not 100) warning was returned, diags area populated
//       STATUS_ERROR: error was returned, diags area populated
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLrole::getRoleDetails(const char *pRoleName) {
  CmpSeabaseDDLauth::AuthStatus retcode = getAuthDetails(pRoleName, false);
  if (retcode == STATUS_GOOD && !isRole()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_A_ROLE) << DgString0(getAuthDbName().data());
    retcode = STATUS_ERROR;
  }
  return retcode;
}

// ----------------------------------------------------------------------------
// Public method: getRoleIDFromRoleName
//
// Lookup a role name in the Trafodion metadata
//
// Input:  Role name to lookup
// Output: Role ID if role was found
//    true returned if role found
//   false returned if role not found
// ----------------------------------------------------------------------------
bool CmpSeabaseDDLrole::getRoleIDFromRoleName(const char *roleName, int &roleID)

{
  CmpSeabaseDDLauth::AuthStatus authStatus = getAuthDetails(roleName, false);

  if (authStatus != STATUS_GOOD && authStatus != STATUS_WARNING) return false;

  if (getAuthType() != COM_ROLE_CLASS) return false;

  roleID = getAuthID();
  return true;
}

// ****************************************************************************
// Class CmpSeabaseDDLtenant methods
// ****************************************************************************

// ----------------------------------------------------------------------------
// Constructor
// ----------------------------------------------------------------------------
CmpSeabaseDDLtenant::CmpSeabaseDDLtenant(const NAString &systemCatalog, const NAString &MDSchema)
    : CmpSeabaseDDLauth(systemCatalog, MDSchema)

{
  isDetached_ = false;
}

CmpSeabaseDDLtenant::CmpSeabaseDDLtenant() : CmpSeabaseDDLauth() { isDetached_ = false; }

// ----------------------------------------------------------------------------
// public method: getTenantDetails
//
// Initializes the CmpSeabaseDDLtenant class containing tenant details for the
// requested tenantID and its usages
//
// Input:
//    tenantID
//
//  Output:
//    populated TenantInfo class
//
// Returns:
//   STATUS_GOOD: authorization details are populated:
//   STATUS_NOTFOUND: authorization details were not found
//   STATUS_ERROR: error was returned, diags area populated
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLtenant::getTenantDetails(int tenantID, TenantInfo &tenantInfo) {
  if (!msg_license_multitenancy_enabled())
    SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLtenant::getTenantDetails, Invalid operation, tenant feature unavailable");

  if (!isTenantID(tenantID)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(getAuthDbName().data())
                        << DgString1("tenant");
    return STATUS_ERROR;
  }

  CmpSeabaseDDLauth::AuthStatus retcode = getAuthDetails(tenantID);
  if (retcode != STATUS_GOOD) return retcode;

  tenantInfo.setTenantName(getAuthDbName());

  // Read TENANTS to get attributes for the tenant
  int rc = TenantInfo::getTenantInfo(tenantID, tenantInfo);
  if (rc == 100) return STATUS_NOTFOUND;

  return (rc == 0) ? STATUS_GOOD : STATUS_ERROR;
}

// ----------------------------------------------------------------------------
// public method: getTenantDetails
//
// Initializes the CmpSeabaseDDLtenant class containing tenant details for the
// requested tenant name and its usages
//
// Input:
//    tenant  name
//
//  Output:
//    populated TenantInfo class
//
// Returns:
//   STATUS_GOOD: authorization details are populated:
//   STATUS_NOTFOUND: authorization details were not found
//   STATUS_ERROR: error was returned, diags area populated
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLtenant::getTenantDetails(const char *pTenantName, TenantInfo &tenantInfo) {
  if (!msg_license_multitenancy_enabled())
    SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLtenant::getTenantDetails, Invalid operation, tenant feature unavailable");

  CmpSeabaseDDLauth::AuthStatus retcode = getAuthDetails(pTenantName, false);
  if (retcode != STATUS_GOOD) return retcode;

  if (!isTenant()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(getAuthDbName().data())
                        << DgString1("tenant");
    return STATUS_ERROR;
  }

  tenantInfo.setTenantName(pTenantName);

  // Read TENANTS to get attributes for the tenant
  int rc = TenantInfo::getTenantInfo(getAuthID(), tenantInfo);
  if (rc == 100) return STATUS_NOTFOUND;

  return (rc == 0) ? STATUS_GOOD : STATUS_ERROR;
}

// ----------------------------------------------------------------------------
// public method: getTenantDetails
//
// Initializes the CmpSeabaseDDLtenant class containing tenant details for the
// requested tenant name and its usages
//
// Input:
//   schema UID = UID of one of the schemas that belong to the tenant
//
//  Output:
//    populated TenantInfo class
//
// Returns:
//   STATUS_GOOD: authorization details are populated:
//   STATUS_NOTFOUND: authorization details were not found
//   STATUS_ERROR: error was returned, diags area populated
// ----------------------------------------------------------------------------
#if 0
CmpSeabaseDDLauth::AuthStatus
CmpSeabaseDDLtenant::getTenantDetails(
  const long schemaUID,
  TenantInfo &tenantInfo)
{
  if (!msg_license_multitenancy_enabled())
    SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLtenant::getTenantDetails, Invalid operation, tenant feature unavailable");
    
  CmpSeabaseDDLauth::AuthStatus retcode = getAuthDetails(pTenantName, false);
  if (retcode != STATUS_GOOD)
    return retcode;

  if (!isTenant())
  {
    *CmpCommon::diags() << DgSqlCode (-CAT_IS_NOT_CORRECT_AUTHID)
                        << DgString0(getAuthDbName().data())
                        << DgString1("tenant");
     return STATUS_ERROR;
  }

  // Read TENANTS to get attributes for the tenant
  int rc = TenantInfo::getTenantInfo(getAuthID(), tenantInfo);
  if (rc == 100)
    return STATUS_NOTFOUND;

  return (rc == 0) ? STATUS_GOOD : STATUS_ERROR;
}
#endif

// -----------------------------------------------------------------------------
// method:  findDetachedTenant
//
// If the specified role already exists, returns the roleID to be used as the
// administrator for the tenant given these constraints:
//   role ID is really a role
//   roleID not used by another active tenant
//   roleID is associated with a detached tenant, return the detached tenant ID
//
// Returns:
//    adminRoleID
//        0 - calculate a fresh adminRoleID
//       >0 - roleID to use
//    tenantID
//        0 - calculate a fresh tenantID
//       >0 - tenantID to use
// -----------------------------------------------------------------------------
short CmpSeabaseDDLtenant::findDetachedTenant(const StmtDDLTenant *pNode, int &adminRoleID, int &tenantID) {
  tenantID = 0;
  adminRoleID = 0;

  CmpSeabaseDDLauth authInfo;
  CmpSeabaseDDLauth::AuthStatus authStatus = authInfo.getAuthDetails(pNode->getRoleName()->data(), true);
  if (authStatus == STATUS_ERROR) return -1;
  if (authStatus == STATUS_NOTFOUND) return 0;

  adminRoleID = authInfo.getAuthID();

  // authID exists
  // verify that the authID is a role, tenants are currently owned by roles
  if (!isRoleID(adminRoleID) /* && !isUserID(adminRoleID)*/) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_UNAVAILABLE) << DgString0(pNode->getRoleName()->data())
                        << DgString1(getAuthDbName().data());
    return -1;
  }

  // verify that authID is not used by another tenant
  TenantInfo tenantForRole(STMTHEAP);
  int retcode = TenantInfo::getTenantInfo(adminRoleID, tenantForRole);

  if (retcode <= 0) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_UNAVAILABLE) << DgString0(pNode->getRoleName()->data())
                        << DgString1(getAuthDbName().data());
    return -1;
  }

  // see if authID is associated with a detached tenant
  // if so, reuse the tenantID from the previously detached tenant
  PrivMgrRoles roles;
  std::vector<int32_t> roleIDs;
  roleIDs.push_back(adminRoleID);
  std::vector<int32_t> granteeIDs;

  // get list of detached tenants (granteeIDs)
  PrivStatus privStatus = roles.fetchGranteesForRoles(roleIDs, granteeIDs, true, true);
  if (privStatus == PrivStatus::STATUS_ERROR) return -1;

  // zero or one tenant can be granted admin role
  for (size_t i = 0; i < granteeIDs.size(); i++) {
    if (isTenantID(granteeIDs[i])) {
      tenantID = granteeIDs[i];
      break;
    }
  }

  if (tenantID > 0) setIsDetached(true);

  return 0;
}

// -----------------------------------------------------------------------------
// method: generateSchemaUsages
//
// Returns a TenantUsageList of all the schemas that are owned by the passed
// in adminRoleID. It reads the OBJECTS table where the admin role ID matches
// the schema owner.
//
// Returns:
//    -1 = unexpected error reading metadata
//     0 = operation successful
// -----------------------------------------------------------------------------
short CmpSeabaseDDLtenant::generateSchemaUsages(const int &tenantID, const int &adminRoleID,
                                                TenantUsageList *&existingUsageList) {
  // Get the list of schemas that are owned by the adminRoleID
  std::vector<UIDAndOwner> objectRows;
  char buf[1000];
  snprintf(buf, sizeof(buf),
           "where object_type in ('PS', 'SS') and "
           "object_owner = %d",
           adminRoleID);

  std::string whereClause(buf);
  std::string orderByClause(" ORDER BY OBJECT_UID");
  PrivMgrObjects objects;
  PrivStatus privStatus = objects.fetchUIDandOwner(whereClause, orderByClause, objectRows);
  if (privStatus == PrivStatus::STATUS_ERROR) return -1;
  if (objectRows.size() == 0) return 0;

  for (size_t i = 0; i < objectRows.size(); i++) {
    TenantUsage usage(tenantID, objectRows[i].UID, TenantUsage::TENANT_USAGE_SCHEMA);
    usage.setIsNew(true);
    existingUsageList->insert(usage);
  }
  return 0;
}

// -----------------------------------------------------------------------------
// method: generateTenantID
//
// Determine unique auth ID for tenant.
// Cannot use current method because AUTHS table does not include
// detached tenant IDs.  Since the number of tenants will be small
// generate the list of existing tenants, add the detached tenants and
// find a gap
// -----------------------------------------------------------------------------
short CmpSeabaseDDLtenant::generateTenantID(int &tenantID) {
  if (tenantID == 0) {
    // get list of all tenant IDs from AUTHS table
    NAString whereClause("where auth_id >= 1500100 and auth_id < 1599999");
    std::set<int32_t> tenantIDs;
    CmpSeabaseDDLauth::AuthStatus authStatus = selectAuthIDs(whereClause, tenantIDs);
    if (authStatus == STATUS_ERROR) return -1;

    // add any detached tenants from role usage table
    PrivMgrRoles roles;
    std::vector<int32_t> granteeIDs;
    PrivStatus privStatus = roles.fetchTenantGrantees(granteeIDs);
    if (privStatus == PrivStatus::STATUS_ERROR) return -1;

    for (size_t i = 0; i < granteeIDs.size(); i++) tenantIDs.insert(granteeIDs[i]);

    // we now have a list of all the used tenantIDs
    // Find the first entry that is missing the next value in numeric order
    //   that is, tenantID + 1 does not match the next tenantID in the list
    tenantID = ROOT_TENANTID - 1;
    for (std::set<int32_t>::iterator it = tenantIDs.begin(); it != tenantIDs.end(); ++it) {
      int32_t tempID = *it;
      if ((tenantID + 1) != tempID) break;
      tenantID = tempID;
    }

    // add one to get the unused entry
    tenantID++;
  }

  if (tenantID > MAX_TENANTID) {
    SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLauth::getUniqueAuthID failed, ran out of available IDs");
    return -1;
  }

  return 0;
}

// ----------------------------------------------------------------------------
// method:  getSchemaDetails
//
// This method returns the schema UID and schema owner given the schema name
//
// Input:  Schema name
//
// Output:  schema UID and schema owner
//
// Returns the results of the operation:
//  STATUS_GOOD - successful
//  STATUS_NOTFOUND - schema not found
//  STATUS_ERROR - unexpected error occurred (diags area has been populated)
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLtenant::getSchemaDetails(ExeCliInterface *cliInterface,
                                                                    const SchemaName *schemaName, long &schUID,
                                                                    ComObjectType &schType, int &schOwner) {
  CMPASSERT(schemaName);

  NAString catName = schemaName->getCatalogName();
  NAString schName = schemaName->getSchemaName();

  // Get schema details
  NAString objName(SEABASE_SCHEMA_OBJECTNAME);

  CmpSeabaseDDL cmpSBD(STMTHEAP);
  schUID =
      cmpSBD.getObjectTypeandOwner(cliInterface, catName.data(), schName.data(), objName.data(), schType, schOwner);
  if (schUID == -1) {
    *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR) << DgString0(catName.data())
                        << DgString1(schName.data());
    return STATUS_ERROR;
  }

  return STATUS_GOOD;
}

// -----------------------------------------------------------------------------
// method: addGroups
//
// Add groups to usages list
// Register any groups that don't exist and set status to auto created
//
// throws UserException if unsuccessful (ComDiags area is setup)
// -----------------------------------------------------------------------------
void CmpSeabaseDDLtenant::addGroups(const StmtDDLTenant *pNode, const int tenantID,
                                    const TenantUsageList *tenantGroupList, TenantUsageList *&usageList) {
  assert(usageList);

  // Get list of groups
  ElemDDLNode *groupList = (ElemDDLNode *)pNode->getGroupList();
  if (groupList == NULL || groupList->entries() == 0) return;

  for (CollIndex i = 0; i < groupList->entries(); i++) {
    ElemDDLTenantGroup *group = (*groupList)[i]->castToElemDDLTenantGroup();

    // Get groupID, if group already exists
    CmpSeabaseDDLgroup groupInfo;
    CmpSeabaseDDLauth::AuthStatus retcode = groupInfo.getGroupDetails(group->getGroupName().data());
    if (retcode == STATUS_ERROR) {
      UserException excp(NULL, 0);
      throw excp;
    }

    int groupID = (retcode == STATUS_GOOD) ? groupInfo.getAuthID() : NA_UserIdDefault;
    // If group already exists and config specified, should they be the same?

    // Register group
    if (groupID == NA_UserIdDefault) {
      if (CmpCommon::getDefault(TRAF_AUTO_REGISTER_GROUP) == DF_OFF) {
        *CmpCommon::diags() << DgSqlCode(-CAT_GROUP_NOT_EXIST) << DgString0(group->getGroupName().data());
        UserException excp(NULL, 0);
        throw excp;
      }

      groupID = groupInfo.registerGroupInternal(group->getGroupName(), group->getConfig(), true);
      if (groupID <= 0) {
        UserException excp(NULL, 0);
        throw excp;
      }
    }

    // Verify that group is not already part of the tenant
    else {
      if (tenantGroupList != NULL) {
        for (CollIndex j = 0; j < tenantGroupList->entries(); j++) {
          TenantUsage groupUsage = (*tenantGroupList)[j];
          if (groupUsage.getUsageID() == (long)groupID) {
            *CmpCommon::diags() << DgSqlCode(-CAT_GROUP_ALREADY_DEFINED) << DgString0(group->getGroupName().data())
                                << DgString1(pNode->getTenantName().data());
            UserException excp(NULL, 0);
            throw excp;
          }
        }
      }
    }

    // Add group to usages
    TenantUsage tenantUsage(tenantID, groupID, TenantUsage::TENANT_USAGE_GROUP);
    usageList->insert(tenantUsage);
  }
  return;
}

// -----------------------------------------------------------------------------
// method: dropGroups
//
// drop group/tenant relationships
//
// throws UserException if unsuccessful (ComDiags area is setup)
// -----------------------------------------------------------------------------
void CmpSeabaseDDLtenant::dropGroups(const StmtDDLTenant *pNode, TenantInfo &tenantInfo, TenantUsageList *&usageList) {
  // Get list of groups, if none, just return
  ElemDDLNode *groupList = (ElemDDLNode *)pNode->getGroupList();
  if (groupList == NULL || groupList->entries() == 0) return;

  assert(usageList);

  // Generate the list of usages to remove
  for (CollIndex i = 0; i < groupList->entries(); i++) {
    ElemDDLTenantGroup *group = (*groupList)[i]->castToElemDDLTenantGroup();

    // Error if group does not exist
    CmpSeabaseDDLgroup groupInfo;
    CmpSeabaseDDLauth::AuthStatus retcode = groupInfo.getGroupDetails(group->getGroupName().data());
    if (retcode == STATUS_ERROR) {
      UserException excp(NULL, 0);
      throw excp;
    }

    if (retcode == STATUS_NOTFOUND || !isUserGroupID(groupInfo.getAuthID())) {
      *CmpCommon::diags() << DgSqlCode(-CAT_GROUP_NOT_EXIST) << DgString0(group->getGroupName().data());
      UserException excp(NULL, 0);
      throw excp;
    }

    // verify that group is part of tenant
    // return an error?
    long usageUID = groupInfo.getAuthID();
    TenantUsage tenantUsage(tenantInfo.getTenantID(), usageUID, TenantUsage::TENANT_USAGE_GROUP);
    if (tenantInfo.findTenantUsage(usageUID, tenantUsage)) {
      // Add group to list to remove
      usageList->insert(tenantUsage);
    } else {
      *CmpCommon::diags() << DgSqlCode(-CAT_NOT_TENANT_USAGE) << DgString0("User group")
                          << DgString1(group->getGroupName().data()) << DgString2(pNode->getTenantName().data());
      UserException excp(NULL, 0);
      throw excp;
    }
  }
}

// -----------------------------------------------------------------------------
// method: addResources
//
// Input:
//  - user request (parse node)
//  - cliInterface to read metadata
//  - tenant details
//

// Returns:
//  - ordered unique list of nodes available to the tenant (outNodeList)
//  - new tenant-rgroup usages (usageList) to insert into TENANT_USAGE
//  - new node-tenant usages(resourceUsageList) to insert into RESOURCE_USAGE
//
// If a resource group had not been specified, RGROUP_DEFAULT is assumed.
//   RGROUP_DEFAULT assumes all nodes available in the cluster.
//
// If a node resource does not exist (RESOURCES), it will be added.
//
// throws UserException if unsuccessful (ComDiags area is setup)
// -----------------------------------------------------------------------------
void CmpSeabaseDDLtenant::addResources(const StmtDDLTenant *pNode, ExeCliInterface *cliInterface,
                                       TenantInfo &tenantInfo, TenantNodeInfoList *&outNodeList,
                                       TenantUsageList *&usageList, TenantResourceUsageList *&resourceUsageList) {
  // Lists must be pre-allocated
  assert(usageList);
  assert(outNodeList);
  assert(resourceUsageList);

  NAString defGroup(RGROUP_DEFAULT);
  bool rgroupAllocated(false);
  Int16 retcode = 0;

  ElemDDLNode *rgroupInputList = (ElemDDLNode *)pNode->getRGroupList();
  NAList<NAString> rgroupNameList(STMTHEAP, rgroupInputList->entries());
  if (rgroupInputList == NULL || rgroupInputList->entries() == 0)
    rgroupNameList.insert(defGroup);
  else {
    for (CollIndex i = 0; i < rgroupInputList->entries(); i++) {
      ElemDDLTenantResourceGroup *group = (*rgroupInputList)[i]->castToElemDDLTenantResourceGroup();
      rgroupNameList.insert(group->getGroupName());
    }
  }

  bool defGroupRequested = rgroupNameList.contains(defGroup);

  // See if tenant already has the DB__RGROUP_DEFAULT assigned
  bool defGroupExists = false;
  if (tenantInfo.getUsageList()->entries() == 1) {
    TenantResource existingDefGroup(cliInterface, STMTHEAP);
    retcode = getTenantResource(defGroup, false /*isNode*/, false /*fetch usages*/, existingDefGroup);
    assert(retcode == 0);  // default group should always exist
    TenantUsage tenantDefGroup;
    defGroupExists = tenantInfo.findTenantUsage(existingDefGroup.getResourceUID(), tenantDefGroup);
  }

  bool isReplace = pNode->replaceRGroupList();

  // Error checks - cannot specify default resource group (defGroup) with
  // other resource groups:
  // 1 - error: requested defGroup and other non default resource groups
  // 2 - error: not replacing resource groups and requested defGroup
  //     but tenant is assigned one or more non default resource groups
  // 3 - error: not replacing resource groups and requested non default
  //     resource groups but tenant is assigned defGroup
  if (/*1*/ (defGroupRequested && (rgroupNameList.entries() > 1)) ||
      /*2*/ (!isReplace && defGroupRequested && tenantInfo.getUsageList()->entries() > 0 && !defGroupExists) ||
      /*3*/ (!isReplace && !defGroupRequested && defGroupExists)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_TENANT_RESOURCE_GROUPS) << DgString0(defGroup.data())
                        << DgString1(getAuthDbName().data());
    UserException excp(NULL, 0);
    throw excp;
  }

  // Get node name/nodeID for the existing list of resource groups
  if (!isReplace) {
    TenantNodeInfoList *existingNodeList = new (STMTHEAP) TenantNodeInfoList(STMTHEAP);
    if (getNodeDetails(resourceUsageList, existingNodeList) == -1) {
      NADELETE(existingNodeList, TenantNodeInfoList, STMTHEAP);
      UserException excp(NULL, 0);
      throw excp;
    }

    // Add existing nodes to outNodeList
    for (CollIndex i = 0; i < existingNodeList->entries(); i++) {
      TenantNodeInfo *nodeInfo = (*existingNodeList)[i];
      if (!outNodeList->contains(nodeInfo->getNodeName())) outNodeList->orderedInsert(nodeInfo);
    }
    NADELETE(existingNodeList, TenantNodeInfoList, STMTHEAP);
  }

  // Add nodes for new resource groups
  for (CollIndex i = 0; i < rgroupNameList.entries(); i++) {
    NAString rgroupName = rgroupNameList[i];

    // Read RESOURCES for the resource group and RESOURCE_USAGE for its
    // associated nodes.  Return an error if the resource group does not exist
    TenantResource rgroup(cliInterface, STMTHEAP);
    retcode = getTenantResource(rgroupName, true /*isNode*/, true /*fetch usages*/, rgroup);
    if (retcode == -1) {
      UserException excp(NULL, 0);
      throw excp;
    }
    if (retcode == 1) {
      *CmpCommon::diags() << DgSqlCode(-CAT_RESOURCE_GROUP_NOT_EXIST) << DgString0(rgroupName.data());
      UserException excp(NULL, 0);
      throw excp;
    }

    // See if tenant has already been allocated resource
    TenantUsage tenantUsage;
    rgroupAllocated = tenantInfo.findTenantUsage(rgroup.getResourceUID(), tenantUsage);

    if (rgroupAllocated) {
      // turn off obsolete so it won't get deleted
      for (int i = 0; i < usageList->entries(); i++) {
        if ((*usageList)[i].getUsageID() == tenantUsage.getUsageID() &&
            (*usageList)[i].getTenantID() == tenantUsage.getTenantID())
          (*usageList)[i].setIsObsolete(false);
      }
    }

    // Not allocated, add tenant-rgroup usage
    else {
      TenantUsage newTenantUsage(getAuthID(), rgroup.getResourceUID(), TenantUsage::TENANT_USAGE_RESOURCE);
      newTenantUsage.setIsNew(true);
      usageList->insert(newTenantUsage);
    }

    // Modify existing node-tenant usages
    TenantResourceUsageList *rgroupUsageList = rgroup.getUsageList();
    for (int j = 0; j < rgroupUsageList->entries(); j++) {
      TenantResourceUsage *existingUsage = (*rgroupUsageList)[j];
      NAString usageName = existingUsage->getUsageName();
      TenantResourceUsage *resourceUsage =
          resourceUsageList->findResource(&usageName, getAuthID(), TenantResourceUsage::RESOURCE_USAGE_TENANT);

      // Usage found, may be part of another resource group
      if (resourceUsage) resourceUsage->setIsObsolete(false);

      // Usage not found, add it
      else {
        TenantResourceUsage *newUsage =
            new (STMTHEAP) TenantResourceUsage(existingUsage->getUsageUID(), existingUsage->getUsageName(), getAuthID(),
                                               getAuthDbName(), TenantResourceUsage::RESOURCE_USAGE_TENANT);
        newUsage->setIsNew(true);
        resourceUsageList->insert(newUsage);
      }
    }

    // Get the list of nodes assigned to the resource group
    TenantNodeInfoList *nodeList = new (STMTHEAP) TenantNodeInfoList(STMTHEAP);
    if (rgroupName == defGroup)
      retcode = getNodeDetails(cliInterface, nodeList);
    else
      retcode = getNodeDetails(rgroup.getUsageList(), nodeList);
    if (retcode == -1) {
      NADELETE(nodeList, TenantNodeInfoList, STMTHEAP);
      UserException excp(NULL, 0);
      throw excp;
    }

    // In case there are additional nodes that were not part of the
    // original resourceUsageList, add them now
    for (CollIndex i = 0; i < nodeList->entries(); i++) {
      TenantNodeInfo *nodeInfo = (*nodeList)[i];
      NAString nodeName = nodeInfo->getNodeName();
      TenantResourceUsage *existingUsage =
          resourceUsageList->findResource(&nodeName, getAuthID(), TenantResourceUsage::RESOURCE_USAGE_TENANT);
      if (!existingUsage) {
        TenantResourceUsage *newUsage = new (STMTHEAP)
            TenantResourceUsage(nodeInfo->getMetadataNodeID(), nodeInfo->getNodeName(), getAuthID(), getAuthDbName(),
                                TenantResourceUsage::RESOURCE_USAGE_TENANT, nodeInfo->getNodeWeight());
        newUsage->setIsNew(true);
        resourceUsageList->insert(newUsage);
      } else
        existingUsage->setIsObsolete(false);

      // Update list of ascending unique nodes (outNodeList) for this resource group
      // Update list of node-tenant usages (resourceUsageList) that associate the
      // node with a tenant.  The node-tenant usage store the units allocated to
      // the tenant.
      if (!outNodeList->contains(nodeInfo->getNodeName())) outNodeList->orderedInsert(nodeInfo);
    }
    NADELETE(nodeList, TenantNodeInfoList, STMTHEAP);
  }
}

// -----------------------------------------------------------------------------
// method: alterResources
//
// Input:
//  - cliInterface to read metadata
//  - tenant details
//

// Returns:
//  - ordered unique list of nodes available to the tenant (outNodeList)
//  - updated node-tenant usages(resourceUsageList)
//
// throws UserException if unsuccessful (ComDiags area is setup)
// -----------------------------------------------------------------------------
void CmpSeabaseDDLtenant::alterResources(ExeCliInterface *cliInterface, TenantInfo &tenantInfo,
                                         TenantNodeInfoList *&outNodeList,
                                         TenantResourceUsageList *&resourceUsageList) {
  // Lists must be pre-allocated
  assert(outNodeList);
  assert(resourceUsageList);

  TenantResourceUsageList *updatedUsageList = new (STMTHEAP) TenantResourceUsageList(cliInterface, STMTHEAP);
  if (getNodesForRGroups(tenantInfo.getUsageList(), updatedUsageList) == -1) {
    NADELETE(updatedUsageList, TenantResourceUsageList, STMTHEAP);
    UserException excp(NULL, 0);
    throw excp;
  }

  // Get node name/nodeID for the update list of usages
  TenantNodeInfoList *nodeList = new (STMTHEAP) TenantNodeInfoList(STMTHEAP);
  if (getNodeDetails(updatedUsageList, nodeList) == -1) {
    NADELETE(nodeList, TenantNodeInfoList, STMTHEAP);
    NADELETE(updatedUsageList, TenantResourceUsageList, STMTHEAP);
    UserException excp(NULL, 0);
    throw excp;
  }
  NADELETE(updatedUsageList, TenantResourceUsageList, STMTHEAP);

  for (CollIndex i = 0; i < nodeList->entries(); i++) {
    // Populate the outNodeList with the adjusted node list
    TenantNodeInfo *nodeInfo = (*nodeList)[i];
    NAString nodeName = nodeInfo->getNodeName();
    if (!outNodeList->contains(nodeName)) outNodeList->orderedInsert(nodeInfo);

    // If node is new, add it to the resourceUsageList
    TenantResourceUsage *rUsage =
        resourceUsageList->findResource(&nodeName, getAuthID(), TenantResourceUsage::RESOURCE_USAGE_TENANT);
    if (rUsage)
      rUsage->setUsageValue(-1);
    else {
      TenantResourceUsage *newUsage =
          new (STMTHEAP) TenantResourceUsage(nodeInfo->getMetadataNodeID(), nodeName, getAuthID(), getAuthDbName(),
                                             TenantResourceUsage::RESOURCE_USAGE_TENANT);
      newUsage->setIsNew(true);
      resourceUsageList->insert(newUsage);
    }
  }
}

// -----------------------------------------------------------------------------
// method: dropResources
//
// Called during alter tenant drop resource groups.  It returns the usages that
// need to be removed and the list of nodes now available for tenant placement
//
// Input:
//  - user request (parse node)
//  - cliInterface to read metadata
//  - tenant details
//  - resourceUsageList - original node-tenant usages
//
// Returns:
//  - outNodeList: ordered unique list of nodes available to the tenant
//  - usageList: tenant-rgroup usages with entries to delete marked as invalid
//  - resourceUsageList: original node-tenant usages with entries marked obsolete
//
// throws UserException if unsuccessful (ComDiags area is setup)
// -----------------------------------------------------------------------------
void CmpSeabaseDDLtenant::dropResources(const StmtDDLTenant *pNode, ExeCliInterface *cliInterface,
                                        TenantInfo &tenantInfo, TenantNodeInfoList *&outNodeList,
                                        TenantUsageList *&usageList, TenantResourceUsageList *&resourceUsageList) {
  // Lists must be pre-allocated
  assert(usageList);
  assert(outNodeList);
  assert(resourceUsageList);

  Int16 retcode = 0;

  // Mark dropped tenant-resource group usages invalid
  ElemDDLNode *rgroupInputList = (ElemDDLNode *)pNode->getRGroupList();
  for (CollIndex i = 0; i < rgroupInputList->entries(); i++) {
    // rgroupInputList contains the rgroup name to remove from the tenant
    ElemDDLTenantResourceGroup *rgroupInput = (*rgroupInputList)[i]->castToElemDDLTenantResourceGroup();
    NAString rgroupName = rgroupInput->getGroupName();

    // Verify resource group exists
    TenantResource rgroup(cliInterface, STMTHEAP);
    retcode = getTenantResource(rgroupName, true /*isNode*/, true /*fetch usages*/, rgroup);
    if (retcode == -1) {
      UserException excp(NULL, 0);
      throw excp;
    }
    if (retcode == 1) {
      *CmpCommon::diags() << DgSqlCode(-CAT_RESOURCE_GROUP_NOT_EXIST) << DgString0(rgroupName.data());
      UserException excp(NULL, 0);
      throw excp;
    }

    // Search the list of resource groups assigned to the tenant.  Make sure
    // resource group has been assigned to the tenant.
    TenantUsage tenantUsage;
    if (!tenantInfo.findTenantUsage(rgroup.getResourceUID(), tenantUsage)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_RESOURCE_GROUP_NOT_ASSIGNED) << DgString0(rgroupName.data())
                          << DgString1(getAuthDbName().data());
      UserException excp(NULL, 0);
      throw excp;
    }

    // mark the tenant-resource group usage as obsolete (to be removed)
    tenantUsage.setIsObsolete(true);
    tenantInfo.setTenantUsageObsolete(rgroup.getResourceUID(), true);
    usageList->insert(tenantUsage);
  }

  // return an error if all resource groups are dropped
  int numRGroups = tenantInfo.getNumberRGroupUsages(true);
  if (numRGroups == 0) {
    *CmpCommon::diags() << DgSqlCode(-CAT_LAST_RESOURCE_GROUP) << DgString0(getAuthDbName().data());
    UserException excp(NULL, 0);
    throw excp;
  }

  // get list of rgroup - nodes based on remaining resource groups
  TenantResourceUsageList *updatedUsageList = new (STMTHEAP) TenantResourceUsageList(cliInterface, STMTHEAP);
  retcode = getNodesForRGroups(tenantInfo.getUsageList(), updatedUsageList);
  if (retcode == -1) {
    NADELETE(updatedUsageList, TenantResourceUsageList, STMTHEAP);
    UserException excp(NULL, 0);
    throw excp;
  }

  // Get node name/nodeID for the update list of usages
  TenantNodeInfoList *nodeList = new (STMTHEAP) TenantNodeInfoList(STMTHEAP);
  if (getNodeDetails(updatedUsageList, nodeList) == -1) {
    NADELETE(nodeList, TenantNodeInfoList, STMTHEAP);
    NADELETE(updatedUsageList, TenantResourceUsageList, STMTHEAP);
    UserException excp(NULL, 0);
    throw excp;
  }
  NADELETE(updatedUsageList, TenantResourceUsageList, STMTHEAP);

  // Populate the outNodeList with the adjusted node list
  for (CollIndex i = 0; i < nodeList->entries(); i++) {
    TenantNodeInfo *nodeInfo = (*nodeList)[i];
    if (!outNodeList->contains(nodeInfo->getNodeName())) outNodeList->orderedInsert(nodeInfo);
  }

  // Update list of node-tenant usages (resourceUsageList)
  for (CollIndex i = 0; i < resourceUsageList->entries(); i++) {
    TenantResourceUsage *rUsage = (*resourceUsageList)[i];

    // Node is still available, reset value
    if (nodeList->contains(rUsage->getResourceName())) rUsage->setUsageValue(-1);

    // Node is no longer available, usage should be removed from metadata
    else
      rUsage->setIsObsolete(true);
  }

  NADELETE(nodeList, TenantNodeInfoList, STMTHEAP);
}

// -----------------------------------------------------------------------------
// method: getNodeDetails
//
// Returns a list of nodes (nodesToAdd) to assign to the tenant based on all
// the nodes defined in the cluster
// ----------------------------------------------------------------------------
short CmpSeabaseDDLtenant::getNodeDetails(ExeCliInterface *cliInterface, TenantNodeInfoList *&nodesToAdd) {
  int numNodes = CURRCONTEXT_CLUSTERINFO->hasVirtualNodes() ? 1 : CURRCONTEXT_CLUSTERINFO->getTotalNumberOfCPUs();
  const NAArray<CollIndex> &nodeList = CURRCONTEXT_CLUSTERINFO->getCPUArray();

  // TBD: For performance, do a single metadata select that returns the list of
  // nodes that have not yet been added.
  for (int i = 0; i < numNodes; i++) {
    CollIndex nodeID = nodeList[i];
    NAString nodeName;
    bool result = CURRCONTEXT_CLUSTERINFO->mapNodeIdToNodeName(nodeID, nodeName);
    TenantResource node(cliInterface, STMTHEAP);
    if (node.generateNodeResource(&nodeName, ComUser::getCurrentUser()) == -1) return -1;

    TenantNodeInfo *newNodeInfo =
        new (STMTHEAP) TenantNodeInfo(nodeName, nodeID, -1, node.getResourceUID(), -1, STMTHEAP);
    nodesToAdd->insert(newNodeInfo);
  }
  return 0;
}

// -----------------------------------------------------------------------------
// method: getNodeDetails
//
// Returns a list of node (nodesToAdd) to assign to the tenant based on all
// the nodes defined in the resource group (nodesInRGroup).
// ----------------------------------------------------------------------------
short CmpSeabaseDDLtenant::getNodeDetails(const TenantResourceUsageList *nodesInRGroup,
                                          TenantNodeInfoList *&nodesToAdd) {
  NAList<NAString> nodeNameList(STMTHEAP);
  bool useTempNodes = false;

  // create a list of nodeNames/units from cqd
  std::string tempNodes = CmpCommon::getDefaultString(NODE_ALLOCATIONS).data();
  if (CURRCONTEXT_CLUSTERINFO->hasVirtualNodes() && tempNodes.size() > 0) {
    useTempNodes = true;
    size_t pos1 = 0;
    size_t pos2 = 0;
    char nodeDelim(' ');
    char numDelim(':');
    std::string nodeGroup;
    const char *nodeValue = NULL;
    while ((pos1 = tempNodes.find(nodeDelim)) != string::npos) {
      std::string nodeGroup = tempNodes.substr(0, pos1).c_str();
      pos2 = nodeGroup.find(numDelim);
      NAString nodeName = nodeGroup.substr(0, pos2).c_str();
      nodeNameList.insert(nodeName);
      tempNodes.erase(0, pos1 + 1);
    }
    nodeGroup = tempNodes.substr(0, tempNodes.length()).c_str();
    pos2 = nodeGroup.find(numDelim);
    NAString nodeName = nodeGroup.substr(0, pos2).c_str();
    nodeNameList.insert(nodeName);
  }

  for (CollIndex i = 0; i < nodesInRGroup->entries(); i++) {
    TenantResourceUsage *usage = (*nodesInRGroup)[i];
    NAString nodeName = usage->getUsageName();
    if (usage->getUsageType() == TenantResourceUsage::RESOURCE_USAGE_NODE)
      nodeName = usage->getUsageName();
    else
      nodeName = usage->getResourceName();
    int nodeID = -1;

    // check temp node list for valid names
    if (useTempNodes) {
      for (CollIndex j = 0; j < nodeNameList.entries(); j++) {
        if (nodeName == nodeNameList[j]) {
          nodeID = j;
          break;
        }
      }

      if (nodeID == -1) {
        *CmpCommon::diags() << DgSqlCode(-CAT_NODE_NAME_INVALID) << DgString0(nodeName.data());
        return -1;
      }
    }

    // check cluster for valid nodes names
    else {
      nodeID = CURRCONTEXT_CLUSTERINFO->mapNodeNameToNodeNum(nodeName);
      if (nodeID == IPC_CPU_DONT_CARE) {
        *CmpCommon::diags() << DgSqlCode(-CAT_NODE_NAME_INVALID) << DgString0(nodeName.data());
        return -1;
      }
    }

    if (!nodesToAdd->contains(nodeName)) {
      TenantNodeInfo *newNodeInfo =
          new (STMTHEAP) TenantNodeInfo(nodeName, nodeID, -1, usage->getUsageUID(), usage->getUsageValue(), STMTHEAP);
      nodesToAdd->orderedInsert(newNodeInfo);
    }
  }
  return 0;
}

// -----------------------------------------------------------------------------
// method: getTenantResource
//
// Returns the tenant resource by reading RESOURCES for the resource name and
// the list of resource usages.  A resource can be a resource group or a node.
// -----------------------------------------------------------------------------
short CmpSeabaseDDLtenant::getTenantResource(const NAString &resourceName, const bool isNodeResource,
                                             const bool &fetchUsages, TenantResource &tenantResource) {
  TenantResource::TenantResourceType resourceType =
      (isNodeResource ? TenantResource::RESOURCE_TYPE_NODE : TenantResource::RESOURCE_TYPE_GROUP);
  NAString resourceClause("WHERE resource_name = '");
  resourceClause += resourceName + NAString("'");
  NAString resourceUsageClause = resourceClause;
  resourceUsageClause += " AND usage_type = '";
  resourceUsageClause += tenantResource.getResourceTypeAsString(resourceType) + NAString("'");
  return tenantResource.fetchMetadata(resourceClause, resourceUsageClause, fetchUsages);
}

// -----------------------------------------------------------------------------
// method: createSchemas
//
// Creates schemas that will become part of the tenant.
//
// throws UserException if unsuccessful (ComDiags area is setup)
// -----------------------------------------------------------------------------
void CmpSeabaseDDLtenant::createSchemas(ExeCliInterface *cliInterface, const StmtDDLTenant *pNode,
                                        const int &tenantID, const TenantUsageList *origUsageList,
                                        NAString schemaOwner, long &defSchUID, TenantUsageList *&usageList) {
  defSchUID = NA_UserIdDefault;

  bool hasSchemaUsages = (origUsageList && origUsageList->hasSchemaUsages());

  if (pNode->getSchemaList() && pNode->getSchemaList()->entries() > 0) {
    // Create schemas if they don't already exist
    for (int i = 0; i < pNode->getSchemaList()->entries(); i++) {
      SchemaName *schemaName = (*pNode->getSchemaList())[i];

      // If schema already part of tenant, just continue
      // requester may want to change schema attributes (e.g. default schema)
      if (hasSchemaUsages) {
        // Get the UID for the schema and see if it exists in the usage list
        long schUID = getSchemaUID(cliInterface, schemaName);
        if (schUID > 0) {
          TenantUsage tenantUsage;
          if (origUsageList->findTenantUsage(schUID, tenantUsage)) {
            if (pNode->getAlterType() == StmtDDLTenant::NOT_ALTER)
              tenantUsage.setIsNew(true);
            else
              tenantUsage.setUnchanged(true);
            usageList->insert(tenantUsage);
            continue;
          } else {
            *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_NOT_PART_OF_TENANT)
                                << DgString0(schemaName->getSchemaNameAsAnsiString().data())
                                << DgString1(pNode->getTenantName());
            UserException excp(NULL, 0);
            throw excp;
          }
        }
        // else - schema does not exist, go create it
      }

      // create schema
      char buf[200 + MAX_DBUSERNAME_LEN * 2];
      snprintf(buf, sizeof(buf),
               "create private schema %s authorization \"%s\" "
               " namespace '%d'",
               schemaName->getSchemaNameAsAnsiString().data(), schemaOwner.data(), tenantID);

      NAString createStmt(buf);
      int cliRC = cliInterface->executeImmediate(createStmt);
      if (cliRC < 0) {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        UserException excp(NULL, 0);
        throw excp;
      }

      // Grant DML privileges to current user if authorization is role
      if (pNode->getRoleName() && !ComUser::isRootUserID()) {
        NAString grantStmt("grant all on schema ");
        grantStmt += schemaName->getSchemaNameAsAnsiString();
        grantStmt += " to ";
        if (PrivMgr::isDelimited(ComUser::getCurrentUsername())) {
          grantStmt += '"';
          grantStmt += ComUser::getCurrentUsername();
          grantStmt += '"';
        } else
          grantStmt += ComUser::getCurrentUsername();

        grantStmt += " with grant option by ";
        if (PrivMgr::isDelimited(pNode->getRoleName()->data())) {
          grantStmt += '"';
          grantStmt += pNode->getRoleName()->data();
          grantStmt += '"';
        } else
          grantStmt += pNode->getRoleName()->data();
        cliRC = cliInterface->executeImmediate(grantStmt);
        if (cliRC < 0) {
          cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
          UserException excp(NULL, 0);
          throw excp;
        }
      }

      long schUID = getSchemaUID(cliInterface, schemaName);
      if (schUID <= 0) {
        *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR)
                            << DgString0(schemaName->getCatalogName().data())
                            << DgString1(schemaName->getSchemaName().data());
        UserException excp(NULL, 0);
        throw excp;
      }

      TenantUsage tenantUsage(tenantID, schUID, TenantUsage::TENANT_USAGE_SCHEMA);
      usageList->insert(tenantUsage);
      const SchemaName *defSchemaName = pNode->getDefaultSchema();
      if (defSchemaName && defSchemaName->getSchemaNameAsAnsiString() == schemaName->getSchemaNameAsAnsiString())
        defSchUID = schUID;
    }
  }

  // Add existing schema UIDs to list
  if (hasSchemaUsages && pNode->getAlterType() == StmtDDLTenant::NOT_ALTER) {
    for (int i = 0; i < origUsageList->entries(); i++) {
      TenantUsage tenantUsage = (*origUsageList)[i];
      TenantUsage existingUsage;
      if (!usageList->findTenantUsage(tenantUsage.getUsageID(), existingUsage)) {
        tenantUsage.setIsNew(true);
        usageList->insert(tenantUsage);
      }
    }
  }
}

// -----------------------------------------------------------------------------
// method: dropSchemas
//
// Drop one or more schemas from the tenant
//
// throws UserException if unsuccessful (ComDiags area is setup)
// -----------------------------------------------------------------------------
void CmpSeabaseDDLtenant::dropSchemas(ExeCliInterface *cliInterface, const int tenantID,
                                      const NAList<NAString> *schemaList) {
  // drop (cleanup) schemas
  for (int i = 0; i < schemaList->entries(); i++) {
    // NAString dropStmt("CLEANUP SCHEMA ");
    NAString dropStmt("DROP SCHEMA ");
    dropStmt += (*schemaList)[i];
    dropStmt += (" CASCADE");
    int cliRC = cliInterface->executeImmediate(dropStmt);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      UserException excp(NULL, 0);
      throw excp;
    }
  }
}

NAString CmpSeabaseDDLtenant::getNodeName(const long nodeID) {
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  TenantResourceUsageList usageList(&cliInterface, STMTHEAP);
  NAString whereClause("where usage_uid = ");
  whereClause += (to_string((long long int)nodeID).c_str());
  NAString orderByClause("order by usage_name limit 1");
  int retcode = usageList.fetchUsages(whereClause, orderByClause);
  if (retcode != 0 || usageList.entries() == 0) return NAString("?");

  TenantResourceUsage *usage = usageList.operator[](0);
  return usage->getUsageName();
}

NAString CmpSeabaseDDLtenant::getRGroupName(const long groupUID) {
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  NAString whereClause("WHERE resource_uid = ");
  whereClause += (to_string((long long int)groupUID).c_str());
  TenantResource group(&cliInterface, STMTHEAP);
  short retcode = group.fetchMetadata(whereClause, whereClause, true);
  if (retcode != 0) return NAString("?");

  return group.getResourceName();
}

// ----------------------------------------------------------------------------
// method:  getSchemaName
//
// This method returns the schema name given the schema UID
//
// Input:  Schema UID
//
// Output:  catalog and schema name
//
// If schema name is not found, then "?" is returned
// ----------------------------------------------------------------------------
NAString CmpSeabaseDDLtenant::getSchemaName(const long schUID) {
  // Get schema details
  NAString catName;
  NAString schName;
  NAString objName;

  NAString fullSchName;
  ExeCliInterface cliInterface(STMTHEAP);
  CmpSeabaseDDL cmpSBD(STMTHEAP);
  int32_t diagsMark = CmpCommon::diags()->mark();

  if (cmpSBD.getObjectName(&cliInterface, schUID, catName, schName, objName) == 0) {
    ComObjectName extName((const NAString &)catName, (const NAString &)schName, (const NAString &)objName,
                          COM_UNKNOWN_NAME, ComAnsiNamePart::INTERNAL_FORMAT);
    fullSchName = extName.getCatalogNamePartAsAnsiString();
    fullSchName += ".";
    fullSchName += extName.getSchemaNamePartAsAnsiString();
  } else
    fullSchName = "?";

  // clear any errors
  CmpCommon::diags()->rewind(diagsMark);
  return fullSchName;
}

// -----------------------------------------------------------------------------
// method: getSchemaUID
//
// Reads the OBJECTS table for the specified schema name and returns the
// associated schema UID.
//
// returns:
//     -1 - unexpected error occurred reading the metadata
//      0 - schema not found
//     >0 - schemaUID
// -----------------------------------------------------------------------------
long CmpSeabaseDDLauth::getSchemaUID(ExeCliInterface *cliInterface, const SchemaName *schemaName) {
  int selectStmtSize = 600;
  char selectStmt[selectStmtSize];
  int stmtSize = snprintf(selectStmt, sizeof(selectStmt),
                            "select object_uid from %s.\"%s\".%s  "
                            "where catalog_name = '%s' and schema_name = '%s' "
                            "and object_name = '%s'",
                            CmpSeabaseDDL::getSystemCatalogStatic().data(), SEABASE_MD_SCHEMA, SEABASE_OBJECTS,
                            schemaName->getCatalogNameAsAnsiString().data(), schemaName->getSchemaName().data(),
                            SEABASE_SCHEMA_OBJECTNAME);

  if (stmtSize >= selectStmtSize) {
    SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLauth::getSchemaUID failed, internal buffer size too small");
    return -1;
  }

  int diagsMark = CmpCommon::diags()->mark();
  Queue *tableQueue = NULL;
  int cliRC = cliInterface->fetchAllRows(tableQueue, selectStmt, 0, false, false, true);

  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }
  if (cliRC == 100)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    return 0;
  }

  tableQueue->position();
  long schUID = -1;
  OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();

  // column 1:  usage_uid
  schUID = (*(long *)pCliRow->get(0));

  return schUID;
}

// -----------------------------------------------------------------------------
// method: getTenantSchemas
//
// Get the list of schema UIDs that belong to the tenant
//
// returns:
//   -1 = error (ComDiags contains error)
//    0 = successful, schemaUIDList contains value, schemaUIDList can be empty
// -----------------------------------------------------------------------------
int CmpSeabaseDDLtenant::getTenantSchemas(ExeCliInterface *cliInterface, NAList<long> *&schemaUIDList) {
  assert(schemaUIDList);
  NAString MDLoc;
  CONCAT_CATSCH(MDLoc, CmpSeabaseDDL::getSystemCatalogStatic(), SEABASE_TENANT_SCHEMA);
  MDLoc += NAString(".") + SEABASE_TENANT_USAGE;

  NAString selectStmt("select distinct usage_uid from ");
  selectStmt += MDLoc;
  selectStmt += " where usage_type = 'S' order by 1";

  int32_t diagsMark = CmpCommon::diags()->mark();
  Queue *tableQueue = NULL;
  int cliRC = cliInterface->fetchAllRows(tableQueue, selectStmt, 0, false, false, true);

  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  // If no rows, just return
  if (cliRC == 100 || tableQueue->numEntries() == 0)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    return 0;
  }

  tableQueue->position();
  for (int i = 0; i < tableQueue->numEntries(); i++) {
    OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();

    // column 1:  usage_uid
    long schemaUID(*(long *)pCliRow->get(0));

    // add to list
    schemaUIDList->insert(schemaUID);
  }
  return 0;
}

// ----------------------------------------------------------------------------
// Public method: alterTenant
//
// changes attributes for a tenant
//
// Input:  parse tree containing the tenant changes
// Output: the global diags area is set up with the result
// ----------------------------------------------------------------------------
void CmpSeabaseDDLtenant::alterTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode) {
  int retcode;

  // Need multi-tenancy license
  if (!msg_license_multitenancy_enabled()) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Resource group feature is not enabled.");
    return;
  }

  if (!verifyAuthority(SQLOperation::MANAGE_TENANTS)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return;
  }

  // Verify that the specified name is not reserved
  if (isAuthNameReserved(pNode->getTenantName())) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTH_NAME_RESERVED) << DgString0(pNode->getTenantName().data());
    return;
  }

  // read tenant details from the AUTHS table
  const NAString dbTenantName(pNode->getTenantName());
  TenantInfo tenantInfo(STMTHEAP);
  CmpSeabaseDDLauth::AuthStatus authStatus = getTenantDetails(dbTenantName, tenantInfo);
  if (authStatus == STATUS_ERROR) return;
  if (authStatus == STATUS_NOTFOUND) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(dbTenantName.data())
                        << DgString1("tenant");
    return;
  }

  // Keeps track of changes to tenant usages
  TenantUsageList *usageList = NULL;

  // Variables required for zookeeper/cgroup updates
  NAWNodeSet *origTenantNodeSet = NULL;
  NAWNodeSet *tenantNodeSet = NULL;
  TenantNodeInfoList *nodeList = NULL;
  TenantNodeInfoList *origNodeList = NULL;
  TenantResourceUsageList *resourceUsageList = NULL;
  int tenantSize = -1;
  NAString origDefaultSchema = tenantInfo.getDefaultSchemaName();

  switch (pNode->getAlterType()) {
    case StmtDDLTenant::ALTER_TENANT_OPTIONS: {
      // At alter time, the parser sets tenant option value (affinity,
      // cluster size, etc) to -1 if user did not request a change.
      bool deleteUsages = false;
      bool insertUsages = false;
      bool modifyUsages = false;

      // ======================================================================
      // alter tenant change default schema
      // change default schema
      if (pNode->getDefaultSchema() && pNode->isDefaultSchemaOpt() && pNode->getSchemaList() == NULL) {
        long defSchUID = 0;
        const SchemaName *defaultSchema = pNode->getDefaultSchema();
        int32_t schOwnerID;
        ComObjectType schType;
        if (getSchemaDetails(cliInterface, defaultSchema, defSchUID, schType, schOwnerID) != STATUS_GOOD) return;
        if (defSchUID > 0) {
          // Make sure tenant's admin role has at least one privilege on the
          // default schema - Mantis 5975
          if (schOwnerID != tenantInfo.getAdminRoleID()) {
            std::vector<int64_t> schemaList;
            NAString privMgrMDLoc;
            CONCAT_CATSCH(privMgrMDLoc, systemCatalog_.data(), SEABASE_PRIVMGR_SCHEMA);
            PrivMgrSchemaPrivileges schemaPrivs(std::string(privMgrMDLoc.data()), CmpCommon::diags());
            schemaPrivs.getSchemasForRole(tenantInfo.getAdminRoleID(), schemaList);
            if (std::find(schemaList.begin(), schemaList.end(), defSchUID) == schemaList.end()) {
              char roleName[MAX_DBUSERNAME_LEN + 1];
              int32_t length;

              // just display what getAuthNameFromAuthID returns
              retcode = ComUser::getAuthNameFromAuthID(tenantInfo.getAdminRoleID(), roleName, MAX_DBUSERNAME_LEN,
                                                       length, NULL /* don't update diags*/);

              *CmpCommon::diags() << DgSqlCode(-CAT_TENANT_DEFAULT_SCHEMA_INVALID)
                                  << DgString0(defaultSchema->getSchemaName().data()) << DgString1(dbTenantName.data())
                                  << DgString2(roleName);

              return;
            }
          }

          tenantInfo.setDefaultSchemaUID(defSchUID);
        }
      }

      // at this time, we cannot alter the system tenant
      // for any of the remaining options
      else if (dbTenantName == DB__SYSTEMTENANT) {
        *CmpCommon::diags() << DgSqlCode(-CAT_RESERVED_TENANT) << DgString0(dbTenantName.data());
        return;
      }

      // ======================================================================
      // create admin role
      if (pNode->getRoleName()) {
        CmpSeabaseDDLrole adminRole;
        int adminRoleID = adminRole.createAdminRole(*pNode->getRoleName(), pNode->getTenantName(),
                                                      ComUser::getCurrentUser(), ComUser::getCurrentUsername());
        assert(adminRoleID > 0);
        tenantInfo.setAdminRoleID(adminRoleID);
      }

      // ======================================================================
      // add schemas
      if (pNode->getSchemaList() && pNode->addSchemaList()) {
        assert(pNode->getSchemaList()->entries() > 0);
        if (usageList == NULL) usageList = new (STMTHEAP) TenantUsageList(STMTHEAP);
        const LIST(SchemaName *) *schemaList = pNode->getSchemaList();
        long defSchUID = NA_UserIdDefault;

        char roleName[MAX_DBUSERNAME_LEN + 1];
        int32_t length;

        // If adminRoleID not found in metadata, ComDiags is populated with
        // error 8732: Authorization ID <adminRoleID> is not a registered user or role
        retcode = ComUser::getAuthNameFromAuthID(tenantInfo.getAdminRoleID(), roleName, MAX_DBUSERNAME_LEN, length,
                                                 FALSE, CmpCommon::diags());
        if (retcode != 0) return;

        try {
          createSchemas(cliInterface, pNode, tenantInfo.getTenantID(), tenantInfo.getUsageList(), NAString(roleName),
                        defSchUID, usageList);

          // If default schema not requested and only 1 schema for tenant including
          // existing schemas (tenantInfo.usageList_) and new schemas (usageList),
          // make it the default schema
          int totalSchemas = tenantInfo.getNumberSchemaUsages();
          totalSchemas += usageList->getNumberUsages(TenantUsage::TENANT_USAGE_SCHEMA, true);
          if (defSchUID == 0 && totalSchemas == 1) {
            if (pNode->getSchemaList()->entries() == 1)
              defSchUID = usageList->possibleDefSchUID();
            else if (tenantInfo.getUsageList()->entries() == 1)
              defSchUID = tenantInfo.getUsageList()->possibleDefSchUID();
          }

          if (defSchUID != NA_UserIdDefault) tenantInfo.setDefaultSchemaUID(defSchUID);

          // If just changing the default schema then no need to insert usages
          if (usageList->getNewSchemaUsages() > 0) insertUsages = true;
        }

        catch (...) {
          // At this time, an error should be in the diags area.
          // If there is no error, set up an internal error
          if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
            SEABASEDDL_INTERNAL_ERROR("AlterTenant: catch block for create schemas");
          NADELETE(usageList, TenantUsageList, STMTHEAP);
          // TODO: Restore old state in zookeeper, if needed
          return;
        }
      }

      // ======================================================================
      // drop schemas
      if (pNode->getSchemaList() && !pNode->addSchemaList()) {
        long schUID = 0;
        assert(pNode->getSchemaList() && pNode->getSchemaList()->entries() > 0);
        NAList<NAString> *schemaList = NULL;
        try {
          if (usageList == NULL) usageList = new (STMTHEAP) TenantUsageList(STMTHEAP);
          schemaList = new (STMTHEAP) NAList<NAString>(STMTHEAP);
          for (int i = 0; i < pNode->getSchemaList()->entries(); i++) {
            // Verify schema is part of tenant
            SchemaName *schemaName = (*pNode->getSchemaList())[i];
            schUID = getSchemaUID(cliInterface, schemaName);
            if (schUID <= 0) {
              *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR)
                                  << DgString0(schemaName->getCatalogName().data())
                                  << DgString1(schemaName->getSchemaName().data());
              UserException excp(NULL, 0);
              throw excp;
            }

            TenantUsage usage;
            if (!tenantInfo.findTenantUsage(schUID, usage)) {
              // better error msg?
              *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR)
                                  << DgString0(schemaName->getCatalogName().data())
                                  << DgString1(schemaName->getSchemaName().data());
              UserException excp(NULL, 0);
              throw excp;
            }

            // update lists
            schemaList->insert(schemaName->getSchemaNameAsAnsiString());
            TenantUsage tenantUsage(tenantInfo.getTenantID(), schUID, TenantUsage::TENANT_USAGE_SCHEMA);
            usageList->insert(tenantUsage);
          }

          // drop schemas
          dropSchemas(cliInterface, tenantInfo.getTenantID(), schemaList);
          tenantInfo.setDefaultSchemaUID(getDefSchemaUID(tenantInfo, usageList));
          deleteUsages = true;
        }

        catch (...) {
          if (usageList) NADELETE(usageList, TenantUsageList, STMTHEAP);

          if (schemaList) NADELETE(schemaList, NAList<NAString>, STMTHEAP);

          // At this time, an error should be in the diags area.
          // If there is no error, set up an internal error
          if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
            SEABASEDDL_INTERNAL_ERROR("AlterTenant: catch block for drop schemas");
          // TODO: restore old state in zookeeper, if needed
          return;
        }
      }

      // ======================================================================
      // alter tenant details related to node allocation, cgroups and zookeeper
      //   - adjust node-tenant unit allocation for RG tenants
      //   - adjust affinity, cluster size, and node assignments for AS tenants
      //   - adjust tenant size for both AS and RG tenants
      //   - adjust session limit and default schema for both AS and RG tenants
      if (pNode->isZookeeperUpdate()) {
        bool AStenant = tenantInfo.getTenantNodes()->castToNAASNodes();

        // If this is an AStenant but specified a resource group list, return error
        if (AStenant && (pNode->addRGroupList() || pNode->dropRGroupList() || pNode->replaceRGroupList())) {
          *CmpCommon::diags() << DgSqlCode(-CAT_MISMATCH_AS_RG_TENANTS) << DgString0(dbTenantName.data())
                              << DgString1("adaptive segmentation") << DgString2("resource groups");
          return;
        }

        // If this is an RG tenant but specified AS attributes, return error
        if (pNode->isAffinitySizing() && !AStenant) {
          *CmpCommon::diags() << DgSqlCode(-CAT_MISMATCH_AS_RG_TENANTS) << DgString0(dbTenantName.data())
                              << DgString1("resource groups") << DgString2("adaptive segmentation");
          return;
        }

        int origClusterSize = tenantInfo.getClusterSize();
        int clusterSize = -1;
        int origAffinity = tenantInfo.getAffinity();
        int affinity = -1;
        int origTenantSize = tenantInfo.getTenantNodes()->getTotalWeight();

        // If call to balanceTenantNodeAlloc completes and an error occurs,
        // redo the registration process
        bool registrationComplete = false;

        try {
          // allocate lists (if not already allocated)
          //   origNodeList - nodes available before changes are processed
          //   nodeList - nodes available after changes are processed
          //   usageList - changed tenant usages (tenant-rgroups) for TENANT_USAGE
          //   resourceUsageList - changed node-tenant usages for RESOURCE_USAGE
          origNodeList = new (STMTHEAP) TenantNodeInfoList(STMTHEAP);
          nodeList = new (STMTHEAP) TenantNodeInfoList(STMTHEAP);
          if (!usageList) usageList = new (STMTHEAP) TenantUsageList(STMTHEAP);
          resourceUsageList = new (STMTHEAP) TenantResourceUsageList(cliInterface, STMTHEAP);

          // Both AStenant and RGtenant requires tenant size
          tenantSize = (pNode->isTenantSizeSpecified()) ? pNode->getTenantSize() : origTenantSize;

          if (AStenant) {
            // Get updated values for affinity and cluster size
            affinity = (pNode->getAffinity() >= 0) ? pNode->getAffinity() : origAffinity;

            clusterSize = (pNode->getClusterSize() >= 0) ? pNode->getClusterSize() : origClusterSize;

            // Create NAASNodes, one with original values, one with new values
            tenantInfo.allocTenantNode(AStenant, origAffinity, origClusterSize, origTenantSize, origNodeList,
                                       origTenantNodeSet, false);
            tenantInfo.setOrigTenantNodes(origTenantNodeSet);

            // allocate the NAASNodes based on updated attributes
            tenantInfo.allocTenantNode(AStenant, affinity, clusterSize, tenantSize, nodeList, tenantNodeSet);
            tenantInfo.setTenantNodes(tenantNodeSet);

            // Update metadata with new affinity, cluster size, and tenant size
            modifyUsages = TRUE;
          }

          else  // RGtenant
          {
            // Get list of nodes for tenant before update
            if (tenantInfo.getNodeList(resourceUsageList) == -1) {
              UserException excp(NULL, 0);
              throw excp;
            }

            if (getNodeDetails(resourceUsageList, origNodeList) == -1) {
              UserException excp(NULL, 0);
              throw excp;
            }

            // Create NAWNodeSet (origTenantNodeSet) from the original tenant
            // assignments. The NAWNodeSet should be complete
            tenantInfo.allocTenantNode(AStenant, affinity, clusterSize, origTenantSize, origNodeList, origTenantNodeSet,
                                       true);
            tenantInfo.setOrigTenantNodes(origTenantNodeSet);

            // Reset allocated units; for replace, obsolete each entry
            if (pNode->doNodeBalance()) resourceUsageList->resetValue(pNode->replaceRGroupList());

            // add resource groups
            if (pNode->addRGroupList()) {
              addResources(pNode, cliInterface, tenantInfo, nodeList, usageList, resourceUsageList);
              insertUsages = true;
            }

            // drop resource groups
            else if (pNode->dropRGroupList()) {
              dropResources(pNode, cliInterface, tenantInfo, nodeList, usageList, resourceUsageList);
              deleteUsages = true;
            }

            // replace resource groups
            else if (pNode->replaceRGroupList()) {
              // Parser should catch this case
              if (pNode->getRGroupList() == NULL || pNode->getRGroupList()->entries() == 0)
                SEABASEDDL_INTERNAL_ERROR("AlterTenant: no resource groups specified");

              // Move current tenant-resource group usages to usageList and obsolete,
              // this simulates the drop of existing resource groups from the tenant
              for (int i = 0; i < tenantInfo.getUsageList()->entries(); i++) {
                TenantUsage usage = (*tenantInfo.getUsageList())[i];
                if (usage.getUsageType() == TenantUsage::TENANT_USAGE_RESOURCE) {
                  TenantUsage newUsage = usage;
                  newUsage.setIsObsolete(true);
                  usageList->insert(newUsage);
                }
              }

              addResources(pNode, cliInterface, tenantInfo, nodeList, usageList, resourceUsageList);
              modifyUsages = true;
            }

            // For rebalancing,  may have a different list of nodes if nodes
            // were added to one of the tenants resource groups.
            else if (pNode->doNodeBalance()) {
              alterResources(cliInterface, tenantInfo, nodeList, resourceUsageList);
              modifyUsages = true;
            }

            // Changing the session limit or default schema -  don't rebalance
            // but still need the list of nodes to allocate the NAWSetOfNodeIds
            else {
              if (getNodeDetails(resourceUsageList, nodeList) == -1) {
                UserException excp(NULL, 0);
                throw excp;
              }
              modifyUsages = true;
            }

            // allocate the NAWSetOfNodeIds based on updated resource group mapping
            tenantInfo.allocTenantNode(AStenant, affinity, clusterSize, tenantSize, nodeList, tenantNodeSet);
            tenantInfo.setTenantNodes(tenantNodeSet);
          }

          // Update zookeeper, cgroups, and determine node allocations
          NAString nodeAllocationList = CmpCommon::getDefaultString(NODE_ALLOCATIONS).data();
          if (CURRCONTEXT_CLUSTERINFO->hasVirtualNodes() && nodeAllocationList.length() > 0) {
            if (!AStenant) {
              if (nodeAllocationList.length() > 0) {
                int unitsToAllocate = tenantSize;
                tenantSize = predefinedNodeAllocations(unitsToAllocate, resourceUsageList);
              }
            }
          }

          else {
            // adjust node allocations, etc
            tenantInfo.balanceTenantNodeAlloc(tenantInfo.getDefaultSchemaName(), pNode->getSessionLimit(), false,
                                              origTenantNodeSet, tenantNodeSet, resourceUsageList,
                                              registrationComplete);
          }

          // update node-tenant usages into RESOURCE_USAGE
          if (!AStenant) {
            // log list of nodes used
            NAString nodeMsg = nodeList->listOfNodes();
            nodeMsg.prepend("Alter tenant updated set of nodes: ");
            QRLogger::log(CAT_SQL_EXE, LL_INFO, nodeMsg.data());

            if (resourceUsageList->modifyUsages() == -1) {
              UserException excp(NULL, 0);
              throw excp;
            }
          }
        }

        catch (...) {
          // if registrationComplete is true, reset node allocation
          // back to the original values
          if (registrationComplete) {
            try {
              // reset tenantNodes based on origTenantNodeset
              tenantInfo.balanceTenantNodeAlloc(origDefaultSchema, -2, /* use existing session limit */
                                                true,                  /*backing out a previous balance op */
                                                tenantNodeSet, origTenantNodeSet, resourceUsageList,
                                                registrationComplete);
            } catch (...) {
              NAString msg("AlterTenant: unable to reset tenant allocations for tenant ");
              msg += dbTenantName;
              SEABASEDDL_INTERNAL_ERROR(msg.data());
            }
          }

          if (usageList) NADELETE(usageList, TenantUsageList, STMTHEAP);

          if (nodeList) NADELETE(nodeList, TenantNodeInfoList, STMTHEAP);

          if (origNodeList) NADELETE(origNodeList, TenantNodeInfoList, STMTHEAP);

          if (resourceUsageList) NADELETE(resourceUsageList, TenantResourceUsageList, STMTHEAP);

          // At this time, an error should be in the diags area.
          // If there is no error, set up an internal error
          if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
            SEABASEDDL_INTERNAL_ERROR("AlterTenant: catch block processing NAWNodeSet");

          return;
        }

        if (nodeList) NADELETE(nodeList, TenantNodeInfoList, STMTHEAP);

        if (origNodeList) NADELETE(origNodeList, TenantNodeInfoList, STMTHEAP);

        if (resourceUsageList) NADELETE(resourceUsageList, TenantResourceUsageList, STMTHEAP);
      }

      // ======================================================================
      // add and drop user groups
      if (pNode->getGroupList()) {
        if (usageList == NULL) usageList = new (STMTHEAP) TenantUsageList(STMTHEAP);
        try {
          if (pNode->addGroupList()) {
            addGroups(pNode, getAuthID(), tenantInfo.getUsageList(), usageList);
            insertUsages = true;
          } else {
            dropGroups(pNode, tenantInfo, usageList);
            deleteUsages = true;
          }
        } catch (...) {
          NADELETE(usageList, TenantUsageList, STMTHEAP);
          return;
        }
      }

      // ======================================================================
      // always update TENANTS to change the redef time
      if (insertUsages)
        tenantInfo.updateTenantInfo(true, usageList, tenantSize);
      else if (deleteUsages)
        tenantInfo.dropTenantInfo(true, usageList);
      else if (modifyUsages)
        tenantInfo.modifyTenantInfo(true, usageList);
      else
        tenantInfo.updateTenantInfo(true, NULL, tenantSize);

      break;
    }

    case StmtDDLTenant::NOT_ALTER:
    default:
      SEABASEDDL_INTERNAL_ERROR("AlterTenant: default case for alter operation");
      break;
  }
  authStatus = updateRedefTime();

  if (usageList) NADELETE(usageList, TenantUsageList, STMTHEAP);
}

// -----------------------------------------------------------------------------
// Method:  getNodesForRGroups
//
// Reads RESOURCE_USAGE to get the nodes assigned to the tenant from the list
// of valid resource groups.
// -----------------------------------------------------------------------------
int CmpSeabaseDDLtenant::getNodesForRGroups(const TenantUsageList *usageList, TenantResourceUsageList *&nodeList) {
  assert(nodeList);
  char buf[200];
  NAString whereClause;

  // get nodes based on resource group-node usage
  // for all valid resource groups assigned to the tenant
  whereClause += "where resource_uid in (";
  bool addComma = false;
  for (CollIndex i = 0; i < usageList->entries(); i++) {
    TenantUsage usage = (*usageList)[i];
    if (!usage.isObsolete() && usage.isRGroupUsage()) {
      snprintf(buf, sizeof(buf), "%s %ld", (addComma ? ", " : " "), usage.getUsageID());
      addComma = true;
      whereClause += buf;
    }
  }
  whereClause += ") and usage_type = 'N'";

  NAString orderByClause("order by usage_name");
  if (nodeList->fetchUsages(whereClause, orderByClause) == -1) return -1;
  return 0;
}

// ----------------------------------------------------------------------------
// method:  getDefSchemaUID
//
// ----------------------------------------------------------------------------
long CmpSeabaseDDLtenant::getDefSchemaUID(TenantInfo &tenantInfo, const TenantUsageList *dropUsageList) {
  // Determine default schema
  long defSchUID = tenantInfo.getDefaultSchemaUID();

  // See if current default schema is in drop list
  TenantUsage droppedUsage(tenantInfo.getTenantID(), defSchUID, TenantUsage::TENANT_USAGE_SCHEMA);
  if (defSchUID > 0 && !dropUsageList->contains(droppedUsage)) return defSchUID;

  // If there in only 1 schema left, make that the default
  if (tenantInfo.getNumberSchemaUsages() == dropUsageList->entries() + 1) {
    // Find the schema in the existingUsageList that is not in the
    // dropUsageList, make that the default schema
    const TenantUsageList *existingUsageList = tenantInfo.getUsageList();
    int i = 0;
    NABoolean schemaInDropList = TRUE;
    while (schemaInDropList && i < existingUsageList->entries()) {
      TenantUsage existingUsage = (*existingUsageList)[i];
      if (dropUsageList->contains(existingUsage)) {
        i++;
        continue;
      }
      schemaInDropList = FALSE;
      defSchUID = existingUsage.getUsageID();
    }
  } else
    defSchUID = 0;
  return defSchUID;
}

// ----------------------------------------------------------------------------
// method: registerStandardTenant
//
// Creates a standard tenant in the AUTHS metadata
//
// Input:
//    tenantName
//    tenantID
//
// returns:
//   -1 - failed, ComDiags set up with error
//    0 - succeeded
// ----------------------------------------------------------------------------
int CmpSeabaseDDLtenant::registerStandardTenant(const std::string tenantName, const int32_t tenantID) {
  setAuthType(COM_TENANT_CLASS);  // we are a tenant
  if (!createStandardAuth(tenantName, tenantID)) return -1;

  // Insert tenant information into metadata
  // If different values for affinity, node size, and cluster size are
  // required, then change this line
  int adminRole = (CmpCommon::context()->isAuthorizationEnabled()) ? ROOT_ROLE_ID : NA_UserIdDefault;

  TenantInfo tenantInfo(tenantID, adminRole, 0, NULL, new (STMTHEAP) NAASNodes(-1, -1, -1), getAuthDbName(), STMTHEAP);
  return tenantInfo.addTenantInfo();
}

// ----------------------------------------------------------------------------
// Public method: registerTenantPrepare
//
// prepares for registering a tenant in the Trafodion metadata
//
// register tenant runs in multiple transactions,
//   see CmpSeabaseDDL::registerSeabaseTenant for details
//
// Input:  parse tree containing a definition of the tenant
// Output:
//   class is setup with AUTHS details
//   adminRoleID (gets sent to registerTenant)
//      0 - role does not exist
//     >0 - role to use
//
// returns:  results of operation
//    0 = successful
//   -1 = failed (ComDiags contains error)
// ----------------------------------------------------------------------------
int CmpSeabaseDDLtenant::registerTenantPrepare(StmtDDLTenant *pNode, int &adminRoleID) {
  // if group list specified and advanced feature bit not set, return error
  bool enabled = (msg_license_advanced_enabled() || msg_license_multitenancy_enabled());
  if (pNode->getGroupList() && !enabled) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("User group feature is not enabled.");
    return -1;
  }

  // Verify user is authorized to perform REGISTER TENANT requests
  if (!verifyAuthority(SQLOperation::MANAGE_TENANTS)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return -1;
  }

  // Verify that the specified name is not reserved
  setAuthDbName(pNode->getTenantName());
  if (isAuthNameReserved(pNode->getTenantName())) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTH_NAME_RESERVED) << DgString0(pNode->getTenantName().data());
    return -1;
  }
  setAuthExtName(pNode->getTenantName());

  // Verify that the name does not include unsupported special characters
  std::string validChars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
  std::string strToScan = getAuthExtName().data();
  size_t found = strToScan.find_first_not_of(validChars);
  if (!found == string::npos) {
    *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_CHARS_IN_AUTH_NAME) << DgString0(pNode->getTenantName().data());
    return -1;
  }

  // Make sure tenant has not already been registered
  if (authExists(getAuthDbName(), false)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTHID_ALREADY_EXISTS) << DgString0("Authorization identifier")
                        << DgString1(getAuthDbName().data());
    return -1;
  }

  // Make sure an admin role name was specified
  if (pNode->getRoleName() == NULL) {
    *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_ADMIN_ROLE) << DgString0(getAuthDbName().data());
    return -1;
  }

  // Make sure admin role is not PUBLIC
  if (strcmp(pNode->getRoleName()->data(), ComUser::getPublicUserName()) == 0) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ROLE_UNAVAILABLE) << DgString0(pNode->getRoleName()->data())
                        << DgString1(getAuthDbName().data());
    UserException excp(NULL, 0);
    throw excp;
  }

  // If admin role exists, see if should use detached tenant
  adminRoleID = 0;
  int tenantID = 0;
  if (findDetachedTenant(pNode, adminRoleID, tenantID) == -1) return -1;

  // tenantID is zero, then no detached tenant exists, go generate an ID
  if (tenantID == 0)
    if (generateTenantID(tenantID) == -1) return -1;
  assert(tenantID > 0);

  // set up AUTHS class members from parse node
  setAuthID(tenantID);
  setAuthType(COM_TENANT_CLASS);  // we are a tenant
  setAuthValid(true);             // assume a valid tenant
  setAuthIsAdmin(false);
  setAuthCreator(ComUser::getCurrentUser());

  long createTime = NA_JulianTimestamp();
  setAuthCreateTime(createTime);
  setAuthRedefTime(createTime);  // make redef time the same as create time

  return 0;
}

// ----------------------------------------------------------------------------
// Public method: registerTenant
//
// registers a tenant in the Trafodion metadata and zookeeper
//
// Input:
//   parse tree containing a definition of the tenant
//   adminRoleID - role to use for the tenant
// Output: result of operation
//
// returns:  results of operation
//    0 = successful
//   -1 = failed (ComDiags contains error)
// ----------------------------------------------------------------------------
int CmpSeabaseDDLtenant::registerTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode, int adminRoleID) {
  long defSchUID = 0;
  TenantUsageList *usageList = new (STMTHEAP) TenantUsageList(STMTHEAP);
  TenantNodeInfoList *nodeList = NULL;
  TenantResourceUsageList *resourceUsageList = NULL;
  TenantUsageList *existingUsageList = NULL;
  NAString tenantDefaultSchema;
  int tenantSize = -1;
  int clusterSize = 0;
  NAWNodeSet *tenantNodeSet = NULL;
  bool registrationComplete = false;
  int retcode = 0;

  // Set up a global try/catch loop to cleanup after unexpected errors
  try {
    // Add the tenant to AUTHS table, insertRow throws exception
    insertRow();

    // ======================================================================
    // Process admin role:

    // If not using existing roleID, create role
    if (adminRoleID == 0) {
      CmpSeabaseDDLrole roleInfo;
      adminRoleID = roleInfo.createAdminRole(*pNode->getRoleName(), pNode->getTenantName(), ComUser::getCurrentUser(),
                                             ComUser::getCurrentUsername());
      if (adminRoleID <= 0) {
        *CmpCommon::diags() << DgSqlCode(-CAT_INTERNAL_EXCEPTION_ERROR) << DgString0(__FILE__) << DgInt0(__LINE__)
                            << DgString1("Unable to create admin role");
        UserException excp(NULL, 0);
        throw excp;
      }
    } else {
      if (isDetached()) {
        // admin role exists, get list of schemas it owns
        existingUsageList = new (STMTHEAP) TenantUsageList(STMTHEAP);
        if (generateSchemaUsages(getAuthID(), adminRoleID, existingUsageList) == -1) {
          UserException excp(NULL, 0);
          throw excp;
        }

        // Every role has a grant from system (_SYSTEM) to the role creator.
        // It may have a grant from system (_SYSTEM) to a detached role.
        // Remove any role to detached tenant that may exist.
        PrivMgrRoles role;
        PrivStatus privStatus = role.revokeRoleFromCreator(adminRoleID, getAuthID(), false /*remove tenantID only*/);
        if (privStatus == PrivStatus::STATUS_ERROR) {
          UserException excp(NULL, 0);
          throw excp;
        }

        setIsDetached(false);
      }
    }

    // ======================================================================
    // Process schemas:
    //   if schemas specified, createSchemas
    if (pNode->getSchemaList() || existingUsageList != NULL) {
      createSchemas(cliInterface, pNode, getAuthID(), existingUsageList, (NAString)*pNode->getRoleName(), defSchUID,
                    usageList);
    }

    if (existingUsageList) NADELETE(existingUsageList, TenantUsageList, STMTHEAP);
    existingUsageList = NULL;

    // ======================================================================
    // Process default schema:
    //   If default schema not specified only 1 schema for tenant, set
    //   it to the default schema
    if (!pNode->getDefaultSchema() && defSchUID == 0) defSchUID = usageList->possibleDefSchUID();

    // Default schema specified
    if (pNode->getDefaultSchema()) {
      // get schema UID
      const SchemaName *defaultSchema = pNode->getDefaultSchema();
      long schUID = getSchemaUID(cliInterface, defaultSchema);
      if (schUID <= 0) {
        *CmpCommon::diags() << DgSqlCode(-CAT_SCHEMA_DOES_NOT_EXIST_ERROR)
                            << DgString0(defaultSchema->getCatalogName().data())
                            << DgString1(defaultSchema->getSchemaName().data());
        UserException excp(NULL, 0);
        throw excp;
      }
      defSchUID = schUID;
      tenantDefaultSchema = CmpSeabaseDDLtenant::getSchemaName(defSchUID);
    }

    if (defSchUID > 0) tenantDefaultSchema = CmpSeabaseDDLtenant::getSchemaName(defSchUID);

    // ======================================================================
    // Process user groups:
    //   Add group details to class
    if (pNode->getGroupList()) {
      addGroups(pNode, getAuthID(), NULL, usageList);
    }

    // ======================================================================
    // For AS tenants - set up affinity, cluster size and tenant size
    // For RG tenants - generate tenant-rgroup and node-tenant usages, and
    //   set tenant size
    bool AStenant = pNode->isAffinitySizing();

    // At register tenant time, the parser defaults tenant size and cluster
    // size to 0 and affinity to -1. If the user did not specify them. Pick
    // reasonable default values here.
    tenantSize =
        (pNode->getTenantSize() <= 0 ? CmpCommon::getDefaultLong(TENANT_DEFAULT_SIZE) : pNode->getTenantSize());

    clusterSize =
        (pNode->getClusterSize() == 0) ? CURRCONTEXT_CLUSTERINFO->getTotalNumberOfCPUs() : pNode->getClusterSize();

    // allocate lists (if not already allocated)
    //   nodeList - nodes available based on specified resource groups
    //   usageList - additional tenant usages (tenant-rgroups) for TENANT_USAGE
    //   resourceUsageList - node-tenant usages for RESOURCE_USAGE
    nodeList = new (STMTHEAP) TenantNodeInfoList(STMTHEAP);
    resourceUsageList = new (STMTHEAP) TenantResourceUsageList(cliInterface, STMTHEAP);

    TenantInfo tenantInfo(getAuthID(), adminRoleID, defSchUID, usageList, tenantNodeSet, pNode->getTenantName(),
                          STMTHEAP);

    // updates nodeList, usageList, and resourceUsageList
    if (!AStenant) addResources(pNode, cliInterface, tenantInfo, nodeList, usageList, resourceUsageList);

    // creates the tenantNodeSet
    tenantInfo.allocTenantNode(AStenant, pNode->getAffinity(), clusterSize, tenantSize, nodeList, tenantNodeSet);
    tenantInfo.setTenantNodes(tenantNodeSet);

    NAString nodeAllocationList = CmpCommon::getDefaultString(NODE_ALLOCATIONS).data();
    if (CURRCONTEXT_CLUSTERINFO->hasVirtualNodes() && nodeAllocationList.length() > 0) {
      // For testing, we skip the call to balance node allocations and set up
      // zookeeper and cgroups to test DDL only.
      if (!AStenant) {
        int unitsToAllocate = tenantSize;
        tenantSize = predefinedNodeAllocations(unitsToAllocate, resourceUsageList);
      }
    } else {
      // adds tenant to zookeeper and cgroups.  Also returns node allocations
      // based on the tenantNodeSet
      tenantInfo.balanceTenantNodeAlloc(tenantDefaultSchema, pNode->getSessionLimit(), false, NULL, tenantNodeSet,
                                        resourceUsageList, registrationComplete);
      tenantInfo.setTenantNodes(tenantNodeSet);
    }

    // ======================================================================
    // update metadata
    if (!AStenant) {
      if (resourceUsageList->modifyUsages() == -1) {
        UserException excp(NULL, 0);
        throw excp;
      }
    }

    if (tenantInfo.addTenantInfo(tenantSize) == -1) {
      UserException excp(NULL, 0);
      throw excp;
    }
  }

  catch (...) {
    if (registrationComplete) {
      NAString nodeAllocationList = CmpCommon::getDefaultString(NODE_ALLOCATIONS).data();
      if (!CURRCONTEXT_CLUSTERINFO->hasVirtualNodes() || nodeAllocationList.length() == 0)
        TenantHelper_JNI::getInstance()->unregisterTenant(pNode->getTenantName());
    }

    if (nodeList) NADELETE(nodeList, TenantNodeInfoList, STMTHEAP);

    if (resourceUsageList) NADELETE(resourceUsageList, TenantResourceUsageList, STMTHEAP);

    if (existingUsageList) NADELETE(existingUsageList, TenantUsageList, STMTHEAP);

    // if (usageList)
    //  NADELETE (usageList, TenantUsageList, STMTHEAP);

    // At this time, an error should be in the diags area.
    // If there is no error, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("catch block in CmpSeabaseDDLtenant::registerTenant");

    return -1;
  }

  if (nodeList) NADELETE(nodeList, TenantNodeInfoList, STMTHEAP);

  if (resourceUsageList) NADELETE(resourceUsageList, TenantResourceUsageList, STMTHEAP);

  // if (usageList)
  //  NADELETE (usageList, TenantUsageList, STMTHEAP);

  return 0;
}

// ----------------------------------------------------------------------------
// public method:  unregisterTenant
//
// This method removes a tenant from the database
//
// Input:  parse tree containing a definition of the tenant
// Output: the global diags area is set up with the result
// ----------------------------------------------------------------------------
int CmpSeabaseDDLtenant::unregisterTenant(ExeCliInterface *cliInterface, StmtDDLTenant *pNode) {
  int cliRC;
  const NAString dbTenantName(pNode->getTenantName());
  TenantInfo tenantInfo(STMTHEAP);
  CmpSeabaseDDLauth::AuthStatus retcode = getTenantDetails(dbTenantName, tenantInfo);
  if (retcode == STATUS_ERROR) return -1;
  if (retcode == STATUS_NOTFOUND) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(dbTenantName.data())
                        << DgString1("tenant");
    return -1;
  }

  // TDB - revoke tenant roles from tenant users

  // drop users and roles where admin role is the creator
#if 0
  if (pNode->dropDependencies())
  {
    std::vector<std::string> userNames;
    std::vector<std::string> roleNames;
 
    getAuthsForCreator(tenantInfo.getAdminRoleID(), userNames, roleNames);
    for (size_t i = 0; i < userNames.size(); i++)
    {
      std::string authName = userNames[i];
      NAString dropStmt("UNREGISTER USER ");
      dropStmt += authName.c_str();
      cliRC = cliInterface->executeImmediate(dropStmt);
      if (cliRC < 0)
      {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }
    }

    for (size_t i = 0; i < roleNames.size(); i++)
    {
      std::string authName = roleNames[i];
      dropStmt += authName.c_str();
      cliRC = cliInterface->executeImmediate(dropStmt);
      if (cliRC < 0)
      {
        cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
        return -1;
      }
    }
  }
#endif

  char roleName[MAX_DBUSERNAME_LEN + 1];
  if (tenantInfo.getAdminRoleID() > NA_UserIdDefault) {
    int32_t length;

    // If adminRoleID not found in metadata, ComDiags is populated with
    // error 8732: Authorization ID <adminRoleID> is not a registered user or role
    cliRC = ComUser::getAuthNameFromAuthID(tenantInfo.getAdminRoleID(), roleName, MAX_DBUSERNAME_LEN, length, FALSE,
                                           CmpCommon::diags());
  }

  if (cliRC != 0) return -1;

  if (pNode->dropDependencies()) {
    // admin role, drop it
    CmpSeabaseDDLrole role;
    if (role.dropAdminRole(NAString(roleName), tenantInfo.getTenantID()) == -1) return -1;
  }

  else {
    // Since admin role may be used again, create a role usage between
    // the admin role and tenant.  This can be used to later reattach the role
    // and all objects owned by the role to a tenant.
    PrivMgrRoles roles;
    PrivStatus privStatus =
        roles.grantRoleToCreator(tenantInfo.getAdminRoleID(), std::string(roleName), SYSTEM_USER, SYSTEM_AUTH_NAME,
                                 tenantInfo.getTenantID(), std::string(tenantInfo.getTenantName()));
    if (privStatus != PrivStatus::STATUS_GOOD) return -1;
  }

  // Remove node/tenant usages
  TenantResourceUsageList usageList(cliInterface, STMTHEAP);
  NAString whereClause("where usage_uid = ");
  whereClause += (to_string((long long int)tenantInfo.getTenantID())).c_str();
  if (usageList.deleteUsages(whereClause) == -1) return -1;

  // drop tenant details
  if (tenantInfo.dropTenantInfo() != 0) return -1;

  try {
    // delete the row from AUTHS table
    deleteRow(getAuthDbName());
  }

  catch (...) {
    // At this time, an error should be in the diags area.
    // If there is no error, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("catch block in CmpSeabaseDDLuser::unregisterTenant");
  }

  return 0;
}

// ----------------------------------------------------------------------------
// Public method: unregisterTenantPrepare
//
// prepares for unregistering a tenant in the Trafodion metadata
//
// Input:  parse tree containing a definition of the tenant
//
// Unregister tenant runs in multiple transactions,
//   see CmpSeabaseDDL::unregisterSeabaseTenant for details
//
// returns:  results of operation
//    0 = successful
//   -1 = failed (ComDiags contains error)
// ----------------------------------------------------------------------------
int CmpSeabaseDDLtenant::unregisterTenantPrepare(StmtDDLTenant *pNode) {
  if (!verifyAuthority(SQLOperation::MANAGE_TENANTS)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return -1;
  }

  // verify that not dropping a system tenant
  bool isSpecialAuth = false;
  if (isSystemAuth(COM_TENANT_CLASS, pNode->getTenantName(), isSpecialAuth)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTH_NAME_RESERVED) << DgString0(pNode->getTenantName().data());
    return -1;
  }

  return 0;
}

// -----------------------------------------------------------------------------
// public method:  describe
//
// This method returns the showddl text for the requested tenant in string format
//
// Input:
//   tenantName - name of tenant to describe
//
// Input/Output:
//   tenantText - the REGISTER TENANT text
//              tenantText include the tenant creator as a comment
//
// returns result:
//    true -  successful
//    false - failed (ComDiags area will be set up with appropriate error)
//-----------------------------------------------------------------------------
bool CmpSeabaseDDLtenant::describe(const NAString &tenantName, NAString &tenantText) {
  NAString currentTenant(ComTenant::getCurrentTenantName());
  if (tenantName != currentTenant) {
    bool hasAdminRole = false;
    if (msg_license_multitenancy_enabled()) {
      int adminRole = NA_UserIdDefault;
      AuthStatus authStatus = getAdminRole(ComUser::getCurrentUser(), adminRole);
      if (authStatus == STATUS_ERROR) return false;
      hasAdminRole = (authStatus == STATUS_GOOD);
    }

    if (!verifyAuthority(SQLOperation::SHOW) && !hasAdminRole) {
      PrivMgrComponentPrivileges componentPrivileges;
      if (!componentPrivileges.hasSQLPriv(ComUser::getCurrentUser(), SQLOperation::MANAGE_TENANTS, true)) {
        // No authority.  We're outta here.
        *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
        return false;
      }
    }
  }

  try {
    TenantInfo tenantInfo(STMTHEAP);
    CmpSeabaseDDLauth::AuthStatus retcode = getTenantDetails(tenantName.data(), tenantInfo);

    // If the user was not found, set up an error
    if (retcode == STATUS_NOTFOUND) {
      *CmpCommon::diags() << DgSqlCode(-CAT_AUTHID_DOES_NOT_EXIST_ERROR) << DgString0("Tenant")
                          << DgString1(tenantName.data());
      return false;
    }

    // If an error was detected, throw an exception so the catch handler will
    // put a value in ComDiags area in case no message exists
    if (retcode == STATUS_ERROR) {
      UserException excp(NULL, 0);
      throw excp;
    }

    if (!isTenant()) {
      *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(tenantName.data())
                          << DgString1("tenant");
      UserException excp(NULL, 0);
      throw excp;
    }

    // Get name of tenant creator
    char nameStr[MAX_DBUSERNAME_LEN + 1];
    int32_t length;

    // If can't get name, just print what is returned
    ComUser::getAuthNameFromAuthID(getAuthCreator(), nameStr, MAX_DBUSERNAME_LEN, length, TRUE,
                                   NULL /*don't update diags*/);

    // Generate output text
    std::string tenantName(getAuthExtName());
    tenantText = "CREATE TENANT ";
    if (PrivMgr::isDelimited(tenantName)) tenantText += '"';
    tenantText += getAuthExtName();
    if (PrivMgr::isDelimited(tenantName)) tenantText += '"';

    char intStr[100];
    tenantText += " TENANT SIZE ";
    tenantText += str_itoa(tenantInfo.getTenantSize(), intStr);
    if (tenantInfo.getAffinity() >= 0) {
      snprintf(intStr, sizeof(intStr), ", AFFINITY %d, CLUSTER SIZE %d", tenantInfo.getAffinity(),
               tenantInfo.getClusterSize());
      tenantText += intStr;
    }

    // Display default tenant
    if (tenantInfo.getDefaultSchemaUID() > 0) {
      tenantText += ", DEFAULT SCHEMA ";
      if (PrivMgr::isDelimited(nameStr)) {
        tenantText += '"';
        tenantText += getSchemaName(tenantInfo.getDefaultSchemaUID());
        tenantText += '"';
      } else
        tenantText += getSchemaName(tenantInfo.getDefaultSchemaUID());
    }

    // Display admin role
    if (tenantInfo.getAdminRoleID() > NA_UserIdDefault) {
      tenantText += ", ADMIN ROLE ";
      // If can't get name, just print what is returned
      ComUser::getAuthNameFromAuthID(tenantInfo.getAdminRoleID(), nameStr, MAX_DBUSERNAME_LEN, length, TRUE,
                                     NULL /*don't update diags*/);
      if (PrivMgr::isDelimited(nameStr)) {
        tenantText += '"';
        tenantText += nameStr;
        tenantText += '"';
      } else
        tenantText += nameStr;
    }

    // Display usages
    NAString groupText;
    NAString schemaText;
    NAString rgroupText;
    NAString rgroupAllocationText;
    ExeCliInterface cliInterface(STMTHEAP);
    if (tenantInfo.getUsageList()) {
      for (int i = 0; i < tenantInfo.getUsageList()->entries(); i++) {
        TenantUsage usage = (*tenantInfo.getUsageList())[i];
        if (usage.isGroupUsage()) {
          if (groupText.isNull())
            groupText += ", GROUPS (";
          else
            groupText += ", ";
          CmpSeabaseDDLauth group;
          group.getAuthDetails(usage.getUsageID());
          std::string groupName(group.getAuthDbName().data());
          if (PrivMgr::isDelimited(groupName)) {
            groupText += '"';
            groupText += group.getAuthDbName();
            groupText += '"';
          } else
            groupText += group.getAuthDbName();
        }

        if (usage.isSchemaUsage()) {
          if (schemaText.isNull())
            schemaText += ", SCHEMAS (";
          else
            schemaText += ", ";
          schemaText += getSchemaName(usage.getUsageID());
        }

        if (usage.isRGroupUsage()) {
          if (rgroupText.isNull())
            rgroupText += ", RESOURCE GROUPS (";
          else
            rgroupText += ", ";
          rgroupText += getRGroupName(usage.getUsageID());
        }
      }

      if (groupText.length() > 0) tenantText += groupText + NAString(")");

      if (schemaText.length() > 0) tenantText += schemaText + NAString(")");

      if (rgroupText.length() > 0) tenantText += rgroupText + NAString(")");
    }
    tenantText += ";\n";

    // Display comment text
    if (rgroupText.length() > 0) {
      TenantResourceUsageList usageList(&cliInterface, STMTHEAP);
      NAString whereClause("where usage_uid = ");
      whereClause += (to_string((long long int)getAuthID()).c_str());
      NAString orderByClause("order by resource_name");

      if (usageList.fetchUsages(whereClause, orderByClause) == 0) {
        if (usageList.entries() > 0) tenantText += "-- Tenant node assignments: ";
        for (int i = 0; i < usageList.entries(); i++) {
          TenantResourceUsage *usage = (usageList)[i];

          // skip nodes with unallocated units
          if (usage->getUsageValue() <= 0) continue;

          if (i > 0)
            snprintf(nameStr, sizeof(nameStr), ", node '%s' (%ld) units", usage->getResourceName().data(),
                     usage->getUsageValue());
          else
            snprintf(nameStr, sizeof(nameStr), " node '%s' (%ld) units", usage->getResourceName().data(),
                     usage->getUsageValue());
          tenantText += nameStr;
        }
      }
    }
  }

  catch (...) {
    // At this time, an error should be in the diags area.
    // If there is no error, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("Describe method in CmpSeabaseDDLtenant::describe");
    return false;
  }

  return true;
}

int CmpSeabaseDDLtenant::predefinedNodeAllocations(const int unitsToAllocate,
                                                     TenantResourceUsageList *&resourceUsageList) {
  // we have the list of nodes, the available units, use round robin
  // to allocate them
  int unitsAllocated = 0;
  div_t unitValues;
  int numNodes = 0;
  int validUsage = 0;
  for (int i = 0; i < resourceUsageList->entries(); i++) {
    if ((*resourceUsageList)[i]->isObsolete() == false) {
      numNodes++;
      validUsage = i;
    }
  }
  unitValues = div(unitsToAllocate, numNodes);
  for (int r = 0; r < resourceUsageList->entries(); r++) {
    TenantResourceUsage *usage = (*resourceUsageList)[r];
    if (!usage->isObsolete()) {
      usage->setUsageValue(unitValues.quot);
      unitsAllocated += unitValues.quot;
    }
  }

  // add the remainder to the last valid usage
  TenantResourceUsage *usage = (*resourceUsageList)[validUsage];
  if (usage->getUsageValue() == -1)
    usage->setUsageValue(unitValues.rem);
  else
    usage->setUsageValue(usage->getUsageValue() + unitValues.rem);

  // If node has no units, set to 0
  for (int r = 0; r < resourceUsageList->entries(); r++) {
    TenantResourceUsage *usage = (*resourceUsageList)[r];
    if (usage->getUsageValue() == -1) usage->setUsageValue(0);
  }

  unitsAllocated += unitValues.rem;

  return unitsAllocated;
}

// ****************************************************************************
// Class CmpSeabaseDDLgroup methods
// ****************************************************************************

// ----------------------------------------------------------------------------
// Constructor
// ----------------------------------------------------------------------------
CmpSeabaseDDLgroup::CmpSeabaseDDLgroup(const NAString &systemCatalog, const NAString &MDSchema)
    : CmpSeabaseDDLauth(systemCatalog, MDSchema) {}

CmpSeabaseDDLgroup::CmpSeabaseDDLgroup() : CmpSeabaseDDLauth() {}

// ----------------------------------------------------------------------------
// public method:  alterGroup
//
// This method changes a user group definition
//
// Input: parse tree containing a definition of the group change
//
// Output: the global diags area is set up with the result
// ----------------------------------------------------------------------------
void CmpSeabaseDDLgroup::alterGroup(StmtDDLUserGroup *pNode) {
  CHECK_LICENSE;

  int adminRoleID = NA_UserIdDefault;
  if (!verifyAuthority(SQLOperation::MANAGE_GROUPS)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return;
  }

  // read group details from the AUTHS table
  const NAString groupName(pNode->getGroupName());
  CmpSeabaseDDLauth::AuthStatus retcode = getGroupDetails(groupName);
  if (retcode == STATUS_ERROR) return;
  if (retcode == STATUS_NOTFOUND) {
    *CmpCommon::diags() << DgSqlCode(-CAT_GROUP_NOT_EXIST) << DgString0(groupName.data());
    return;
  }

  // Process the requested operation
  NAString setClause;
  long authFlags = 0;

  if (COM_LDAP_AUTH == currentAuthType_) {
    // Verify that the external group exists in configured identity store
    Int16 foundConfigurationNumber = DBUserAuth::DefaultConfiguration;
    const char *configSectionName = (pNode->getConfig().isNull() ? NULL : pNode->getConfig().data());

    if (!validateExternalGroupname(pNode->getGroupName().data(), configSectionName, foundConfigurationNumber)) return;

    // If configuration changed, authFlags
    if (foundConfigurationNumber != getAuthConfig()) {
      if (foundConfigurationNumber == DBUserAuth::DefaultConfiguration)
        setAuthConfig(DEFAULT_AUTH_CONFIG);
      else
        setAuthConfig((Int16)foundConfigurationNumber);
    }
  }

  switch (pNode->getUserGroupType()) {
    case StmtDDLUserGroup::ADD_USER_GROUP_MEMBER:
    case StmtDDLUserGroup::REMOVE_USER_GROUP_MEMBER: {
      if (COM_LDAP_AUTH == currentAuthType_) {
        *CmpCommon::diags() << DgSqlCode(-CAT_NOT_ALLOWED_OPERATION_ON_LDAP_AUTH);
        return;
      }
      StmtDDLUserGroup::UserGroupType optionType = pNode->getUserGroupType();
      // get members of usergroup
      std::set<int32_t> membersOfGroup;
      {
        std::vector<int32_t> memberList;
        CmpSeabaseDDLauth::AuthStatus status = getUserGroupMembers(memberList);
        if (CmpSeabaseDDLauth::STATUS_GOOD != status && CmpSeabaseDDLauth::STATUS_NOTFOUND != status) {
          // CmpSeabaseDDLauth::STATUS_NOTFOUND -> error CAT_USER_IS_NOT_MEMBER_OF_GROUP
          return;
        }
        // vector -> set
        if (!memberList.empty()) {
          membersOfGroup.insert(memberList.begin(), memberList.end());
        }
      }
      // get pair of authName and authID
      std::map<const char *, int32_t, Char_Compare> authNameAndID;
      bool isErrorOccurred = false;  // for free key of authNameAndID
      if (TRUE == getAuthNameAndAuthID(pNode->getMembers(), authNameAndID)) {
        // check for not exists username
        for (std::map<const char *, int32_t, Char_Compare>::iterator itor = authNameAndID.begin();
             itor != authNameAndID.end();) {
          const char *authName = itor->first;
          int32_t authID = itor->second;
          if (0 == authID) {
            *CmpCommon::diags() << DgSqlCode(-CAT_USER_NOT_EXIST) << DgString0(authName);
            isErrorOccurred = true;
            break;
          }
          if (!isUserID(authID)) {
            *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_A_USER) << DgString0(authName);
            isErrorOccurred = true;
            break;
          }
          if (membersOfGroup.count(authID) == 0) {
            if (optionType == StmtDDLUserGroup::REMOVE_USER_GROUP_MEMBER) {
              *CmpCommon::diags() << DgSqlCode(-CAT_USER_IS_NOT_MEMBER_OF_GROUP) << DgString0(authName)
                                  << DgString1(groupName.data());
              isErrorOccurred = true;
              break;
            }
          } else {
            // Check if user is already in target usergroup
            if (StmtDDLUserGroup::ADD_USER_GROUP_MEMBER == optionType) {
              // free key
              NADELETEBASICARRAY(authName, STMTHEAP);
              itor = authNameAndID.erase(itor);
              continue;
            }
          }
          itor++;
        }

        int (*func)(int, int) = (StmtDDLUserGroup::ADD_USER_GROUP_MEMBER == pNode->getUserGroupType())
                                          ? &CmpSeabaseDDLauth::addGroupMember
                                          : &CmpSeabaseDDLauth::removeGroupMember;
        std::vector<int32_t> memberIDs;
        bool isAllowedGrantRoleToGroup = false;
        IS_ALLOWED_GRANT_ROLE_TO_GROUP(isAllowedGrantRoleToGroup);
        for (std::map<const char *, int32_t, Char_Compare>::iterator itor = authNameAndID.begin();
             itor != authNameAndID.end(); itor++) {
          if (!isErrorOccurred) {
            func(getAuthID(), itor->second);
            if (isAllowedGrantRoleToGroup) {
              memberIDs.push_back(itor->second);
            }
          }
          NADELETEBASICARRAY(itor->first, STMTHEAP);
        }
        authNameAndID.clear();

        // refresh the query cache
        if (!isErrorOccurred && !memberIDs.empty()) {
          // don't use ContextCli::authQuery, which will be polluted the context cache
          std::vector<int32_t> groupRoleIDs;
          std::vector<int32_t> granteeIDs;  // i don't care
          // get all of role from group
          this->getRoleIDs(getAuthID(), groupRoleIDs, granteeIDs);
          if (!groupRoleIDs.empty()) {
            std::vector<int32_t> userRoleIDs;
            std::vector<int32_t> roleIDs;
            std::vector<int32_t> authIDs;
            // Invalidated cache one by one
            for (std::vector<int32_t>::iterator itor = memberIDs.begin(); itor != memberIDs.end(); itor++) {
              granteeIDs.clear();
              userRoleIDs.clear();
              authIDs.clear();
              authIDs.push_back(*itor);
              // need roles from group inheritance
              this->getRoleIDs(*itor, userRoleIDs, granteeIDs, true);
              if (!userRoleIDs.empty()) {
                roleIDs.clear();
                CmpCommon::getDifferenceWithTwoVector(groupRoleIDs, userRoleIDs, roleIDs);
                if (!roleIDs.empty()) {
                  CmpCommon::letsQuerycacheToInvalidated(authIDs, roleIDs);
                }
              } else {
                CmpCommon::letsQuerycacheToInvalidated(authIDs, groupRoleIDs);
              }
            }
          }
        }
      }
    } break;
    default: {
      // If configuration changed, authFlags
      try {
        // Performance improvement, if config section did not change, then
        // don't do the update
        Int16 config = getAuthConfig();
        config = config << 2;  // shift over bits 1 and 2
        authFlags = config;

        // Add remaining flags
        if (isAuthAdmin()) authFlags |= SEABASE_IS_ADMIN_ROLE;
        if (isAuthInTenant()) authFlags |= SEABASE_IS_TENANT_AUTH;
        if (isAutoCreated()) authFlags != SEABASE_IS_AUTO_CREATED;

        setClause += "SET ";
        setClause += " flags = ";
        setClause += (to_string((long long int)authFlags)).c_str();
        updateRow(setClause);
      } catch (...) {
        // At this time, an error should be in the diags area.
        // If there is no error, set up an internal error
        if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
          SEABASEDDL_INTERNAL_ERROR("Switch statement in CmpSeabaseDDLuser::alterUser");
      }
    } break;
  }
}

// ----------------------------------------------------------------------------
// Public method: registerGroup
//
// registers a user group in the Trafodion metadata
//
// Input: parse tree containing a definition of the user
//
// Output: the global diags area is set up with the result
// ----------------------------------------------------------------------------
void CmpSeabaseDDLgroup::registerGroup(StmtDDLUserGroup *pNode) {
  /*
 // Don't allow groups to be registered unless authorization is enabled
 if (!CmpCommon::context()->isAuthorizationEnabled())
 {
   *CmpCommon::diags() << DgSqlCode(-CAT_AUTHORIZATION_NOT_ENABLED);
   return;
 }
 removed for loacl authentication
 user can register/create group when authorization is disabled,
 but cannot execute 'grant' statements
 */

  CHECK_LICENSE;

  if (!verifyAuthority(SQLOperation::MANAGE_GROUPS)) {
    // No authority.  We're outta here.
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return;
  }

  // Make sure group has not already been registered
  if (authExists(pNode->getGroupName())) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTHID_ALREADY_EXISTS) << DgString0("Authorization identifier")
                        << DgString1(pNode->getGroupName().data());
    return;
  }

  if (registerGroupInternal(pNode->getGroupName(), pNode->getConfig(), false) == -1) {
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLgroup::registerGroup");
  }
}

// ----------------------------------------------------------------------------
// method: registerGroupInternal
//
// registers a tenant user group in the Trafodion metadata
//
// Input:  group to register, configuration section to use
// Output:  groupID for newly created group
//   groupID is -1 then error (the global diags area is set up with the result)
//
// ----------------------------------------------------------------------------
int CmpSeabaseDDLgroup::registerGroupInternal(const NAString &groupName, const NAString &config, bool isAutoCreated) {
  // Verify that the name does not include unsupported special characters
  if (!isAuthNameValid(groupName)) {
    *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_CHARS_IN_AUTH_NAME) << DgString0(groupName.data());
    return -1;
  }

  if (COM_LDAP_AUTH == currentAuthType_) {
    // Verify that the external group exists in configured identity store
    Int16 foundConfigurationNumber = DBUserAuth::DefaultConfiguration;
    const char *configSectionName = config.isNull() ? NULL : config.data();

    if (!validateExternalGroupname(groupName.data(), configSectionName, foundConfigurationNumber)) return -1;
    if (foundConfigurationNumber == DBUserAuth::DefaultConfiguration)
      setAuthConfig(DEFAULT_AUTH_CONFIG);
    else
      setAuthConfig((Int16)foundConfigurationNumber);
  } else {
    if (!config.isNull()) {
      *CmpCommon::diags() << DgSqlCode(CAT_IGNORE_SUFFIX_CONFIG_WHEN_LOCAL_AUTH);
    }
    setAuthConfig(DEFAULT_AUTH_CONFIG);
  }

  // set up class members
  setAuthDbName(groupName);
  setAuthExtName(groupName);
  setAuthType(COM_USER_GROUP_CLASS);  // we are a group
  setAuthValid(true);                 // assume a valid group

  long createTime = NA_JulianTimestamp();
  setAuthCreateTime(createTime);
  setAuthRedefTime(createTime);  // make redef time the same as create time

  int userID;
  try {
    // Get a unique auth ID number
    userID = getUniqueAuthID(ROOT_USERGROUP, MAX_USERGROUP);

    // There is a bug where group/tenant usages are not being removed
    // when the group is unregistered.  This bug has been fixed but there may
    // still be orphaned entries in the metadata. If this authID still shows
    // up as a tenant usage, cleanup the inconsistency.
    if (msg_license_multitenancy_enabled()) {
      TenantUsageList *tenantUsageList = new (STMTHEAP) TenantUsageList(STMTHEAP);

      NAString whereClause(" WHERE USAGE_TYPE = 'G' AND USAGE_UID = ");
      whereClause += PrivMgr::authIDToString(userID).c_str();
      int rc = tenantUsageList->deleteUsages(whereClause.data());
      NADELETE(tenantUsageList, TenantUsageList, STMTHEAP);
      if (rc == -1) {
        UserException excp(NULL, 0);
        throw excp;
      }
    }

    setAuthID(userID);
    setAuthCreator(ComUser::getCurrentUser());
    setAuthIsAdmin(false);
    if (msg_license_multitenancy_enabled() && isAutoCreated)
      setAuthInTenant(true);
    else
      setAuthInTenant(false);
    setAutoCreated(isAutoCreated);

    // Add the group to AUTHS table
    insertRow();
  }

  catch (...) {
    // If there is no error in the diags area, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLgroup::registerGroupInternal");
    return -1;
  }

  return userID;
}

// ----------------------------------------------------------------------------
// public method:  unregisterGroup
//
// This method removes a group from the database
//
// Input: parse tree containing a definition of the user
//
// Output: the global diags area is set up with the result
// ----------------------------------------------------------------------------
void CmpSeabaseDDLgroup::unregisterGroup(StmtDDLUserGroup *pNode) {
  int rc;

  CHECK_LICENSE;

  // CASCADE option not yet supported
  if (pNode->getDropBehavior() == COM_CASCADE_DROP_BEHAVIOR) {
    *CmpCommon::diags() << DgSqlCode(-CAT_ONLY_SUPPORTING_RESTRICT_DROP_BEHAVIOR);
    return;
  }

  if (!verifyAuthority(SQLOperation::MANAGE_GROUPS)) {
    // No authority.  We're outta here.
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return;
  }

  NAString groupName = pNode->getGroupName();
  CmpSeabaseDDLauth::AuthStatus retcode = getGroupDetails(groupName);
  if (retcode == STATUS_ERROR) return;
  if (retcode == STATUS_NOTFOUND) {
    *CmpCommon::diags() << DgSqlCode(-CAT_GROUP_NOT_EXIST) << DgString0(groupName.data());
    return;
  }

  // remove tenant/group relationship
  if (msg_license_multitenancy_enabled()) {
    TenantUsageList *tenantUsageList = new (STMTHEAP) TenantUsageList(STMTHEAP);

    NAString whereClause(" WHERE USAGE_TYPE = 'G' AND USAGE_UID = ");
    whereClause += (to_string((long long int)getAuthID())).c_str();
    rc = tenantUsageList->deleteUsages(whereClause.data());
    NADELETE(tenantUsageList, TenantUsageList, STMTHEAP);
    if (rc == -1) return;
  }

  NAString privMgrMDLoc;
  CONCAT_CATSCH(privMgrMDLoc, systemCatalog_.data(), SEABASE_PRIVMGR_SCHEMA);

  // See if group has been granted any roles
  if (CmpCommon::context()->isAuthorizationEnabled()) {
    PrivMgrRoles role(std::string(MDSchema_.data()), std::string(privMgrMDLoc.data()), CmpCommon::diags());
    std::vector<std::string> roleNames;
    std::vector<int32_t> roleDepths;
    std::vector<int32_t> roleIDs;
    std::vector<int32_t> grantees;
    if (role.fetchRolesForAuth(getAuthID(), roleNames, roleIDs, roleDepths, grantees) == PrivStatus::STATUS_ERROR)
      return;

    if (roleNames.size() > 0) {
      NAString roleList;
      int num = (roleNames.size() <= 5) ? roleNames.size() : 5;
      for (size_t i = 0; i < num; i++) {
        if (i > 0) roleList += ", ";
        roleList += roleNames[i].c_str();
      }
      if (roleNames.size() >= 5) roleList += "...";
      *CmpCommon::diags() << DgSqlCode(-CAT_NO_UNREG_USER_GRANTED_ROLES) << DgString0("group")
                          << DgString1(groupName.data()) << DgString2(roleList.data());
      return;
    }

    // See if group has been granted any privs
    PrivMgr privMgr(std::string(privMgrMDLoc), CmpCommon::diags());
    std::vector<PrivClass> privClasses;

    privClasses.push_back(PrivClass::ALL);

    std::vector<int64_t> objectUIDs;
    bool hasComponentPriv = false;
    if (privMgr.isAuthIDGrantedPrivs(getAuthID(), privClasses, objectUIDs, hasComponentPriv)) {
      NAString privGrantee;
      if (hasComponentPriv)
        privGrantee = "one or components";
      else
        // If the objectName is empty, then it is no longer defined,
        // continue.
        privGrantee = getObjectName(objectUIDs);

      if (privGrantee.length() > 0) {
        *CmpCommon::diags() << DgSqlCode(-CAT_NO_UNREG_USER_HAS_PRIVS) << DgString0("group")
                            << DgString1(groupName.data()) << DgString2(privGrantee.data());
        return;
      }
    }
    // See if group has been granted any component privileges
  }

  try {
    // delete the row
    deleteRow(getAuthDbName());
    // delete all member info for this group
    removeAllGroupMember();
  }

  catch (...) {
    // If there is no error in the diags area, set up an internal error
    if (CmpCommon::diags()->getNumber(DgSqlCode::ERROR_) == 0)
      SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLgroup::unregisterGroup");
  }
}
// -----------------------------------------------------------------------------
// public method:  describe
//
// This method returns the showddl text for the requested user group
//
// Input:
//   authName - name of user group to describe
//
// Input/Output:
//   authText - the REGISTER GROUP text
//
// returns result:
//    true -  successful
//    false - failed (ComDiags area will be set up with appropriate error)
//-----------------------------------------------------------------------------
bool CmpSeabaseDDLgroup::describe(const NAString &authName, NAString &authText) {
  if (!verifyAuthority(SQLOperation::SHOW)) {
    // No authority.  We're outta here.
    *CmpCommon::diags() << DgSqlCode(-CAT_NOT_AUTHORIZED);
    return false;
  }

  // If the group is not found, set up an error
  CmpSeabaseDDLauth::AuthStatus retcode = getAuthDetails(authName.data());
  if (retcode == STATUS_NOTFOUND) {
    *CmpCommon::diags() << DgSqlCode(-CAT_AUTHID_DOES_NOT_EXIST_ERROR) << DgString0("User group")
                        << DgString1(authName.data());
    return false;
  }
  if (retcode == STATUS_ERROR) return false;

  if (!isUserGroup()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(authName.data())
                        << DgString1("user group");
    return false;
  }

  // Generate output text
  std::string groupName(getAuthExtName().data());
  if (COM_LDAP_AUTH == currentAuthType_) {
    authText = "REGISTER GROUP ";
  } else {
    authText = "CREATE GROUP ";
  }

  if (PrivMgr::isDelimited(groupName)) authText += '"';
  authText += groupName.c_str();
  if (PrivMgr::isDelimited(groupName)) authText += '"';

  Int16 config = getAuthConfig();
  if (config != DEFAULT_AUTH_CONFIG) {
    DBUserAuth *userSession = DBUserAuth::GetInstance();
    std::string configSection = userSession->getConfigName(config);

    authText += " CONFIG '";
    authText += configSection.c_str();
    authText += "'";
  }
  authText += ";\n";

  return true;
}

// ----------------------------------------------------------------------------
// public method: getGroupDetails
//
// Create the CmpSeabaseDDLgroup class containing user details for the
// requested username
//
// Input:
//    groupName - the database user group
//
//  Output:
//    Returned parameter:
//       STATUS_GOOD: authorization details are populated:
//       STATUS_NOTFOUND: authorization details were not found
//       STATUS_WARNING: (not 100) warning was returned, diags area populated
//       STATUS_ERROR: error was returned, diags area populated
// ----------------------------------------------------------------------------
CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLgroup::getGroupDetails(const char *pGroupName) {
  if (!msg_license_advanced_enabled() && !msg_license_multitenancy_enabled())
    SEABASEDDL_INTERNAL_ERROR("CmpSeabaseDDLgroup::getGroupDetails, Invalid operation, group feature unavailable");

  CmpSeabaseDDLauth::AuthStatus retcode = getAuthDetails(pGroupName, false);
  if (retcode == STATUS_GOOD && !isUserGroup()) {
    *CmpCommon::diags() << DgSqlCode(-CAT_IS_NOT_CORRECT_AUTHID) << DgString0(getAuthDbName().data())
                        << DgString1("user group");
    return STATUS_ERROR;
  }
  return retcode;
}

void CmpSeabaseDDLgroup::removeAllGroupMember() {
  char str[1000] = {0};
  ExeCliInterface cliInterface(STMTHEAP);
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();

  str_sprintf(str, "delete from %s.\"%s\".%s where SUB_ID = %d and text_type = %d", sysCat.data(), SEABASE_MD_SCHEMA,
              SEABASE_TEXT, getAuthID(), COM_USER_GROUP_RELATIONSHIP);

  if (cliInterface.executeImmediate(str) < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }
}

void CmpSeabaseDDLuser::exitAllJoinedUserGroup() {
  char str[1000] = {0};
  ExeCliInterface cliInterface(STMTHEAP);
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();

  std::set<int32_t> roleIDs;
  std::vector<int32_t> groupIDs;
  bool isAllowedGrantRoleToGroup = false;
  IS_ALLOWED_GRANT_ROLE_TO_GROUP(isAllowedGrantRoleToGroup);
  if (isAllowedGrantRoleToGroup) {
    // get all groups
    if (CmpSeabaseDDLauth::STATUS_GOOD == this->getUserGroups(this->getAuthDbName().data(), groupIDs)) {
      for (int i = 0; i < groupIDs.size(); i++) {
        std::vector<int32_t> roleIDs_;    // there's duplicate value check on getRoleIDs, too slow
        std::vector<int32_t> granteeIDs;  // i don't care
        this->getRoleIDs(groupIDs[i], roleIDs_, granteeIDs);
        roleIDs.insert(roleIDs_.begin(), roleIDs_.end());
      }
    }
  }

  str_sprintf(str, "delete from %s.\"%s\".%s where TEXT_UID = %d and text_type = %d", sysCat.data(), SEABASE_MD_SCHEMA,
              SEABASE_TEXT, getAuthID(), COM_USER_GROUP_RELATIONSHIP);

  if (cliInterface.executeImmediate(str) < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }

  if (isAllowedGrantRoleToGroup && !roleIDs.empty()) {
    std::vector<int32_t> authIDs;
    std::vector<int32_t> roleIDs_(roleIDs.begin(), roleIDs.end());
    authIDs.push_back(getAuthID());
    CmpCommon::letsQuerycacheToInvalidated(authIDs, roleIDs_);
  }
}

int CmpSeabaseDDLauth::addGroupMember(int groupid, int userid) {
  char str[1000] = {0};
  if (!isUserGroupID(groupid)) {
    return -1;
  }
  if (!isUserID(userid)) {
    return -1;
  }
  ExeCliInterface cliInterface(STMTHEAP);
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();

  str_sprintf(str, "upsert into %s.\"%s\".%s values(%d,%d,%d,0,0,0)", sysCat.data(), SEABASE_MD_SCHEMA, SEABASE_TEXT,
              userid, COM_USER_GROUP_RELATIONSHIP, groupid);

  int cliRC = cliInterface.executeImmediate(str);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return cliRC;
}

int CmpSeabaseDDLauth::removeGroupMember(int groupid, int userid) {
  char str[1000] = {0};
  if (!isUserGroupID(groupid)) {
    return -1;
  }
  if (!isUserID(userid)) {
    return -1;
  }
  ExeCliInterface cliInterface(STMTHEAP);
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();

  str_sprintf(str, "delete from %s.\"%s\".%s where TEXT_UID = %d and SUB_ID = %d and text_type = %d", sysCat.data(),
              SEABASE_MD_SCHEMA, SEABASE_TEXT, userid, groupid, COM_USER_GROUP_RELATIONSHIP);

  int cliRC = cliInterface.executeImmediate(str);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    return -1;
  }

  return cliRC;
}

int CmpSeabaseDDLauth::getUserGroupMembers(const char *groupName, std::vector<int32_t> &memberIDs) {
  CmpSeabaseDDLauth authInfo;
  CmpSeabaseDDLauth::AuthStatus retcode = authInfo.getAuthDetails(NAString(groupName), false);
  if (STATUS_GOOD != retcode || !isUserGroupID(authInfo.getAuthID())) return -1;

  retcode = authInfo.getUserGroupMembers(memberIDs);
  if ((retcode != STATUS_GOOD) && (retcode != STATUS_NOTFOUND)) return -1;
  return 0;
}

CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLauth::getUserGroupMembers(std::vector<int32_t> &memberIDs) {
  switch (currentAuthType_) {
    case COM_LDAP_AUTH: {
      if (Get_SqlParser_Flags(DISABLE_EXTERNAL_AUTH_CHECKS)) {
        memberIDs.clear();
        return STATUS_GOOD;
      }
      // TODO using "memberof" instand of fetching LDAP attribute "member"
      DBUserAuth *ldapConn = DBUserAuth::GetInstance();
      Int16 configurationNumber = getAuthConfig();
      Int16 foundConfigurationNumber;
      std::vector<char *> members_;
      if (DBUserAuth::GroupListFound == ldapConn->getMembersOnLDAPGroup(NULL, configurationNumber, getAuthDbName(),
                                                                        foundConfigurationNumber, members_)) {
        if (foundConfigurationNumber != configurationNumber && foundConfigurationNumber != -2) {
          try {
            long authFlags = foundConfigurationNumber << 2;  // shift over bits 1 and 2

            // Add remaining flags
            if (isAuthAdmin()) authFlags |= SEABASE_IS_ADMIN_ROLE;
            if (isAuthInTenant()) authFlags |= SEABASE_IS_TENANT_AUTH;
            if (isAutoCreated()) authFlags != SEABASE_IS_AUTO_CREATED;

            NAString setClause;
            setClause += "SET ";
            setClause += " flags = ";
            setClause += (to_string((long long int)authFlags)).c_str();
            updateRow(setClause);
          } catch (...) {
            // don't warning
          }
        }
        std::map<const char *, int32_t, Char_Compare> authNameAndID;
        getAuthNameAndAuthID(members_, authNameAndID);
        for (std::vector<char *>::iterator itor = members_.begin(); itor != members_.end();
             itor = members_.erase(itor)) {
          // entries in members_ are created by 'new char[len]'
          // NADELETEBASICARRAY(*itor, STMTHEAP);
          delete[] * itor;
        }
        for (std::map<const char *, int32_t, Char_Compare>::iterator itor = authNameAndID.begin();
             itor != authNameAndID.end(); itor++) {
          int32_t authID = itor->second;
          NADELETEBASICARRAY(itor->first, STMTHEAP);
          if (0 == authID) {
            continue;
          }
          memberIDs.push_back(authID);
        }
      }
    } break;
    case COM_NOT_AUTH:  // is ok
    case COM_NO_AUTH:
    case COM_LOCAL_AUTH:
    default: {
      char buf[1000] = {0};
      ExeCliInterface cliInterface(STMTHEAP);
      NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
      str_sprintf(buf, "select distinct TEXT_UID from %s.\"%s\".%s where SUB_ID = %d and text_type = %d", sysCat.data(),
                  SEABASE_MD_SCHEMA, SEABASE_TEXT, this->getAuthID(), COM_USER_GROUP_RELATIONSHIP);
      Queue *cntQueue = NULL;
      int cliRC = cliInterface.fetchAllRows(cntQueue, buf, 0, false, false, true);
      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        UserException excp(NULL, 0);
        throw excp;
      } else if (cliRC == 100 || cntQueue->numEntries() == 0) {
        return STATUS_NOTFOUND;
      }
      cntQueue->position();
      for (int count = cntQueue->numEntries(); count; count--) {
        OutputInfo *pCliRow = (OutputInfo *)cntQueue->getNext();
        memberIDs.push_back(*((int *)pCliRow->get(0)));
      }
    } break;
  }
  return STATUS_GOOD;
}

// for local auth
NABoolean CmpSeabaseDDLauth::getAuthNameAndAuthID(ElemDDLList *nameList,
                                                  std::map<const char *, int32_t, Char_Compare> &result) {
  // AUTH_DB_NAME
  NAString whereClause = "where AUTH_DB_NAME in (";
  result.clear();
  for (CollIndex i = 0; i < nameList->entries(); i++) {
    // member & group use the same class to store data
    ElemDDLGroup *entry = ((*nameList)[i])->castToElemDDLGroup();
    NAString name = entry->getAuthorizationIdentifier();
    if (!isAuthNameValid(name)) {
      *CmpCommon::diags() << DgSqlCode(-CAT_INVALID_CHARS_IN_AUTH_NAME) << DgString0(name);
      goto ERROR;
    } else {
      whereClause += "'" + name + "',";
    }
    // copy key
    {
      char *tmp = new (STMTHEAP) char[strlen(name.data()) + 1];
      strcpy(tmp, name.data());
      result[(const char *)tmp] = 0;
    }
  }
  if (!result.empty()) {
    whereClause.replace(whereClause.length() - 1, 1, ")");
    selectAuthIDAndNames(whereClause, result);
  }
  return TRUE;
ERROR:
  result.clear();
  return FALSE;
}

// for ldap auth
NABoolean CmpSeabaseDDLauth::getAuthNameAndAuthID(std::vector<char *> &nameList,
                                                  std::map<const char *, int32_t, Char_Compare> &result) {
  // AUTH_EXT_NAME
  NAString whereClause = "where AUTH_EXT_NAME in (";
  bool hasVaildAuthName = false;
  result.clear();
  for (std::vector<char *>::iterator itor = nameList.begin(); itor != nameList.end(); itor++) {
    NAString authName(*itor);
    // member & group use the same class to store data
    if (!isAuthNameValid(authName)) {
      // don't waring
      continue;
    } else {
      authName.toUpper();
      whereClause += "'" + authName + "',";
      hasVaildAuthName = true;
    }
  }
  if (hasVaildAuthName) {
    whereClause.replace(whereClause.length() - 1, 1, ")");
    selectAuthIDAndNames(whereClause, result, false);
  }
  return TRUE;
}

CmpSeabaseDDLauth::AuthStatus CmpSeabaseDDLuser::getAllJoinedUserGroup(std::vector<int32_t> &groupIDs) {
  switch (currentAuthType_) {
    case COM_LDAP_AUTH: {
      // too slow
      this->getUserGroups(groupIDs);
    } break;
    case COM_NOT_AUTH:  // is ok
    case COM_NO_AUTH:
    case COM_LOCAL_AUTH:
    default: {
      char buf[1000] = {0};
      ExeCliInterface cliInterface(STMTHEAP);
      NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
      str_sprintf(buf, "select distinct SUB_ID from %s.\"%s\".%s where TEXT_UID= %d and text_type = %d", sysCat.data(),
                  SEABASE_MD_SCHEMA, SEABASE_TEXT, this->getAuthID(), COM_USER_GROUP_RELATIONSHIP);
      Queue *cntQueue = NULL;
      int cliRC = cliInterface.fetchAllRows(cntQueue, buf, 0, false, false, true);
      if (cliRC < 0) {
        cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
        UserException excp(NULL, 0);
        throw excp;
      } else if (cliRC == 100 || cntQueue->numEntries() == 0) {
        return STATUS_NOTFOUND;
      }
      cntQueue->position();
      for (int count = cntQueue->numEntries(); count; count--) {
        OutputInfo *pCliRow = (OutputInfo *)cntQueue->getNext();
        groupIDs.push_back(*((int *)pCliRow->get(0)));
      }
    } break;
  }
  return STATUS_GOOD;
}

int CmpSeabaseDDLauth::getGracePwdCnt(int userid, Int16 &num) {
  int val = getSeqnumFromText(userid);
  if (0 != val) {
    if (is_little_endian()) {
      num = *((Int16 *)&val);
    } else {
      // little -> big
      Int16 *ptr = (Int16 *)&val;
      Int8 *ptr1 = (Int8 *)(ptr + 1);
      num = (*ptr1 << 8) | (*(ptr1 + 1) >> 8);
    }
  } else {
    num = 0;
  }
  return 0;
}

int CmpSeabaseDDLauth::updateGracePwdCnt(int userid, Int16 num) {
  char buf[1000] = {0};
  int val, val2;
  bool isLittle = is_little_endian();
  val = getSeqnumFromText(userid);
  val2 = val;

  Int16 *ptr = isLittle ? ((Int16 *)&val) : ((Int16 *)&val) + 1;

  *ptr = num;

  if (val2 == val) {
    // not change
    return 0;
  }

  if (!isLittle) {
    val = swapByteOrderOfS((unsigned int)val);
  }

  ExeCliInterface cliInterface(STMTHEAP);

  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  str_sprintf(buf, "update %s.\"%s\".%s set SEQ_NUM = %d where text_uid = %d and text_type = %d", sysCat.data(),
              SEABASE_MD_SCHEMA, SEABASE_TEXT, val, userid, COM_USER_PASSWORD);
  int cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }

  return cliRC;
}

void CmpSeabaseDDLuser::reflashPasswordModTime() {
  // i don't want to update column AUTH_REDEF_TIME of table AUTHS when reflashing passwordModTime
  // in official usage,the passwordModTime is reflashed only by changing userpassword.
  // this function is only call by executing 'alter user xxx unlock' with DB__ROOT
  char buf[1000] = {0};
  ExeCliInterface cliInterface(STMTHEAP);
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  str_sprintf(buf, "update  %s.\"%s\".%s set flags = %ld where text_uid = %d and text_type = %d", sysCat.data(),
              SEABASE_MD_SCHEMA, SEABASE_TEXT, getPasswdModTime(), getAuthID(), COM_USER_PASSWORD);
  int cliRC = cliInterface.executeImmediate(buf);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }
}
void CmpSeabaseDDLauth::getAuthDbNameByAuthIDs(std::vector<int32_t> &idList, std::vector<std::string> &nameList) {
  if (idList.empty()) {
    return;
  }
  NAString whereClause = " where AUTH_ID in (";
  for (std::vector<int32_t>::iterator itor = idList.begin(); itor != idList.end(); itor++) {
    // for gcc 4.4.x
    long long int tmp = *itor;
    whereClause += NAString(std::to_string(tmp));
    whereClause += ",";
  }
  whereClause.replace(whereClause.length() - 1, 1, ')');
  selectAuthNames(whereClause, nameList, nameList, nameList);
}

NAString CmpSeabaseDDLuser::getDefAuthInfo(int uid) {
  char buf[1024] = {0};
  static unsigned char dePassword[2 * (MAX_PASSWORD_LENGTH + EACH_ENCRYPTION_LENGTH) + 1];
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  if (dePassword[0] == 0) {
    int cryptlen = 0;
    if (ComEncryption::encrypt_ConvertBase64_Data(ComEncryption::AES_128_CBC, (unsigned char *)(AUTH_DEFAULT_WORD),
                                                  strlen(AUTH_DEFAULT_WORD), (unsigned char *)AUTH_PASSWORD_KEY,
                                                  (unsigned char *)AUTH_PASSWORD_IV, (unsigned char *)dePassword,
                                                  cryptlen)) {
      goto END;
    }
  }
  str_sprintf(buf, "insert into %s.\"%s\".%s values (%d, %d, 0, 0, %ld, '%s')", sysCat.data(), SEABASE_MD_SCHEMA,
              SEABASE_TEXT, uid, COM_USER_PASSWORD, NA_JulianTimestamp(), dePassword);
END:
  return NAString(buf);
}

int CmpSeabaseDDLauth::getMaxUserIdInUsed(ExeCliInterface *cliInterface) {
  Queue *cntQueue = NULL;
  char buf[1024] = {0};
  str_sprintf(buf, "select MAX(AUTH_ID) from %s.\"%s\".%s where AUTH_TYPE = '%s'",
              CmpSeabaseDDL::getSystemCatalogStatic().data(), SEABASE_MD_SCHEMA, SEABASE_AUTHS, COM_USER_CLASS_LIT);
  int cliRC = cliInterface->fetchAllRows(cntQueue, buf, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    UserException excp(NULL, 0);
    throw excp;
  }
  cntQueue->position();
  OutputInfo *pCliRow = (OutputInfo *)cntQueue->getNext();

  return *(int *)pCliRow->get(0);
}

void CmpSeabaseDDLauth::initAuthPwd(ExeCliInterface *cliInterface) {
  cliInterface->autoCommit(false);
  Queue *cntQueue = NULL;
  char buf[1024] = {0};
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  str_sprintf(buf,
              "select AUTH_ID from %s.\"%s\".%s where AUTH_TYPE = '%s' except select TEXT_UID from %s.\"%s\".%s where "
              "TEXT_TYPE = %d",
              sysCat.data(), SEABASE_MD_SCHEMA, SEABASE_AUTHS, COM_USER_CLASS_LIT, sysCat.data(), SEABASE_MD_SCHEMA,
              SEABASE_TEXT, COM_USER_PASSWORD);
  int cliRC = cliInterface->fetchAllRows(cntQueue, buf, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
    cliInterface->rollbackWork();
    UserException excp(NULL, 0);
    throw excp;
  } else if (cliRC == 100 || cntQueue->numEntries() == 0) {
    // all ready!
    goto UPDATE_MAX_USERID;
  }
  cntQueue->position();
  for (int count = cntQueue->numEntries(); count; count--) {
    OutputInfo *pCliRow = (OutputInfo *)cntQueue->getNext();
    int uid = *((int *)pCliRow->get(0));
    NAString stmt = CmpSeabaseDDLuser::getDefAuthInfo(uid);
    if (stmt.isNull()) {
      continue;
    }
    cliRC = cliInterface->executeImmediate(stmt);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      cliInterface->rollbackWork();
      UserException excp(NULL, 0);
      throw excp;
    }
  }
UPDATE_MAX_USERID:
  // update max userID in used
  {
    int maxUserID = getMaxUserIdInUsed(cliInterface);
    char buf[1024] = {0};
    str_sprintf(buf, "update %s.\"%s\".%s set SUB_ID = %d where TEXT_UID = %d and TEXT_TYPE = %d", sysCat.data(),
                SEABASE_MD_SCHEMA, SEABASE_TEXT, maxUserID, ROOT_USER_ID, COM_USER_PASSWORD);
    cliRC = cliInterface->executeImmediate(buf);
    if (cliRC < 0) {
      cliInterface->retrieveSQLDiagnostics(CmpCommon::diags());
      cliInterface->rollbackWork();
      UserException excp(NULL, 0);
      throw excp;
    }
  }
  cliInterface->commitWork();
}

void CmpSeabaseDDLauth::getAuthDbNameByAuthIDs(std::map<int32_t, std::string *> &authMap) {
  if (authMap.empty()) {
    return;
  }

  std::string authIDs = "(";
  for (std::map<int32_t, std::string *>::iterator itor = authMap.begin(); itor != authMap.end(); itor++) {
    // for gcc 4.4.x
    long long int tmp = itor->first;
    authIDs += std::to_string(tmp);
    authIDs += ",";
  }
  authIDs.replace(authIDs.length() - 1, 1, ")");
  NAString sysCat = CmpSeabaseDDL::getSystemCatalogStatic();
  int stmtSize = authIDs.length() + 500;
  int flagBits = GetCliGlobals()->currContext()->getSqlParserFlags();
  CmpSeabaseDDLauth::AuthStatus authStatus;
  GetCliGlobals()->currContext()->setSqlParserFlags(0x20000);
  char buf[stmtSize];
  // get id and name
  str_sprintf(buf, "select distinct auth_id,auth_db_name from %s.\"%s\".%s where auth_id in %s", sysCat.data(),
              SEABASE_MD_SCHEMA, SEABASE_AUTHS, authIDs.c_str());

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  Queue *queue = NULL;

  // set pointer in diags area
  int32_t diagsMark = CmpCommon::diags()->mark();

  int32_t cliRC = cliInterface.fetchAllRows(queue, (char *)buf, 0, false, false, true);
  if (cliRC < 0) {
    cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
    goto END;
  }

  if (cliRC == 100)  // did not find the row
  {
    CmpCommon::diags()->rewind(diagsMark);
    goto END;
  }

  queue->position();
  for (int idx = 0; idx < queue->numEntries(); idx++) {
    OutputInfo *pCliRow = (OutputInfo *)queue->getNext();
    int32_t authID = *((int *)pCliRow->get(0));
    authMap[authID] = new (STMTHEAP) std::string((char *)pCliRow->get(1));
  }
  GetCliGlobals()->currContext()->setSqlParserFlags(flagBits);
  return;
END:
  GetCliGlobals()->currContext()->setSqlParserFlags(flagBits);
  authMap.clear();
}
