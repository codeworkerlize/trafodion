
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpSqlSession.cpp
 * Description:
 *
 * Created:      6/6/2006
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "arkcmp/CmpSqlSession.h"

#include "cli/SQLCLIdev.h"
#include "common/ComCextdecs.h"
#include "common/ComRtUtils.h"
#include "common/ComSchemaName.h"
#include "common/ComSmallDefs.h"
#include "common/ComSqlId.h"
#include "common/NAUserId.h"
#include "common/Platform.h"
#include "optimizer/ObjectNames.h"

#define SQLPARSERGLOBALS_FLAGS
#include "parser/SqlParserGlobals.h"  // last #include

CmpSqlSession::CmpSqlSession(NAHeap *heap)
    : heap_(heap),
      numSessions_(0),
      sessionInUse_(FALSE),
      volatileSchemaInUse_(FALSE),
      vsiuWasSaved_(FALSE),
      savedVSIU_(FALSE),
      segmentNum_(-1),
      parentQid_(NULL),
      tenantID_(0) {
  // The initial user and tenant info will be the default identity
  // chosen by the local CLI. Call getUserInfoFromCLI() to retrieve
  // the user and tenant info from CLI and store copies in this
  // instance.
  int sqlcode = getUserInfoFromCLI();
  CMPASSERT(sqlcode == 0);
}

CmpSqlSession::~CmpSqlSession() {
  if (parentQid_) {
    NADELETEBASIC(parentQid_, heap_);
    parentQid_ = NULL;
  }
}

// Private method to retrieve user and tenant information from CLI
// and store copies in the databaseUserID_, databaseUserName_,
// tenantID_ and tenantName_ members. The return value is a SQLCODE.
// When a value other than zero is returned, error information is
// written into CmpCommon::diags().
int CmpSqlSession::getUserInfoFromCLI() {
  NABoolean doDebug = FALSE;
#ifdef _DEBUG
  doDebug = (getenv("DBUSER_DEBUG") ? TRUE : FALSE);
  if (doDebug) printf("[DBUSER:%d] BEGIN CmpSqlSession::getUserInfoFromCLI\n", (int)getpid());
#endif

  int sqlcode = 0;
  int localUserID = 0;
  char localUserName[MAX_DBUSERNAME_LEN + 1] = "";
  int localTenantID = 0;
  char localTenantName[MAX_DBUSERNAME_LEN + 1] = "";

  sqlcode = SQL_EXEC_GetSessionAttr(SESSION_DATABASE_USER_ID, &localUserID, NULL, 0, NULL);
  if (sqlcode != 0) {
    SQL_EXEC_MergeDiagnostics_Internal(*CmpCommon::diags());
    SQL_EXEC_ClearDiagnostics(NULL);
  }

  if (doDebug) printf("[DBUSER:%d]   SQL_EXEC_GetSessionAttr for user ID returned %d\n", (int)getpid(), (int)sqlcode);

  if (sqlcode >= 0) {
    sqlcode = SQL_EXEC_GetSessionAttr(SESSION_DATABASE_USER_NAME, NULL, localUserName, sizeof(localUserName), NULL);
    if (sqlcode != 0) {
      SQL_EXEC_MergeDiagnostics_Internal(*CmpCommon::diags());
      SQL_EXEC_ClearDiagnostics(NULL);
    }

    if (doDebug)
      printf("[DBUSER:%d]   SQL_EXEC_GetSessionAttr for user name returned %d\n", (int)getpid(), (int)sqlcode);
  }

  if (sqlcode >= 0) {
    sqlcode = SQL_EXEC_GetSessionAttr(SESSION_TENANT_ID, &localTenantID, NULL, 0, NULL);
    if (sqlcode != 0) {
      SQL_EXEC_MergeDiagnostics_Internal(*CmpCommon::diags());
      SQL_EXEC_ClearDiagnostics(NULL);
    }

    if (doDebug)
      printf("[DBUSER:%d]   SQL_EXEC_GetSessionAttr for tenant ID returned %d\n", (int)getpid(), (int)sqlcode);
  }

  if (sqlcode >= 0) {
    sqlcode = SQL_EXEC_GetSessionAttr(SESSION_TENANT_NAME, NULL, localTenantName, sizeof(localTenantName), NULL);
    if (sqlcode != 0) {
      SQL_EXEC_MergeDiagnostics_Internal(*CmpCommon::diags());
      SQL_EXEC_ClearDiagnostics(NULL);
    }

    if (doDebug)
      printf("[DBUSER:%d]   SQL_EXEC_GetSessionAttr for tenant name returned %d\n", (int)getpid(), (int)sqlcode);
  }

  if (sqlcode >= 0) {
    databaseUserID_ = localUserID;
    databaseUserName_ = localUserName;
    tenantID_ = localTenantID;
    tenantName_ = localTenantName;

    // On Linux the value of externalUserName_ is always the same as
    // databaseUserName_
    externalUserName_ = localUserName;

    if (doDebug) {
      printf("[DBUSER:%d]   Retrieved user ID %d, name [%s]\n", (int)getpid(), (int)localUserID, localUserName);
      printf("[DBUSER:%d]   Retrieved tenant ID %d, tenant name [%s]\n", (int)getpid(), (int)localTenantID,
             localTenantName);
    }
  }

  if (doDebug) printf("[DBUSER:%d] END CmpSqlSession::getUserInfoFromCLI\n", (int)getpid());

  return sqlcode;
}

// This method is called when a message from the master executor
// arrives informing the compiler to establish a new user
// identity. The return value is a SQLCODE. When a value other than
// zero is returned, error information is found in CmpCommon::diags().
//
// The method performs the following steps
// 1. The method is a no-op if the new user ID and tenant ID is the same
//    as the current user ID and tenant ID
// 2. Call CLI with the new user and tenant information. This establishes
//    the new user identity.
// 3. Call a helper method that will retrieve the current user and tenant
//    information from CLI and store copies of those values in data
//    members.
int CmpSqlSession::setDatabaseUserAndTenant(int userID, const char *userName, int tenantID, const char *tenantName,
                                            const char *tenantNodes, const char *tenantDefaultSchema) {
  NABoolean doDebug = FALSE;
#ifdef _DEBUG
  doDebug = (getenv("DBUSER_DEBUG") ? TRUE : FALSE);
  if (doDebug) {
    printf("[DBUSER:%d] BEGIN CmpSqlSession::setDatabaseUser\n", (int)getpid());
    printf("[DBUSER:%d]   Current user ID %d, new user ID %d\n", (int)getpid(), (int)databaseUserID_, (int)userID);
  }
#endif

  // 1. The method is a no-op if the new user ID is the same as the
  //    current user ID and the new tenant ID is the same as the
  //    current tenant ID.  This assumes that if the ID's match so
  //    do the names.
  if ((databaseUserID_ == userID) && (tenantID_ == tenantID)) {
    if (doDebug) printf("[DBUSER:%d] END CmpSqlSession::setDatabaseUser\n", (int)getpid());
    return 0;
  }

  int sqlcode = 0;

  // 2a. Call CLI with the new tenant
  if (tenantID_ != tenantID) {
    sqlcode = SQL_EXEC_SetSessionAttr_Internal(SESSION_TENANT_ID, tenantID, (char *)tenantName);
    if (sqlcode != 0) {
      SQL_EXEC_MergeDiagnostics_Internal(*CmpCommon::diags());
      SQL_EXEC_ClearDiagnostics(NULL);
    }
    sqlcode = SQL_EXEC_SetSessionAttr_Internal(SESSION_TENANT_NAME, tenantID, (char *)tenantName);
    if (sqlcode != 0) {
      SQL_EXEC_MergeDiagnostics_Internal(*CmpCommon::diags());
      SQL_EXEC_ClearDiagnostics(NULL);
    }
    if (doDebug) printf("[DBUSER:%d]   SQL_EXEC_SetSessionAttr on tenant returned %d\n", (int)getpid(), (int)sqlcode);

    sqlcode = SQL_EXEC_SetSessionAttr_Internal(SESSION_TENANT_NODES, 0, tenantNodes);
    if (sqlcode != 0) {
      SQL_EXEC_MergeDiagnostics_Internal(*CmpCommon::diags());
      SQL_EXEC_ClearDiagnostics(NULL);
    }

    if (doDebug)
      printf("[DBUSER:%d]   SQL_EXEC_SetSessionAttr on tenantNodes returned %d\n", (int)getpid(), (int)sqlcode);

    if (tenantDefaultSchema != "") {
      sqlcode = SQL_EXEC_SetSessionAttr_Internal(SESSION_TENANT_DEFAULT_SCHEMA, 0, (char *)tenantDefaultSchema);
      if (sqlcode != 0) {
        SQL_EXEC_MergeDiagnostics_Internal(*CmpCommon::diags());
        SQL_EXEC_ClearDiagnostics(NULL);
      }

      if (doDebug)
        printf("[DBUSER:%d]   SQL_EXEC_SetSessionAttr on tenantDefaultSchema returned %d\n", (int)getpid(),
               (int)sqlcode);
    }
  }

  // 2b. Call CLI with the new integer user identity
  if ((sqlcode >= 0) && (databaseUserID_ != userID)) {
    sqlcode = SQL_EXEC_SetSessionAttr_Internal(SESSION_DATABASE_USER, userID, (char *)userName);
    if (sqlcode != 0) {
      SQL_EXEC_MergeDiagnostics_Internal(*CmpCommon::diags());
      SQL_EXEC_ClearDiagnostics(NULL);
    }

    if (doDebug) printf("[DBUSER:%d]   SQL_EXEC_SetSessionAttr on user ID returned %d\n", (int)getpid(), (int)sqlcode);
  }

  // 3. Call a helper method that will retrieve the current user and tenant
  //    information from CLI and store copies
  if (sqlcode >= 0) sqlcode = getUserInfoFromCLI();

  if (doDebug) printf("[DBUSER:%d] END CmpSqlSession::setDatabaseUser\n", (int)getpid());

  return sqlcode;
}

void CmpSqlSession::setSessionId(NAString &sessionID) {
  sessionID_ = sessionID;

  if (NOT sessionID_.isNull()) {
    volatileSchemaName_ = COM_VOLATILE_SCHEMA_PREFIX;
    volatileSchemaName_ += COM_SESSION_ID_PREFIX;

    char sName[200];
    long cpu_l;
    long pin_l;
    long schemaNameCreateTime = 0;
    long sessionUniqNum;
    int userNameLen = 0;
    int userSessionNameLen = 0;
    int tenantIdLen = 0;
    ComSqlId::extractSqlSessionIdAttrs((char *)sessionID.data(), sessionID.length(), segmentNum_, cpu_l, pin_l,
                                       schemaNameCreateTime, sessionUniqNum, userNameLen, NULL, tenantIdLen, NULL,
                                       userSessionNameLen, NULL);
    str_sprintf(sName, "%02d%03ld%06ld%018ld%010ld", ComSqlId::SQ_SQL_ID_VERSION, segmentNum_, pin_l,
                schemaNameCreateTime, sessionUniqNum);
    volatileSchemaName_ += sName;

    volatileSchemaName_.toUpper();

    // get segment name
    segmentName_ = "NSK";

    sessionInUse_ = TRUE;
    volatileSchemaInUse_ = FALSE;
    //
    // it's a new session
    numSessions_++;
  } else {
    sessionInUse_ = FALSE;
    volatileSchemaInUse_ = FALSE;
  }
}

void CmpSqlSession::setSessionUsername(NAString &userName) {
  if (NOT userName.isNull()) {
    /* Prior to Seaquest M4, the string received here would be a
       session ID and we would extract the user name from the session
       ID. This step is no longer needed. The old code is shown here
       in this comment, in case the old scheme ever needs to be
       revived.

    char uName[42];
    long uNameLen = 40;

    ComSqlId::getSqlSessionIdAttr
      (ComSqlId::SQLQUERYID_USERNAME,
       (char*)sessionUsername.data(),
       sessionUsername.length(),
       uNameLen,
       uName);
    databaseUserName_ = uName;
    databaseUserName_.strip();
    */

    databaseUserName_ = userName;
    databaseUserName_.strip();

    short status = 1;
    char ldapName[ComSqlId::MAX_LDAP_USER_NAME_LEN + 1];

    if (status) {
      // On NT and Linux: the value of externalUserName_ is always the
      // same as databaseUserName_
      //
      // On NSK: It is a maintenance id if status == 13. Otherwise we
      // just revert to old behaviour and set the LDAP name to be the
      // same as the database user name. Stricter error handling here
      // will need to be tested more.
      strcpy(ldapName, databaseUserName_.data());
    }
    externalUserName_ = ldapName;
    externalUserName_.strip();
  }
}

void CmpSqlSession::setVolatileCatalogName(NAString &volatileCatalogName, NABoolean noSegmentAppend) {
  volatileCatalogName_ = volatileCatalogName;
  if ((NOT noSegmentAppend) && (NOT segmentName_.isNull())) {
    volatileCatalogName_ += "_";
    volatileCatalogName_ += segmentName_;
  }

  volatileCatalogName_.toUpper();
}

void CmpSqlSession::setVolatileSchemaName(NAString &volatileSchemaName) {
  volatileSchemaName_ = volatileSchemaName;

  volatileSchemaName_.toUpper();
}

NABoolean CmpSqlSession::isValidVolatileSchemaName(NAString &schName) {
  if (NOT schName.isNull()) {
    ComSchemaName csn(schName);

    if (NOT csn.isValid()) {
      // Schema name $0~SchemaName is not valid.
      *CmpCommon::diags() << DgSqlCode(-8009) << DgSchemaName(schName);
      return FALSE;
    }

    int len = MINOF(strlen(csn.getSchemaNamePartAsAnsiString().data()), strlen(COM_VOLATILE_SCHEMA_PREFIX));
    NAString upSch(csn.getSchemaNamePartAsAnsiString().data());
    upSch.toUpper();
    if ((len > 0) && (strncmp(upSch.data(), COM_VOLATILE_SCHEMA_PREFIX, len) == 0)) {
      return TRUE;
    }
  }

  return FALSE;
}

NABoolean CmpSqlSession::validateVolatileSchemaName(NAString &schName) {
  if (NOT schName.isNull()) {
    ComSchemaName csn(schName);

    if (NOT csn.isValid()) {
      // Schema name $0~SchemaName is not valid.
      *CmpCommon::diags() << DgSqlCode(-8009) << DgSchemaName(schName);
      return FALSE;
    }

    int len = MINOF(strlen(csn.getSchemaNamePartAsAnsiString().data()), strlen(COM_VOLATILE_SCHEMA_PREFIX));
    NAString upSch(csn.getSchemaNamePartAsAnsiString().data());
    upSch.toUpper();
    if ((NOT Get_SqlParser_Flags(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME)) && (len > 0) &&
        (strncmp(upSch.data(), COM_VOLATILE_SCHEMA_PREFIX, len) == 0)) {
      *CmpCommon::diags() << DgSqlCode(-4193) << DgString0(COM_VOLATILE_SCHEMA_PREFIX);
      return FALSE;
    }
  }

  return TRUE;
}

NABoolean CmpSqlSession::validateVolatileQualifiedSchemaName(QualifiedName &inName) {
  if (NOT inName.getSchemaName().isNull()) {
    if (!validateVolatileSchemaName((NAString &)inName.getSchemaNameAsAnsiString())) return FALSE;
  }

  return TRUE;
}

NABoolean CmpSqlSession::validateVolatileQualifiedName(QualifiedName &inName) {
  if (NOT Get_SqlParser_Flags(ALLOW_VOLATILE_SCHEMA_IN_TABLE_NAME)) {
    if (NOT inName.getCatalogName().isNull()) {
      // cannot be a 3-part name
      *CmpCommon::diags() << DgSqlCode(-4192);
      return FALSE;
    }

    if (NOT inName.getSchemaName().isNull()) {
      // validate that the schemaName part is the currentUserName
      if (inName.getSchemaName() != externalUserName_) {
        *CmpCommon::diags() << DgSqlCode(-4191) << DgString0(inName.getSchemaName()) << DgString1(externalUserName_);
        return FALSE;
      }
    }
  } else {
    // Volatile schema name is allowed.
    // Make sure that it is a valid volatile 3 part name.
    if ((NOT inName.getCatalogName().isNull()) && (NOT inName.getSchemaName().isNull())) {
      // move to a temp to upcase
      ComSchemaName csn(inName.getSchemaName());

      int len = MINOF(strlen(csn.getSchemaNamePartAsAnsiString().data()), strlen(COM_VOLATILE_SCHEMA_PREFIX));
      NAString upSch(csn.getSchemaNamePartAsAnsiString().data());
      upSch.toUpper();
      if ((len < strlen(COM_VOLATILE_SCHEMA_PREFIX)) || (strncmp(upSch.data(), COM_VOLATILE_SCHEMA_PREFIX, len) != 0)) {
        *CmpCommon::diags() << DgSqlCode(-4192);
        return FALSE;
      }
    } else if (NOT inName.getSchemaName().isNull()) {
      // 2 part name
      // validate that the schemaName part is the currentUserName
      if (inName.getSchemaName() != externalUserName_) {
        *CmpCommon::diags() << DgSqlCode(-4191) << DgString0(inName.getSchemaName()) << DgString1(externalUserName_);
        return FALSE;
      }
    }
  }

  return TRUE;
}

NABoolean CmpSqlSession::validateVolatileCorrName(CorrName &corrName) {
  // make sure that if schema name was specified as part of tablename,
  // it is the current user name.
  NABoolean isValid = FALSE;
  if (NOT corrName.isVolatile()) {
    // BYPASS_CHECK_FOR_VOLATILE_SCHEMA_NAME CQD was introduced as a workaround for problem seen in ALM case# 4764.
    if (((corrName.isSpecialTable()) && (CmpCommon::getDefault(BYPASS_CHECK_FOR_VOLATILE_SCHEMA_NAME) == DF_ON)) ||
        validateVolatileQualifiedName(corrName.getQualifiedNameObj())) {
      isValid = TRUE;
    }
  } else {
    isValid = TRUE;
  }

  return isValid;
}

NABoolean CmpSqlSession::validateVolatileName(const char *name) {
  ComObjectName volTabName(name);

  NAString schemaNamePart = volTabName.getSchemaNamePartAsAnsiString(TRUE);

  schemaNamePart.toUpper();

  int len = MINOF(schemaNamePart.length(), strlen(COM_VOLATILE_SCHEMA_PREFIX));

  if ((len < strlen(COM_VOLATILE_SCHEMA_PREFIX)) ||
      (strncmp(schemaNamePart.data(), COM_VOLATILE_SCHEMA_PREFIX, len) != 0)) {
    return FALSE;
  }

  return TRUE;
}

QualifiedName *CmpSqlSession::updateVolatileQualifiedName(QualifiedName &inName) {
  QualifiedName *result = &inName;

  if (volatileSchemaInUse_) {
    result = new (heap_) QualifiedName(inName.getObjectName(), volatileSchemaName(), volatileCatalogName(), heap_);

    CMPASSERT(result);

    result->setNamePosition(inName.getNamePosition());

    result->setIsVolatile(TRUE);
  }

  return result;
}

QualifiedName *CmpSqlSession::updateVolatileQualifiedName(const NAString &inName) {
  QualifiedName *result = NULL;

  if (volatileSchemaInUse_) {
    result = new (heap_) QualifiedName(inName, volatileSchemaName(), volatileCatalogName(), heap_);

    CMPASSERT(result);

    result->setIsVolatile(TRUE);
  }

  return result;
}

SchemaName *CmpSqlSession::updateVolatileSchemaName() {
  SchemaName *result = NULL;

  result = new (heap_) SchemaName(volatileSchemaName(), volatileCatalogName(), heap_);

  CMPASSERT(result);

  return result;
}

CorrName CmpSqlSession::getVolatileCorrName(CorrName &corrName) {
  NAString volTabName = corrName.getQualifiedNameObj().getObjectName();

  if ((NOT volatileCatalogName().isNull()) && (volatileSchemaName().isNull())) {
    *(CmpCommon::diags()) << DgSqlCode(-8009);
    return CorrName();
  }

  CorrName newCorrName(volTabName, heap_, volatileSchemaName(), volatileCatalogName(), corrName.getCorrNameAsString());
  newCorrName.setSpecialType(corrName.getSpecialType());

  return newCorrName;
}

void CmpSqlSession::saveVolatileSchemaInUse() {
  if (NOT vsiuWasSaved_) {
    vsiuWasSaved_ = TRUE;
    savedVSIU_ = volatileSchemaInUse_;
  }
};

void CmpSqlSession::disableVolatileSchemaInUse() {
  saveVolatileSchemaInUse();
  volatileSchemaInUse_ = FALSE;

  // set sqlparserflags to indicate volatile schema has
  // been disabled. This will be propagated to any mxcmp
  // that are started by this process.
  Set_SqlParser_Flags(DISABLE_VOLATILE_SCHEMA);

  SQL_EXEC_SetParserFlagsForExSqlComp_Internal(DISABLE_VOLATILE_SCHEMA);
}

void CmpSqlSession::restoreVolatileSchemaInUse() {
  if (vsiuWasSaved_) {
    volatileSchemaInUse_ = savedVSIU_;

    if (savedVSIU_) {
      Reset_SqlParser_Flags(DISABLE_VOLATILE_SCHEMA);

      SQL_EXEC_ResetParserFlagsForExSqlComp_Internal(DISABLE_VOLATILE_SCHEMA);
    }
  }

  vsiuWasSaved_ = FALSE;
};

NABoolean CmpSqlSession::volatileSchemaInUse() {
  return ((volatileSchemaInUse_) && (NOT Get_SqlParser_Flags(DISABLE_VOLATILE_SCHEMA)));
}

void CmpSqlSession::setParentQid(const char *parentQid) {
  if (parentQid) {
    int len = str_len(parentQid);
    if (len < ComSqlId::MIN_QUERY_ID_LEN) abort();
    if (len > ComSqlId::MAX_QUERY_ID_LEN) abort();
    if (0 != str_cmp(parentQid, COM_SESSION_ID_PREFIX, 4)) abort();
    if (parentQid_ == NULL) parentQid_ = new (heap_) char[ComSqlId::MAX_QUERY_ID_LEN + 1];
    strcpy(parentQid_, parentQid);
  } else if (parentQid_) {
    NADELETEBASIC(parentQid_, heap_);
    parentQid_ = NULL;
  }
}
