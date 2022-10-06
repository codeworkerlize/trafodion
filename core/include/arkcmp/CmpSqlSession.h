
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpSqlSession.h
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
#ifndef CMPSQLSESSION_H
#define CMPSQLSESSION_H

#include "common/NABoolean.h"
#include "export/NAStringDef.h"
#include "optimizer/ObjectNames.h"
#include "common/ComSqlId.h"

class CmpSqlSession : public NABasicObject {
 public:
  CmpSqlSession(NAHeap *heap);

  ~CmpSqlSession();

  // Validates that the input schema or table name is a valid
  // volatile name.
  // Validation is if it doesn't contain the reserved
  // volatile name prefix, is not more than 2 parts, and
  // contains the current user name if a 2-part name.
  // All validate methods return TRUE is valid, FALSE otherwise.
  // CmpCommon::diags is set in case of invalidation.
  static NABoolean isValidVolatileSchemaName(NAString &schName);
  NABoolean validateVolatileSchemaName(NAString &schName);
  NABoolean validateVolatileQualifiedSchemaName(QualifiedName &inName);
  NABoolean validateVolatileQualifiedName(QualifiedName &inName);
  NABoolean validateVolatileCorrName(CorrName &corrName);
  NABoolean validateVolatileName(const char *name);

  QualifiedName *updateVolatileQualifiedName(QualifiedName &inName);
  QualifiedName *updateVolatileQualifiedName(const NAString &inName);
  CorrName getVolatileCorrName(CorrName &corrName);
  SchemaName *updateVolatileSchemaName();

  void setSessionId(NAString &sessionID);

  const NAString &getSessionId() { return sessionID_; }

  void setSessionUsername(NAString &userName);

  NAString &getDatabaseUserName() { return databaseUserName_; }
  int &getDatabaseUserID() { return databaseUserID_; }

  int setDatabaseUserAndTenant(int userID, const char *userName, int tenantID, const char *tenantName,
                                 const char *tenantNodes, const char *tenantDefaultSchema);

  NAString &volatileSchemaName() { return volatileSchemaName_; }
  void setVolatileSchemaName(NAString &volatileSchemaName);

  NAString &volatileCatalogName() { return volatileCatalogName_; }
  void setVolatileCatalogName(NAString &volatileCatalogName, NABoolean noSegmentAppend = FALSE);

  NABoolean sessionInUse() { return sessionInUse_; }
  NABoolean volatileSchemaInUse();

  void setSessionInUse(NABoolean v) { sessionInUse_ = v; };
  void setVolatileSchemaInUse(NABoolean v) { volatileSchemaInUse_ = v; };

  void disableVolatileSchemaInUse();
  void saveVolatileSchemaInUse();
  void restoreVolatileSchemaInUse();
  void setParentQid(const char *parentQid);
  const char *getParentQid() { return parentQid_; }

  inline int getNumSessions() { return numSessions_; }

  int getTenantID() { return tenantID_; }

 private:
  NAHeap *heap_;

  NAString sessionID_;
  int numSessions_;
  int databaseUserID_;
  NAString databaseUserName_;
  int tenantID_;
  NAString tenantName_;

  // On NSK we store a Guardian user name and the external LDAP
  // name. On other platforms the value of externalUserName_ is always
  // the same as databaseUserName_.
  NAString externalUserName_;

  NAString volatileSchemaName_;

  NAString volatileCatalogName_;

  long segmentNum_;
  NAString segmentName_;

  NABoolean sessionInUse_;
  NABoolean volatileSchemaInUse_;

  NABoolean vsiuWasSaved_;
  NABoolean savedVSIU_;
  char *parentQid_;

  // Private method to retrieve user information from CLI and store a
  // copy in the databaseUserID_ and databaseUserName_ members. The
  // return value is a SQLCODE. When a value other than zero is
  // returned, error information is written into CmpCommon::diags().
  int getUserInfoFromCLI();
};

#endif  // CMPSQLSESSION_H
