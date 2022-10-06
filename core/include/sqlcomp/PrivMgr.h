//*****************************************************************************

//*****************************************************************************

#ifndef PRIVMGR_H
#define PRIVMGR_H

#include <set>
#include <string>
#include <vector>
#include "sqlcomp/PrivMgrDefs.h"
#include "PrivMgrMDTable.h"
#include "PrivMgrComponentDefs.h"
#include "PrivMgrUserPrivs.h"
#include "common/ComSmallDefs.h"
#include "sqlcomp/CmpSeabaseDDLauth.h"

// following includes needed for diags interface
class ComDiagsArea;


// Forward references
class NATable;

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class PrivMgr;

// *****************************************************************************
// * Class:         PrivMgr
// * Description:  This is the base class for the Trafodion Privilege Manager.
// *
// *****************************************************************************
class PrivMgr {
 public:
  enum PrivMDStatus {
    PRIV_INITIALIZED = 30,
    PRIV_UNINITIALIZED = 31,
    PRIV_PARTIALLY_INITIALIZED = 32,
    PRIV_INITIALIZE_UNKNOWN = 33
  };

  enum PrivMgrTableEnum {
    OBJECT_PRIVILEGES_TABLE = 30,
    COLUMN_PRIVILEGES_TABLE = 31,
    SCHEMA_PRIVILEGES_TABLE = 32,
    COMPONENTS_TABLE = 33,
    COMPONENT_OPERATIONS_TABLE = 34,
    COMPONENT_PRIVILEGES_TABLE = 35,
    ROLE_USAGE_TABLE = 36,
    UNKNOWN_TABLE = 37
  };

  enum PrivCommand {
    GRANT_OBJECT = 30,
    GRANT_COLUMN = 31,
    REVOKE_OBJECT_RESTRICT = 32,
    REVOKE_OBJECT_CASCADE = 33,
    REVOKE_COLUMN_RESTRICT = 34,
    REVOKE_COLUMN_CASCADE = 35,
    UNKNOWN_PRIV_COMMAND = 36
  };

  bool isRevokeCommand(const PrivCommand command) {
    return (command == REVOKE_OBJECT_RESTRICT || command == REVOKE_OBJECT_CASCADE ||
            command == REVOKE_COLUMN_RESTRICT || command == REVOKE_COLUMN_CASCADE);
  }

  bool isGrantCommand(const PrivCommand command) { return (command == GRANT_OBJECT || command == GRANT_COLUMN); }

  // -------------------------------------------------------------------
  // Static functions:
  // -------------------------------------------------------------------

  // 4.4.6 implementation of to_string only supports double, long long int,
  // and unsigned long long int.  Update when int, etc. are supported.
  static inline std::string authIDToString(const int32_t value) {
    return std::to_string(static_cast<long long int>(value));
  }
  static inline std::string UIDToString(const int64_t value) {
    return std::to_string(static_cast<long long int>(value));
  }

  static const char *getSQLOperationCode(SQLOperation operation);
  static const char *getSQLOperationDescription(SQLOperation operation);
  static const char *getSQLOperationName(SQLOperation operation);
  static const char *getSQLOperationName(std::string operationCode);
  static int32_t getSQLUnusedOpsCount();
  static bool isSQLAlterOperation(SQLOperation operation);
  static bool isSQLCreateOperation(SQLOperation operation);
  static bool isSQLDropOperation(SQLOperation operation);
  static bool isSQLManageOperation(SQLOperation operation);
  static bool isSQLManageOperation(const char *operationCode);
  static bool isMetadataObject(const std::string objectName, std::string &schName);
  static const char *ObjectEnumToLit(ComObjectType objectType);
  static ComObjectType ObjectLitToEnum(const char *objectLiteral);
  static bool isRoleID(int authID) { return CmpSeabaseDDLauth::isRoleID(authID); }
  static bool isUserID(int authID) { return CmpSeabaseDDLauth::isUserID(authID); }
  static bool isTenantID(int authID) { return CmpSeabaseDDLauth::isTenantID(authID); }
  static bool isUserGroupID(int authID) { return CmpSeabaseDDLauth::isUserGroupID(authID); }

  static bool isDelimited(const std::string &identifier);
  static bool isSecurableObject(const ComObjectType objectType) {
    return (objectType == COM_BASE_TABLE_OBJECT || objectType == COM_LIBRARY_OBJECT ||
            objectType == COM_USER_DEFINED_ROUTINE_OBJECT || objectType == COM_TRIGGER_OBJECT ||
            objectType == COM_PACKAGE_OBJECT || objectType == COM_VIEW_OBJECT ||
            objectType == COM_SEQUENCE_GENERATOR_OBJECT || objectType == COM_STORED_PROCEDURE_OBJECT ||
            objectType == COM_PRIVATE_SCHEMA_OBJECT || objectType == COM_SHARED_SCHEMA_OBJECT);
  }

  // Set default privileges for a bitmap based on a table or view
  static void setTablePrivs(PrivMgrBitmap &bitmap) {
    bitmap.reset();
    bitmap.set(SELECT_PRIV);
    bitmap.set(DELETE_PRIV);
    bitmap.set(INSERT_PRIV);
    bitmap.set(UPDATE_PRIV);
    bitmap.set(REFERENCES_PRIV);
    bitmap.set(ALTER_PRIV);
    bitmap.set(DROP_PRIV);
  }

  static void translateObjectName(const std::string inputName, std::string &outputName);

  void static buildGrantText(const std::string &privText, const std::string &objectGranteeText, const int32_t grantorID,
                             const std::string grantorName, bool isWGO, const int32_t ownerID, std::string &grantText);

  static PrivStatus buildPrivText(const std::vector<PrivMgrMDRow *> rowList, const PrivMgrObjectInfo &objectInfo,
                                  PrivLevel privLevel, ComDiagsArea *pDiags_, std::string &privilegeText);

  static void log(const std::string filename, const std::string message, const int index);

  // -------------------------------------------------------------------
  // Constructors and destructors:
  // -------------------------------------------------------------------
  PrivMgr();
  PrivMgr(const std::string &metadataLocation, ComDiagsArea *pDiags = NULL,
          PrivMDStatus authorizationEnabled = PRIV_INITIALIZED);
  PrivMgr(const std::string &trafMetadataLocation, const std::string &metadataLocation, ComDiagsArea *pDiags = NULL,
          PrivMDStatus authorizationEnabled = PRIV_INITIALIZED);
  PrivMgr(const PrivMgr &rhs);
  virtual ~PrivMgr(void);

  // -------------------------------------------------------------------
  // Accessors and destructors:
  // -------------------------------------------------------------------
  PrivStatus getEffectiveGrantor(const bool isGrantedBySpecified, const std::string grantedByName,
                                 const int objectOwner, int &effectiveGrantorID, std::string &effectiveGrantorName);

  PrivStatus getRoleIDsForUserID(int32_t userID, std::vector<int32_t> &roleIDs);

  PrivStatus getGranteeIDsForRoleIDs(const std::vector<int32_t> &roleIDs, std::vector<int32_t> &userIDs,
                                     bool includeSysGrantor = true);

  inline std::string getMetadataLocation(void) { return metadataLocation_; }
  inline const std::string &getMetadataLocation(void) const { return metadataLocation_; }
  inline std::string getTrafMetadataLocation(void) { return trafMetadataLocation_; }
  inline const std::string &getTrafMetadataLocation(void) const { return trafMetadataLocation_; }
  inline void setTrafMetadataLocation(const std::string &trafMetadataLocation) {
    trafMetadataLocation_ = trafMetadataLocation;
  }

  bool isAuthorizationEnabled(void);
  void setAuthorizationEnabled(PrivMDStatus authStatus) { authorizationEnabled_ = authStatus; }
  bool isAuthIDGrantedPrivs(const int32_t authID, std::vector<PrivClass> privClasses, std::vector<int64_t> &objectUIDs,
                            bool &hasComponentPriv);
  void resetFlags();
  void setFlags();

  bool doTenantPrivChecks(void) { return tenantPrivChecks_; }
  void setTenantPrivChecks(bool tenantPrivChecks) { tenantPrivChecks_ = tenantPrivChecks; }

  short updatePrivBitmap(const PrivType privType, int64_t &rowCount);

 protected:
  // Returns status of privilege manager metadata

  PrivMDStatus authorizationEnabled(std::set<std::string> &existingObjectList);

  // -------------------------------------------------------------------
  // Data members:
  // -------------------------------------------------------------------
  std::string trafMetadataLocation_;
  std::string metadataLocation_;
  ComDiagsArea *pDiags_;
  unsigned int parserFlags_;
  PrivMDStatus authorizationEnabled_;
  bool tenantPrivChecks_;

};  // class PrivMgr

#endif  // PRIVMGR_H
