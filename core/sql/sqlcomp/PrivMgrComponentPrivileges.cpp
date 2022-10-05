//*****************************************************************************

//*****************************************************************************
#include "sqlcomp/PrivMgrComponentPrivileges.h"

#include "sqlcomp/PrivMgrDefs.h"
#include "PrivMgrComponentDefs.h"
#include "sqlcomp/PrivMgrMD.h"
#include "PrivMgrMDTable.h"
#include "PrivMgrComponents.h"
#include "PrivMgrComponentOperations.h"
#include "sqlcomp/PrivMgrRoles.h"
#include "sqlcomp/PrivMgrObjects.h"

#include <string>
#include <cstdio>
#include <algorithm>
#include <vector>
#include "common/ComSmallDefs.h"
#include "sqlcomp/CmpSeabaseDDL.h"

// sqlcli.h included because ExExeUtilCli.h needs it (and does not include it!)
#include "cli/sqlcli.h"
#include "executor/ExExeUtilCli.h"
#include "comexe/ComQueue.h"
#include "export/ComDiags.h"
#include "comexe/ComQueue.h"
// CmpCommon.h contains STMTHEAP declaration
#include "common/CmpCommon.h"
#include "sqlcomp/CmpDDLCatErrorCodes.h"
#include "common/ComUser.h"
#include "cli/SQLCLIdev.h"
#include "common/ComSecurityKey.h"

static bool isSQLDMLPriv(const int64_t componentUID, const std::string operationCode);

namespace ComponentPrivileges {

class UserPrivData {
 public:
  int32_t granteeID_;
  std::vector<int32_t> roleIDs_;
  PrivObjectBitmap bitmap_;
  bool managePrivileges_;
  bool selectMetadata_;
};

// *****************************************************************************
// * Class:        MyRow
// * Description:  This class represents a row in the COMPONENT_PRIVILEGES table.
// *****************************************************************************
class MyRow : public PrivMgrMDRow {
 public:
  // -------------------------------------------------------------------
  // Constructors and destructors:
  // -------------------------------------------------------------------
  MyRow(std::string tableName)
      : PrivMgrMDRow(tableName, COMPONENT_PRIVILEGES_ENUM), componentUID_(0), visited_(false), deleted_(false){};
  MyRow(const MyRow &other) : PrivMgrMDRow(other), visited_(false), deleted_(false) {
    componentUID_ = other.componentUID_;
    operationCode_ = other.operationCode_;
    granteeID_ = other.granteeID_;
    granteeName_ = other.granteeName_;
    grantorID_ = other.grantorID_;
    grantorName_ = other.grantorName_;
    grantDepth_ = other.grantDepth_;
    visited_ = other.visited_;
    deleted_ = other.deleted_;
  };
  virtual ~MyRow(){};

  bool operator==(const MyRow &other) const {
    return ((componentUID_ == other.componentUID_) && (operationCode_ == other.operationCode_) &&
            (granteeID_ == other.granteeID_) && (grantorID_ == other.grantorID_));
  }

  inline void clear() { componentUID_ = 0; };

  void describeGrant(const std::string &operationName, const std::string &componentName,
                     std::vector<std::string> &outlines);

  // -------------------------------------------------------------------
  // Data Members:
  // -------------------------------------------------------------------

  //  From COMPONENT_PRIVILEGES
  int64_t componentUID_;
  std::string operationCode_;
  int32_t granteeID_;
  std::string granteeName_;
  int32_t grantorID_;
  std::string grantorName_;
  int32_t grantDepth_;
  bool visited_;
  bool deleted_;

 private:
  MyRow();
};

// *****************************************************************************
// * Class:        MyTable
// * Description:  This class represents the COMPONENT_PRIVILEGES table.
// *
// *****************************************************************************
class MyTable : public PrivMgrMDTable {
 public:
  MyTable(const std::string &tableName, ComDiagsArea *pDiags = NULL)
      : PrivMgrMDTable(tableName, COMPONENT_PRIVILEGES_ENUM, pDiags), lastRowRead_(tableName){};

  inline void clear() { lastRowRead_.clear(); };

  bool dependentGrantsExist(const int32_t grantorID, const int32_t granteeID, const bool onlyWGO,
                            std::vector<MyRow> &rows);

  PrivStatus fetchCompPrivInfo(const int32_t granteeID, const std::vector<int32_t> &roleIDs, PrivObjectBitmap &privs,
                               bool &hasManagePrivPriv, bool &hasSelectMetadata, bool &hasAnyManagePriv);

  PrivStatus fetchOwner(const int64_t componentUID, const std::string &operationCode, int32_t &grantee);

  MyRow findGrantRow(const int32_t componentUID, const int32_t grantorID, const std::string granteeName,
                     const std::string &operationCode, const std::vector<MyRow> &rows);

  bool getOwnerRow(const std::string &operationCode, const std::vector<MyRow> &rows, MyRow &row);

  void getRowsForGrantee(const MyRow &baseRow, std::vector<MyRow> &masterRowList, std::set<size_t> &rowsToDelete);

  void getTreeOfGrantors(const int32_t grantee, const std::vector<MyRow> &rows, std::set<int32_t> &grantorIDs);

  int32_t hasWGO(int32_t authID, const std::vector<int32_t> &roleIDs, const std::vector<MyRow> &rows,
                 PrivMgrComponentPrivileges *compPrivs, std::string &roleName);

  virtual PrivStatus insert(const PrivMgrMDRow &row);

  void scanBranch(const int32_t &grantor, std::vector<MyRow> &rows);

  PrivStatus selectAllWhere(const std::string &whereClause, const std::string &orderByClause, std::vector<MyRow> &rows);

  virtual PrivStatus selectWhereUnique(const std::string &whereClause, PrivMgrMDRow &row);

  PrivStatus selectWhere(const std::string &whereClause, std::vector<MyRow *> &rowList);

  void setRow(OutputInfo &cliInterface, PrivMgrMDRow &rowOut);

  void describeGrantTree(const int32_t grantorID, const std::string &operationName, const std::string &componentName,
                         const std::vector<MyRow> &rows, std::vector<std::string> &outlines);

  int32_t findGrantor(int32_t currentRowID, const int32_t grantorID, const std::vector<MyRow> rows);

 private:
  MyTable();

  MyRow lastRowRead_;
  UserPrivData userPrivs_;
};
}  // End namespace ComponentPrivileges
using namespace ComponentPrivileges;

// *****************************************************************************
//    PrivMgrComponentPrivileges methods
// *****************************************************************************
// -----------------------------------------------------------------------
// Construct a PrivMgrComponentPrivileges object for a new component operation.
// -----------------------------------------------------------------------
PrivMgrComponentPrivileges::PrivMgrComponentPrivileges()
    : PrivMgr(),
      fullTableName_(metadataLocation_ + "." + PRIVMGR_COMPONENT_PRIVILEGES),
      myTable_(*new MyTable(fullTableName_, pDiags_)){};

PrivMgrComponentPrivileges::PrivMgrComponentPrivileges(const std::string &metadataLocation, ComDiagsArea *pDiags)
    : PrivMgr(metadataLocation, pDiags),
      fullTableName_(metadataLocation_ + "." + PRIVMGR_COMPONENT_PRIVILEGES),
      myTable_(*new MyTable(fullTableName_, pDiags)){};

// -----------------------------------------------------------------------
// Copy constructor
// -----------------------------------------------------------------------
PrivMgrComponentPrivileges::PrivMgrComponentPrivileges(const PrivMgrComponentPrivileges &other)
    : PrivMgr(other), myTable_(*new MyTable(fullTableName_, pDiags_)) {
  fullTableName_ = other.fullTableName_;
}

// -----------------------------------------------------------------------
// Destructor.
// -----------------------------------------------------------------------
PrivMgrComponentPrivileges::~PrivMgrComponentPrivileges() { delete &myTable_; }

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::clear                               *
// *                                                                           *
// *    This function clears any cache associated with this object.            *
// *                                                                           *
// *****************************************************************************
void PrivMgrComponentPrivileges::clear()

{
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  myTable.clear();
}
//****************** End of PrivMgrComponentPrivileges::clear ******************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::describeComponentPrivileges         *
// *  calls selectAllWhere to get the list of privileges for the operation.    *
// *  Once list is generated, starts at the root (system grant to owner) and   *
// *  generates grant statements by traversing each branch based on grantor    *
// *  -> grantee relationship.                                                 *
// *                                                                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  componentUIDString               const std::string &                  In *
// *    used with operationCode to find wanted component privileges.           *
// *                                                                           *
// *  componentName                    const std::string &                  In *
// *    used for generate grant statement on the component.                    *
// *                                                                           *
// *  operationCode                    const std::string &                  In *
// *    used with componentUIDString to find wanted component privileges.      *
// *                                                                           *
// *  operationName                    const std::string &                  In *
// *     used for generate grant statement as granted operation.               *
// *                                                                           *
// *  outlines                         std::vector<std::string> &           Out*
// *      output generated GRANT statements to this array.                     *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Component privilege(s) were revoked                          *
// *           *: One or more component privileges were not revoked.           *
// *              A CLI error is put into the diags area.                      *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrComponentPrivileges::describeComponentPrivileges(const std::string &componentUIDString,
                                                                   const std::string &componentName,
                                                                   const std::string &operationCode,
                                                                   const std::string &operationName,
                                                                   std::vector<std::string> &outlines) {
  // Get the list of all privileges granted to the component and component
  // operation
  std::string whereClause(" WHERE COMPONENT_UID = ");
  whereClause += componentUIDString;
  whereClause += " AND OPERATION_CODE = '";
  whereClause += operationCode;
  whereClause += "'";

  std::string orderByClause = "ORDER BY GRANTOR_ID, GRANTEE_ID, GRANT_DEPTH";

  std::vector<MyRow> rows;
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  PrivStatus privStatus = myTable.selectAllWhere(whereClause, orderByClause, rows);

  // We should get at least 1 row back - the grant from the system to the
  // component operation owner
  int32_t rowIndex = 0;
  if ((privStatus == STATUS_NOTFOUND) || (rows.size() == 0) ||
      (rowIndex = myTable.findGrantor(0, SYSTEM_USER, rows) == -1)) {
    std::string errorText("Unable to find any grants for operation ");
    errorText += operationName;
    errorText += " on component ";
    errorText += componentName;
    PRIVMGR_INTERNAL_ERROR(errorText.c_str());
    return STATUS_ERROR;
  }

  // Add the initial grant (system grant -> owner) to text (outlines)
  // There is only one system grant -> owner per component operation
  MyRow row = rows[rowIndex];
  row.describeGrant(operationName, componentName, outlines);

  // Traverse the base branch that starts with the owner and proceeds
  // outward.  In otherwords, describe grants where the grantee is the grantor.
  int32_t newGrantor = row.granteeID_;
  myTable.describeGrantTree(newGrantor, operationName, componentName, rows, outlines);

  return STATUS_GOOD;
}

//****** End of PrivMgrComponentPrivileges::describeComponentPrivileges ********

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::dropAll                             *
// *                                                                           *
// *    This function drops all component privileges.                          *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// *  STATUS_GOOD: All rows were deleted.                                      *
// * STATUS_ERROR: Execution failed. A CLI error is put into the diags area.   *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrComponentPrivileges::dropAll()

{
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  std::string whereClause(" ");

  return myTable.deleteWhere(whereClause);
}
//**************** End of PrivMgrComponentPrivileges::dropAll ******************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::dropAllForComponent                 *
// *                                                                           *
// *    This function drops all component privileges associated with the       *
// *  specified component.                                                     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentUIDString>            const std::string &             In       *
// *    is a string representation of the unique ID associated with the        *
// *    component.                                                             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// *  STATUS_GOOD: All rows associated with the component were deleted.        *
// * STATUS_ERROR: Execution failed. A CLI error is put into the diags area.   *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrComponentPrivileges::dropAllForComponent(const std::string &componentUIDString)

{
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  std::string whereClause("WHERE ");

  whereClause += "COMPONENT_UID = ";
  whereClause += componentUIDString.c_str();

  return myTable.deleteWhere(whereClause);
}
//*********** End of PrivMgrComponentPrivileges::dropAllForComponent ***********

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::dropAllForOperation                 *
// *                                                                           *
// *    This function drops all component privileges associated with the       *
// *  specified component and operation.                                       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentUIDString>            const std::string &             In       *
// *    is a string representation of the unique ID associated with the        *
// *    component.                                                             *
// *                                                                           *
// *  <operationCodeList>             const std::string &             In       *
// *    is a list of 2 character operation codes associateed with the component*
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// *  STATUS_GOOD: All rows associated with the component and operation        *
// *               were deleted.                                               *
// * STATUS_ERROR: Execution failed. A CLI error is put into the diags area.   *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrComponentPrivileges::dropAllForOperation(const std::string &componentUIDString,
                                                           const std::string &operationCode)

{
  MyTable &myTable = static_cast<MyTable &>(myTable_);
  std::string whereClause("WHERE ");

  whereClause += "COMPONENT_UID = ";
  whereClause += componentUIDString.c_str();
  whereClause += " AND OPERATION_CODE = '";
  whereClause += operationCode.c_str();
  whereClause += "'";

  return myTable.deleteWhere(whereClause);
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::dropAllForGrantee                   *
// *                                                                           *
// *    This function drops all component privileges that have been granted    *
// *  to the user specified as "granteeID".  If the grantee had the WGO then   *
// *  the branch of privileges started by granteeID are removed.               *
// *                                                                           *
// *  This code assumes that all roles have been revoked from the granteeID    *
// *  prior to being called.                                                   *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <granteeID>                     const int32_t                   In       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// *  true:  grantees were dropped                                             *
// * false:  unexpected error occurred. Error is put into the diags area.      *
// *                                                                           *
// *****************************************************************************
bool PrivMgrComponentPrivileges::dropAllForGrantee(const int32_t granteeID) {
  // Get the list of all privileges from component_privileges table
  // Skip rows granted by the system (-2)
  std::string whereClause(" WHERE GRANTOR_ID > 0");
  std::string orderByClause = " ORDER BY COMPONENT_UID, GRANTOR_ID, GRANTEE_ID, OPERATION_CODE, GRANT_DEPTH";

  MyTable &myTable = static_cast<MyTable &>(myTable_);
  std::vector<MyRow> masterRowList;

  PrivStatus privStatus = myTable.selectAllWhere(whereClause, orderByClause, masterRowList);
  if (privStatus == STATUS_ERROR) return false;

  // Create a list of indexes into the masterRowList where the granteeID is
  // the target of one or more privileges
  std::vector<size_t> granteeRowList;
  for (size_t i = 0; i < masterRowList.size(); i++) {
    if (masterRowList[i].granteeID_ == granteeID) granteeRowList.push_back(i);
  }

  // if the granteeID has not been granted any privileges, we are done
  if (granteeRowList.size() == 0) return true;

  // Add the rows from granteeRowList to rowsToDelete list
  // If any privileges were granted WGO, also remove the branch.
  std::set<size_t> rowsToDelete;
  for (size_t i = 0; i < granteeRowList.size(); i++) {
    size_t baseIdx = granteeRowList[i];
    MyRow baseRow = masterRowList[baseIdx];

    // If grantDepth < 0, then WGO was specified, remove branch
    if (baseRow.grantDepth_ < 0) myTable.getRowsForGrantee(baseRow, masterRowList, rowsToDelete);
    masterRowList[baseIdx].visited_ = true;
    rowsToDelete.insert(baseIdx);
  }

  // delete all the rows in affected list into statements of 10 rows
  if (rowsToDelete.size() > 0) {
    whereClause = "WHERE ";
    bool isFirst = true;
    size_t count = 0;
    for (std::set<size_t>::iterator it = rowsToDelete.begin(); it != rowsToDelete.end(); ++it) {
      if (count > 20) {
        privStatus == myTable.deleteWhere(whereClause);
        if (privStatus == STATUS_ERROR) return false;
        whereClause = "WHERE ";
        isFirst = true;
        count = 0;
      }
      if (isFirst)
        isFirst = false;
      else
        whereClause += " OR ";
      size_t masterIdx = *it;
      MyRow row = masterRowList[masterIdx];

      const std::string componentUIDString = to_string((long long int)row.componentUID_);
      whereClause += "(component_uid = ";
      whereClause += componentUIDString.c_str();
      whereClause += " AND grantor_name = '";
      whereClause += row.grantorName_;
      whereClause += "' AND grantee_name = '";
      whereClause += row.granteeName_;
      whereClause += "' AND operation_code = '";
      whereClause += row.operationCode_;
      whereClause += "')";
      count++;
    }
    privStatus == myTable.deleteWhere(whereClause);
    if (privStatus == STATUS_ERROR) return false;
  }

  return true;
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::getCount                            *
// *                                                                           *
// *    Returns the number of grants of component privileges.                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: int64_t                                                          *
// *                                                                           *
// *    Returns the number of grants of component privileges.                  *
// *                                                                           *
// *****************************************************************************
int64_t PrivMgrComponentPrivileges::getCount(int componentUID) {
  std::string whereClause(" ");
  if (componentUID != INVALID_COMPONENT_UID) {
    whereClause = "where component_uid = ";
    whereClause += to_string((long long int)componentUID);
  }

  int64_t rowCount = 0;
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  PrivStatus privStatus = myTable.selectCountWhere(whereClause, rowCount);

  if (privStatus != STATUS_GOOD) pDiags_->rewind(diagsMark);

  return rowCount;
}
//***************** End of PrivMgrComponentPrivileges::getCount ****************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::getSQLCompPrivs                     *
// *                                                                           *
// *    Returns the SQL_OPERATIONS privileges that may affect privileges       *
// * for metadata tables.                                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <granteeID>                     const int32_t                   In       *
// *    is the authorization ID of the grantee.                                *
// *                                                                           *
// *  <roleIDs>                       const std::vector<int32_t> &    In       *
// *    is a list of roleIDs granted to the grantee.                           *
// *                                                                           *
// *  <privs>                         PrivObjectBitmap &              In       *
// *    passes back the system-level DML privileges granted to the grantee.    *
// *                                                                           *
// *  <hasManagePrivPriv>             bool &                          In       *
// *    passes back if the user has MANAGE_PRIVILEGES authority.               *
// *                                                                           *
// *  <hasSelectMetadata>             bool &                          In       *
// *    passes back if the user has DML_SELECT_PRIVILEGE                       *
// *                                                                           *
// *  <hasAnyManagePriv>              bool &                          In       *
// *    passes back if the user has any MANAGE privilege                       *
// *                                                                           *
// *****************************************************************************

void PrivMgrComponentPrivileges::getSQLCompPrivs(const int32_t granteeID, const std::vector<int32_t> &roleIDs,
                                                 PrivObjectBitmap &privs, bool &hasManagePrivPriv,
                                                 bool &hasSelectMetadata, bool &hasAnyManagePriv)

{
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  PrivStatus privStatus =
      myTable.fetchCompPrivInfo(granteeID, roleIDs, privs, hasManagePrivPriv, hasSelectMetadata, hasAnyManagePriv);

  if (privStatus != STATUS_GOOD) pDiags_->rewind(diagsMark);
}
//************* End of PrivMgrComponentPrivileges::getSQLCompPrivs *************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::grantExists                         *
// *                                                                           *
// *    Determines if a specific authorization ID (granteee) has been granted  *
// * a component privilege by a specific authorization ID (grantor).           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentUIDString>            const std::string &             In       *
// *    is the component unique ID.                                            *
// *                                                                           *
// *  <operationCode>                 const std::string &             In       *
// *    is the two character code associated with the component operation.     *
// *                                                                           *
// *  <grantorID>                     int32_t                         In       *
// *    is the authorization ID of the grantor.                                *
// *                                                                           *
// *  <granteeName>                   const str::string &             In       *
// *    is the authorization name of the grantee.                              *
// *                                                                           *
// *  <granteeID>                     int32_t                         In       *
// *    is the authorization ID of the grantee.                                *
// *                                                                           *
// *  <grantDepth>                    int32_t &                       In       *
// *    passes back the grant depth if the component privilege grant exists.   *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: Component privilege has been granted to grantee by grantor.         *
// * false: Component privilege has not been granted to grantee by grantor,    *
// * or there was an error trying to read from the COMPONENT_PRIVILEGES table. *
// *                                                                           *
// *****************************************************************************
bool PrivMgrComponentPrivileges::grantExists(const std::string componentUIDString, const std::string operationCode,
                                             int32_t grantorID, const std::string &granteeName, int32_t &granteeID,
                                             int32_t &grantDepth)

{
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  // Not found in cache, look for the component name in metadata.
  std::string whereClause("WHERE COMPONENT_UID = ");

  whereClause += componentUIDString;
  whereClause += " AND OPERATION_CODE = '";
  whereClause += operationCode;
  whereClause += "' AND GRANTOR_ID = ";
  whereClause += authIDToString(grantorID);
  whereClause += " AND GRANTEE_NAME = '";
  whereClause += granteeName;
  whereClause += "'";

  MyRow row(fullTableName_);

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  PrivStatus privStatus = myTable.selectWhereUnique(whereClause, row);

  if (privStatus == STATUS_GOOD || privStatus == STATUS_WARNING) {
    grantDepth = row.grantDepth_;
    granteeID = row.granteeID_;
    return true;
  }

  pDiags_->rewind(diagsMark);

  granteeID = NA_UserIdDefault;
  return false;
}
//*************** End of PrivMgrComponentPrivileges::grantExists ***************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::grantPrivilege                      *
// *                                                                           *
// *    Grants the authority to perform one or more operations on a            *
// *  component to an authID; a row is added to the COMPONENT_PRIVILEGES       *
// *  table for each grant.                                                    *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentName>                 const std::string &              In      *
// *    is the component name.                                                 *
// *                                                                           *
// *  <operations>                    const std::vector<std::string> & In      *
// *    is a list of component operations to be granted.                       *
// *                                                                           *
// *  <grantorIDIn>                   const int32_t                    In      *
// *    is the auth ID granting the privilege.                                 *
// *                                                                           *
// *  <grantorName>                   const std::string &              In      *
// *    is the name of the authID granting the privilege.                      *
// *                                                                           *
// *  <granteeID>                     const int32_t                    In      *
// *    is the the authID being granted the privilege.                         *
// *                                                                           *
// *  <granteeName>                   const std::string &              In      *
// *    is the name of the authID being granted the privilege.                 *
// *                                                                           *
// *  <grantDepth>                    const int32_t                    In      *
// *    is the number of levels this privilege may be granted by the grantee.  *
// *  Initially this is either 0 or -1.                                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Component privilege(s) were granted                          *
// *           *: One or more component privileges were not granted.           *
// *              A CLI error is put into the diags area.                      *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrComponentPrivileges::grantPrivilege(const std::string &componentName,
                                                      const std::vector<std::string> &operations,
                                                      const int32_t grantorIDIn, const std::string &grantorNameIn,
                                                      const int32_t granteeID, const std::string &granteeName,
                                                      const int32_t grantDepth)

{
  // Determine if the component exists.
  PrivMgrComponents component(metadataLocation_, pDiags_);

  std::string componentUIDString;
  int64_t componentUID;
  bool isSystemComponent;
  std::string componentDescription;
  PrivStatus privStatus = STATUS_GOOD;

  if (component.fetchByName(componentName, componentUIDString, componentUID, isSystemComponent, componentDescription) !=
      STATUS_GOOD) {
    *pDiags_ << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) << DgString0(componentName.c_str());
    return STATUS_ERROR;
  }

  // Get roles assigned to the grantor
  PrivMgrRoles roles;
  std::vector<int32_t> roleIDs;

  // If the current user matches the grantorID, then get cached roles
  if (ComUser::getCurrentUser() == grantorIDIn) {
    NAList<int32_t> cachedRoleIDs(STMTHEAP);
    if (ComUser::getCurrentUserRoles(cachedRoleIDs, pDiags_) == -1) return STATUS_ERROR;

    // convert to std:string
    for (CollIndex i = 0; i < cachedRoleIDs.entries(); i++) roleIDs.push_back(cachedRoleIDs[i]);
  } else {
    // get roles granted to grantorID by reading metadata, etc.
    std::vector<std::string> roleNames;
    std::vector<int32_t> grantDepths;
    std::vector<int32_t> grantees;
    if (roles.fetchRolesForAuth(grantorIDIn, roleNames, roleIDs, grantDepths, grantees) == STATUS_ERROR)
      return STATUS_ERROR;
  }

  // OK, the component is defined, what about the operations?
  MyTable &myTable = static_cast<MyTable &>(myTable_);
  PrivMgrComponentOperations componentOperations(metadataLocation_, pDiags_);
  std::vector<std::string> operationCodes;

  int32_t grantorID = grantorIDIn;
  std::string grantorName = grantorNameIn;

  // Check each operation defined in the grant request.  In most cases, there
  // will only be one operation in the grant statement.
  // TDB:  if multiple operations are frequent, then code can be changed to
  // get component privileges for all requested operations ordered by
  // operation code, grantor, and grantee to reduce the number of I/O's
  // against the component_privileges table.
  for (size_t i = 0; i < operations.size(); i++) {
    std::string operationName = operations[i];

    // For the moment we are disabling DML_* privileges. We might remove
    // them in the future. Note that it will still be possible to revoke
    // them. (Note: sizeof counts null terminator, hence the -1.)
    if ((strncmp(operationName.c_str(), "DML_", sizeof("DML_") - 1) == 0) &&
        strcmp(operationName.c_str(), "DML_SELECT_METADATA") != 0) {
      *pDiags_ << DgSqlCode(-CAT_UNSUPPORTED_COMMAND_ERROR);
      return STATUS_ERROR;
    }

    // Get the operation code for the operation
    std::string operationCode;
    bool isSystemOperation = FALSE;
    std::string operationDescription;

    if (componentOperations.fetchByName(componentUIDString, operationName, operationCode, isSystemOperation,
                                        operationDescription) != STATUS_GOOD) {
      *pDiags_ << DgSqlCode(-CAT_INVALID_COMPONENT_PRIVILEGE) << DgString0(operationName.c_str())
               << DgString1(componentName.c_str());
      return STATUS_ERROR;
    }

    // Get all grants for this component and operation code
    std::string whereClause(" WHERE COMPONENT_UID = ");
    whereClause += componentUIDString;
    whereClause += " AND OPERATION_CODE = '";
    whereClause += operationCode;
    whereClause += "'";
    std::string orderByClause = "ORDER BY GRANTOR_ID, GRANTEE_ID";

    std::vector<MyRow> rows;
    MyTable &myTable = static_cast<MyTable &>(myTable_);
    if (myTable.selectAllWhere(whereClause, orderByClause, rows) == STATUS_ERROR) return STATUS_ERROR;

    std::string roleName;

    // See if grantor has the WGO based on the list of grants (rows) or
    // with MANAGE_COMPONENT privilege
    // retcode:   0 - no WGO privilege
    //            1 - grantor has WGO privilege
    //            2 - has WGO privilege through MANAGE COMPONENTS privilege
    //            3 - one or more roles granted to grantor has WGO privilege,
    //                first one in list returned in roleName
    int32_t retcode = myTable.hasWGO(grantorID, roleIDs, rows, this, roleName);

    // If grantor does not have WGO, return error
    if (retcode == 0) {
      *pDiags_ << DgSqlCode(-CAT_NOT_AUTHORIZED);
      return STATUS_ERROR;
    }

    // WGO comes from MANAGE_COMPONENTS privilege.  Set grantorID/grantorName
    // to operation owner.
    if (retcode == 2) {
      MyRow grantRow(fullTableName_);
      if (myTable.getOwnerRow(operationCode, rows, grantRow) == false) {
        std::string msg("Failed to find operation owner for: ");
        msg += operationName;
        msg += " on component: ";
        msg += componentName;
        PRIVMGR_INTERNAL_ERROR(msg.c_str());
        return STATUS_ERROR;
      }

      // update grantor information
      grantorID = grantRow.granteeID_;
      grantorName = grantRow.granteeName_;
    }

    // grantor has privilege through one or more roles, requester must
    // specify which one
    if (retcode == 3) {
      std::string msg("Please retry using the BY CLAUSE for role: ");
      msg += roleName;
      *pDiags_ << DgSqlCode(-CAT_PRIVILEGE_NOT_GRANTED) << DgString0(grantorName.c_str()) << DgString1(msg.c_str());
      return STATUS_ERROR;
    }

    // Mantis-10745:  no check existed to disallow user to grant to themselves.
    // This same issue can occur if there is a circular grant:
    //   user1 -> user2, revoke restrict fails because of user2 -> user3 grant
    //   user2 -> user3, revoke restrict fails because of user3 -> user1 grant
    //   user3 -> user1, revoke restrict fails because of user1 -> user2 grant
    // Prevent granting to self and don't allow circular grants
    if (grantorID == granteeID || granteeID == ComUser::getRootUserID()) {
      *pDiags_ << DgSqlCode(-CAT_CANT_GRANT_TO_SELF_OR_ROOT);
      return STATUS_ERROR;
    }

    std::set<int32_t> grantorIDs;
    myTable.getTreeOfGrantors(grantorID, rows, grantorIDs);

    // Example of preventing circular grants:
    //    system -> root added when operation is created (owner grant)
    //    root  -> user1 succeeds (grantorIDs are root, system)
    //    user1 -> user2 succeeds (grantorIDs are user1, root, system)
    //    user2 -> user3 succeeds (grantorIDs are user2, user1, root, system)
    //    user3 -> user1 fails (grantorIDs are user3, user2, user1, root, system),
    //             user1 has been granted this privilege through another grantor
    //    root  -> user4 succeeds (grantorIDs are root, system)
    //    user4 -> user1 succeeds (grantorIDs are user4, root, system)
    //    user1 -> user2 fails (grantorIDs are user1, user4, root, system, user3, user2)
    //             user2 has been granted this privilege by another grantor
    if (std::find(grantorIDs.begin(), grantorIDs.end(), granteeID) != grantorIDs.end()) {
      *pDiags_ << DgSqlCode(-CAT_CIRCULAR_PRIVS) << DgString0(grantorName.c_str()) << DgString1(granteeName.c_str());
      return STATUS_ERROR;
    }

    // Valid request
    operationCodes.push_back(operationCode);
  }

  // Operations are valid, add or update each entry.
  MyRow row(fullTableName_);

  row.componentUID_ = componentUID;
  row.grantDepth_ = grantDepth;
  row.granteeID_ = granteeID;
  row.granteeName_ = granteeName;
  row.grantorID_ = grantorID;
  row.grantorName_ = grantorName;

  std::string whereClauseHeader(" WHERE COMPONENT_UID = ");

  whereClauseHeader += componentUIDString;
  whereClauseHeader += " AND GRANTEE_ID = ";
  whereClauseHeader += authIDToString(granteeID);
  whereClauseHeader += " AND GRANTOR_ID = ";
  whereClauseHeader += authIDToString(grantorID);
  whereClauseHeader += " AND OPERATION_CODE = '";

  for (size_t oc = 0; oc < operationCodes.size(); oc++) {
    int32_t thisGrantDepth;

    // Add WITH GRANT OPTION if requested otherwise just continue.
    // No error is returned if privilege is already granted.
    int32_t granteeIDFromMD = NA_UserIdDefault;
    if (grantExists(componentUIDString, operationCodes[oc], grantorID, granteeName, granteeIDFromMD, thisGrantDepth)) {
      if (grantDepth == thisGrantDepth || grantDepth == 0) continue;

      std::string whereClause = whereClauseHeader + operationCodes[oc] + "'";

      // Set new grant depth
      std::string setClause(" SET GRANT_DEPTH = ");

      char grantDepthString[20];

      sprintf(grantDepthString, "%d", grantDepth);
      setClause += grantDepthString + whereClause;

      privStatus = myTable.update(setClause);
    } else {
      row.operationCode_ = operationCodes[oc];
      privStatus = myTable.insert(row);
    }

    if (privStatus != STATUS_GOOD) return privStatus;
  }

  return STATUS_GOOD;
}
//************* End of PrivMgrComponentPrivileges::grantPrivilege **************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::grantPrivilegeInternal              *
// *                                                                           *
// *    Internal function to grant one or more component privileges.           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentUID>                  const int64_t                   In       *
// *    is the unique ID associated with the component.                        *
// *                                                                           *
// *  <operationCodes>                const std::vector<std::string> & In      *
// *    is a list of component operation codes for the operation to be granted.*
// *                                                                           *
// *  <grantorID>                     const int32_t                    In      *
// *    is the auth ID granting the privilege.                                 *
// *                                                                           *
// *  <grantorName>                   const std::string &              In      *
// *    is the name of the authID granting the privilege.                      *
// *                                                                           *
// *  <granteeID>                     const int32_t                    In      *
// *    is the the authID being granted the privilege.                         *
// *                                                                           *
// *  <granteeName>                   const std::string &              In      *
// *    is the name of the authID being granted the privilege.                 *
// *                                                                           *
// *  <grantDepth>                    const int32_t                    In      *
// *    is the number of levels this privilege may be granted by the grantee.  *
// *  Initially this is either 0 or -1.                                        *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Component privilege(s) were granted                          *
// *           *: One or more component privileges were not granted.           *
// *              A CLI error is put into the diags area.                      *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrComponentPrivileges::grantPrivilegeInternal(const int64_t componentUID,
                                                              const std::vector<std::string> &operationCodes,
                                                              const int32_t grantorID, const std::string &grantorName,
                                                              const int32_t granteeID, const std::string &granteeName,
                                                              const int32_t grantDepth, const bool checkExistence)

{
  MyTable &myTable = static_cast<MyTable &>(myTable_);
  MyRow row(fullTableName_);

  row.componentUID_ = componentUID;
  row.grantDepth_ = grantDepth;
  row.granteeID_ = granteeID;
  row.granteeName_ = granteeName;
  row.grantorID_ = grantorID;
  row.grantorName_ = grantorName;

  const std::string componentUIDString = to_string((long long int)componentUID);

  for (size_t oc = 0; oc < operationCodes.size(); oc++) {
    row.operationCode_ = operationCodes[oc];

    int32_t granteeIDFromMD = NA_UserIdDefault;
    if (checkExistence && grantExists(componentUIDString, row.operationCode_, row.grantorID_, row.granteeName_,
                                      granteeIDFromMD, row.grantDepth_))
      continue;

    PrivStatus privStatus = myTable.insert(row);

    if (privStatus != STATUS_GOOD) return privStatus;
  }

  return STATUS_GOOD;
}
//********* End of PrivMgrComponentPrivileges::grantPrivilegeInternal **********

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::grantPrivilegeToCreator             *
// *                                                                           *
// *    Grants privilege on operation to creator from _SYSTEM.                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentUID>                  const int64_t                   In       *
// *    is the unique ID associated with the component.                        *
// *                                                                           *
// *  <operationCode>                 const std::string &             In       *
// *    is the two character code associated with the component operation.     *
// *                                                                           *
// *  <granteeID>                    const int32_t                    In       *
// *    is the auth ID of the creator of the component operation.              *
// *                                                                           *
// *  <granteeName>                   const std::string &             In       *
// *    is the name of the creator of the component operation.                 *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Component operation was granted to the auth ID by _SYSTEM.   *
// *           *: Component operation was not granted due to I/O error.        *
// *              A CLI error is put into the diags area.                      *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrComponentPrivileges::grantPrivilegeToCreator(const int64_t componentUID,
                                                               const std::string &operationCode,
                                                               const int32_t granteeID, const std::string &granteeName)

{
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  MyRow row(fullTableName_);

  row.componentUID_ = componentUID;
  row.operationCode_ = operationCode;
  row.granteeID_ = granteeID;
  row.granteeName_ = granteeName;
  row.grantorID_ = SYSTEM_USER;
  row.grantorName_ = SYSTEM_AUTH_NAME;
  row.grantDepth_ = -1;

  return myTable.insert(row);
}
//************** End of PrivMgrRoles::grantPrivilegeToCreator ******************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::hasPriv                             *
// *                                                                           *
// *    Determines if an authID has been granted the privilege on the          *
// * specified component operation.                                            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authID>                        const int32_t                   In       *
// *    is the authorization ID.                                               *
// *                                                                           *
// *  <componentName>                 const std::string &             In       *
// *    is the name of the component.                                          *
// *                                                                           *
// *  <operationName>                 const std::string &             In       *
// *    is the name of the operation.                                          *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: This authorization ID has the component privilege.                  *
// * false: This authorization ID does not have the component privilege, or    *
// *   there was an error trying to read from the COMPONENT_PRIVILEGES table.  *
// *                                                                           *
// *****************************************************************************
bool PrivMgrComponentPrivileges::hasPriv(const int32_t authID, const std::string &componentName,
                                         const std::string &operationName)

{
  // Determine if the component exists.

  PrivMgrComponents component(metadataLocation_, pDiags_);

  if (!component.exists(componentName)) {
    *pDiags_ << DgSqlCode(-CAT_TABLE_DOES_NOT_EXIST_ERROR) << DgTableName(componentName.c_str());
    return STATUS_ERROR;
  }

  std::string componentUIDString;
  int64_t componentUID;
  bool isSystemComponent;
  std::string tempStr;

  component.fetchByName(componentName, componentUIDString, componentUID, isSystemComponent, tempStr);

  // OK, the component is defined, what about the operation?

  PrivMgrComponentOperations componentOperations(metadataLocation_, pDiags_);

  if (!componentOperations.nameExists(componentUID, operationName)) {
    *pDiags_ << DgSqlCode(-CAT_TABLE_DOES_NOT_EXIST_ERROR) << DgTableName(operationName.c_str());
    return STATUS_ERROR;
  }

  std::string operationCode;
  bool isSystemOperation = FALSE;
  std::string operationDescription;

  componentOperations.fetchByName(componentUIDString, operationName, operationCode, isSystemOperation,
                                  operationDescription);

  std::string whereClause(" WHERE COMPONENT_UID = ");

  whereClause += componentUIDString;
  whereClause += " AND OPERATION_CODE = '";
  whereClause += operationCode;
  whereClause += "' AND GRANTEE_ID = ";
  whereClause += authIDToString(authID);

  int64_t rowCount = 0;
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  PrivStatus privStatus = myTable.selectCountWhere(whereClause, rowCount);

  if ((privStatus == STATUS_GOOD || privStatus == STATUS_WARNING) && rowCount > 0) return true;

  pDiags_->rewind(diagsMark);

  return false;
}
//***************** End of PrivMgrComponentPrivileges::hasPriv *****************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::hasSQLPriv                          *
// *                                                                           *
// *    Determines if an authID has been granted the privilege on the          *
// * SQL_OPERATIONS component operation.                                       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authID>                        const int32_t                   In       *
// *    is the authorization ID.                                               *
// *                                                                           *
// *  <operation>                     const SQLOperation              In       *
// *    is the enum for the SQL operation.                                     *
// *                                                                           *
// *  <includeRoles>                  const bool                      In       *
// *    if true, indicates privileges granted to roles granted to the          *
// * authorization ID should be included.                                      *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: This authorization ID has the component privilege.                  *
// * false: This authorization ID does not have the component privilege, or    *
// *   there was an error trying to read from the COMPONENT_PRIVILEGES table.  *
// *                                                                           *
// *****************************************************************************
bool PrivMgrComponentPrivileges::hasSQLPriv(const int32_t authID, const SQLOperation operation, const bool includeRoles)

{
  if (authID == ComUser::getRootUserID()) return true;

  int32_t length;
  char authName[MAX_DBUSERNAME_LEN + 1];

  // If authID not found in metadata, ComDiags is populated with
  // error 8732: Authorization ID <authID> is not a registered user or role
  Int16 retCode = ComUser::getAuthNameFromAuthID(authID, authName, sizeof(authName), length, FALSE, pDiags_);
  if (retCode == -1) return false;

  const std::string &operationCode = PrivMgr::getSQLOperationCode(operation);

  std::string whereClause(" WHERE COMPONENT_UID = 1 AND (OPERATION_CODE = '");

  whereClause += operationCode;

  if (PrivMgr::isSQLCreateOperation(operation)) {
    whereClause += "' OR OPERATION_CODE = '";
    whereClause += PrivMgr::getSQLOperationCode(SQLOperation::CREATE);
  } else if (PrivMgr::isSQLDropOperation(operation)) {
    whereClause += "' OR OPERATION_CODE = '";
    whereClause += PrivMgr::getSQLOperationCode(SQLOperation::DROP);
  } else if (PrivMgr::isSQLAlterOperation(operation)) {
    whereClause += "' OR OPERATION_CODE = '";
    whereClause += PrivMgr::getSQLOperationCode(SQLOperation::ALTER);
  } else if (PrivMgr::isSQLManageOperation(operation)) {
    whereClause += "' OR OPERATION_CODE = '";
    whereClause += PrivMgr::getSQLOperationCode(SQLOperation::MANAGE);
  }

  char buf[MAX_DBUSERNAME_LEN + 10 + 100];
  int stmtSize = snprintf(buf, sizeof(buf),
                            "') AND (GRANTEE_ID = -1 OR "
                            "(GRANTEE_ID = %d AND GRANTEE_NAME = '%s')",
                            authID, authName);
  whereClause += buf;

  // *****************************************************************************
  // *                                                                           *
  // *   If component privileges granted to roles granted to the authorization   *
  // * ID should be considered, get the list of roles granted to the auth ID     *
  // * and add each one as a potential grantee.                                  *
  // *                                                                           *
  // *****************************************************************************

  if (includeRoles) {
    std::vector<std::string> roleNames;
    std::vector<int32_t> roleIDs;
    std::vector<int32_t> grantDepths;
    std::vector<int32_t> grantees;

    PrivMgrRoles roles(" ", metadataLocation_, pDiags_);

    PrivStatus privStatus = roles.fetchRolesForAuth(authID, roleNames, roleIDs, grantDepths, grantees);

    for (size_t r = 0; r < roleIDs.size(); r++) {
      stmtSize = snprintf(buf, sizeof(buf),
                          " OR (GRANTEE_ID = %d AND "
                          "GRANTEE_NAME = '%s')",
                          roleIDs[r], roleNames[r].c_str());
      whereClause += buf;
    }
  }

  whereClause += ")";

  int64_t rowCount = 0;
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  // set pointer in diags area
  int32_t diagsMark = (pDiags_ != NULL ? pDiags_->mark() : -1);

  PrivStatus privStatus = myTable.selectCountWhere(whereClause, rowCount);

  if ((privStatus == STATUS_GOOD || privStatus == STATUS_WARNING) && rowCount > 0) return true;

  if (diagsMark != -1) pDiags_->rewind(diagsMark);

  return false;
}
//*************** End of PrivMgrComponentPrivileges::hasSQLPriv ****************

// *****************************************************************************
// method:  hasWGO
//
// See if grantor has the WGO based on the list of grants (rows) or
// with MANAGE_COMPONENT privilege
// retcode:   0 - no WGO privilege
//            1 - grantor has WGO privilege
//            2 - has WGO privilege through MANAGE COMPONENTS privilege
//            3 - one of grantor's roles has WGO privilege
//
// *****************************************************************************
int32_t MyTable::hasWGO(int32_t authID, const std::vector<int32_t> &roleIDs, const std::vector<MyRow> &rows,
                        PrivMgrComponentPrivileges *compPrivs, std::string &roleName) {
  // Check the list of rows assigned to this component and operation code
  // to see if the authID has WGO
  for (size_t i = 0; i < rows.size(); i++) {
    const MyRow &row = static_cast<const MyRow &>(rows[i]);
    if (row.grantDepth_ == 0) continue;

    if (row.granteeID_ == authID) return 1;
  }

  // see if authID has WGO based on MANAGE_COMPONENTS privilege
  bool hasWGO = compPrivs->hasSQLPriv(authID, SQLOperation::MANAGE_COMPONENTS, true);
  if (hasWGO) return 2;

  // Repeat looking for rows owned by one of the user's roles
  for (size_t i = 0; i < rows.size(); i++) {
    const MyRow &row = static_cast<const MyRow &>(rows[i]);
    if (row.grantDepth_ == 0) continue;

    if (std::find(roleIDs.begin(), roleIDs.end(), row.granteeID_) != roleIDs.end()) {
      roleName = row.granteeName_;
      return 3;
    }
  }

  return 0;
}

// -----------------------------------------------------------------------------
// method: hasSQLPriv
//
// Reads metadata (COMPONENT_PRIVILEGES) to see if authID has been granted one
// of the component privileges in the operationList.
//
// returns:
//   hasPriv with the result
//
// Error:
//   <0 - unexpected error
//    0 - operation successful
//------------------------------------------------------------------------------
short PrivMgrComponentPrivileges::hasSQLPriv(const int32_t authID, const NAList<SQLOperation> &operationList,
                                             bool &hasPriv) {
  hasPriv = false;

  if (authID == ComUser::getRootUserID()) {
    hasPriv = true;
    return 0;
  }

  // Set up WHERE CLAUSE
  std::string whereClause("WHERE COMPONENT_UID = 1 AND (OPERATION_CODE IN (");
  for (CollIndex i = 0; i < operationList.entries(); i++) {
    SQLOperation operation = operationList[i];
    const std::string &operationCode = PrivMgr::getSQLOperationCode(operation);

    whereClause += (i > 0) ? ", '" : "'";
    whereClause += operationCode;
    whereClause += "'";

    if (PrivMgr::isSQLCreateOperation(operation)) {
      whereClause += ", '";
      whereClause += PrivMgr::getSQLOperationCode(SQLOperation::CREATE);
      whereClause += "'";
    } else if (PrivMgr::isSQLDropOperation(operation)) {
      whereClause += ", '";
      whereClause += PrivMgr::getSQLOperationCode(SQLOperation::DROP);
      whereClause += "'";
    } else if (PrivMgr::isSQLAlterOperation(operation)) {
      whereClause += ", '";
      whereClause += PrivMgr::getSQLOperationCode(SQLOperation::ALTER);
      whereClause += "'";
    } else if (PrivMgr::isSQLManageOperation(operation)) {
      whereClause += ", '";
      whereClause += PrivMgr::getSQLOperationCode(SQLOperation::MANAGE);
      whereClause += "'";
    }
  }

  // Add list of grantees to search for:  PUBLIC, authID, and authID's roles
  whereClause += ")) and (grantee_id in (-1, ";
  whereClause += authIDToString(authID);

  // Add roles
  if (authID == ComUser::getCurrentUser()) {
    // Most of the time, this method is called for the current user, in this
    // case get role list from cache to avoid extra I/Os
    NAList<int> roleIDs(STMTHEAP);
    if (ComUser::getCurrentUserRoles(roleIDs, pDiags_) != 0) {
      std::string msg("Unable to retrieve roles for: ");
      msg += ComUser::getCurrentUsername();
      PRIVMGR_INTERNAL_ERROR(msg.c_str());
      return -1;
    }

    for (CollIndex r = 0; r < roleIDs.entries(); r++) {
      whereClause += ", ";
      whereClause += authIDToString(roleIDs[r]);
    }
  } else {
    // Read metadata to retrieve role information
    PrivMgrRoles roles(trafMetadataLocation_, metadataLocation_, pDiags_);
    std::vector<std::string> roleNames;
    std::vector<int32_t> roleIDs;
    std::vector<int32_t> grantDepths;
    std::vector<int32_t> grantees;
    PrivStatus privStatus = roles.fetchRolesForAuth(authID, roleNames, roleIDs, grantDepths, grantees);

    for (size_t r = 0; r < roleIDs.size(); r++) {
      whereClause += ", ";
      whereClause += authIDToString(roleIDs[r]);
    }
  }
  whereClause += "))";

  int64_t rowCount = 0;
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  // set pointer in diags area
  int32_t diagsMark = (pDiags_ != NULL ? pDiags_->mark() : -1);

  PrivStatus privStatus = myTable.selectCountWhere(whereClause, rowCount);

  if (privStatus == STATUS_ERROR) return -1;

  if (rowCount > 0) hasPriv = true;

  if (diagsMark != -1) pDiags_->rewind(diagsMark);

  return 0;
}

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::hasWGO                              *
// *                                                                           *
// *    Determines if an authID has been granted the ability to grant a        *
// * specific component operation.                                             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authID>                        const int32_t                   In       *
// *    is the authorization ID.                                               *
// *                                                                           *
// *  <componentUIDString>            const std::string &             In       *
// *    is the unique ID of the component in string format.                    *
// *                                                                           *
// *  <operationCode>                 const std::string &             In       *
// *    is the two character code for the component operation.                 *
// *                                                                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: This authorization ID has with grant option.                        *
// * false: This authorization ID does not have with grant option, or there    *
// *        was an error trying to read from the ROLE_USAGE table.             *
// *                                                                           *
// *****************************************************************************
bool PrivMgrComponentPrivileges::hasWGO(int32_t authID, const std::string &componentUIDString,
                                        const std::string &operationCode)

{
  // get roles granted to authID
  std::vector<std::string> roleNames;
  std::vector<int32_t> roleIDs;
  std::vector<int32_t> grantDepths;
  std::vector<int32_t> grantees;

  PrivMgrRoles roles(" ", metadataLocation_, pDiags_);

  if (roles.fetchRolesForAuth(authID, roleNames, roleIDs, grantDepths, grantees) == STATUS_ERROR) return false;

  MyTable &myTable = static_cast<MyTable &>(myTable_);

  std::string granteeList;
  granteeList += authIDToString(authID);
  for (size_t i = 0; i < roleIDs.size(); i++) {
    granteeList += ", ";
    granteeList += authIDToString(roleIDs[i]);
  }

  // DB__ROOTROLE is a special role.  If the authID has been granted this role
  // then they have WGO privileges.
  if (std::find(roleIDs.begin(), roleIDs.end(), ROOT_ROLE_ID) != roleIDs.end()) return true;

  std::string whereClause(" WHERE GRANTEE_ID IN (");
  whereClause += granteeList;
  whereClause += ") AND COMPONENT_UID = ";
  whereClause += componentUIDString;
  whereClause += " AND OPERATION_CODE = '";
  whereClause += operationCode;
  whereClause += "' AND GRANT_DEPTH <> 0";

  int64_t rowCount = 0;

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  PrivStatus privStatus = myTable.selectCountWhere(whereClause, rowCount);

  if ((privStatus == STATUS_GOOD || privStatus == STATUS_WARNING) && rowCount > 0) return true;

  pDiags_->rewind(diagsMark);

  // If user has component privilege MANAGE_COMPONENTS, then allow
  if (hasSQLPriv(authID, SQLOperation::MANAGE_COMPONENTS, true)) return true;
  return false;
}
//***************** End of PrivMgrComponentPrivileges::hasWGO ******************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::isAuthIDGrantedPrivs                *
// *                                                                           *
// *    Determines if the specified authorization ID has been granted one or   *
// * more component privileges.                                                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <authID>                        const int32_t                   In       *
// *    is the authorization ID.                                               *
// *                                                                           *
// *  <checkName>                     const bool                      In       *
// *    check both authID and authName matches, defaults to false              *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: Authorization ID has been granted one or more component privileges. *
// * false: Authorization ID has not been granted any component privileges.    *
// *                                                                           *
// *****************************************************************************
bool PrivMgrComponentPrivileges::isAuthIDGrantedPrivs(const int32_t authID, const bool checkName)

{
  std::string whereClause;
  char queryBuf[MAX_DBUSERNAME_LEN + 100];
  int stmtSize = snprintf(queryBuf, sizeof(queryBuf), " WHERE GRANTEE_ID = %d ", authID);
  whereClause += queryBuf;
  if (checkName) {
    int32_t length;
    char authName[MAX_DBUSERNAME_LEN + 1];

    // If authID not found in metadata, ComDiags is populated with
    // error 8732: Authorization ID <authID> is not a registered user or role
    Int16 retCode = ComUser::getAuthNameFromAuthID(authID, authName, sizeof(authName), length, FALSE, pDiags_);
    stmtSize = snprintf(queryBuf, sizeof(queryBuf), " AND GRANTEE_NAME = '%s'", authName);
    whereClause += queryBuf;
  }

  int64_t rowCount = 0;
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  PrivStatus privStatus = myTable.selectCountWhere(whereClause, rowCount);

  if ((privStatus == STATUS_GOOD || privStatus == STATUS_WARNING) && rowCount > 0) return true;

  pDiags_->rewind(diagsMark);

  return false;
}
//*********** End of PrivMgrComponentPrivileges::isAuthIDGrantedPrivs **********

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::isGranted                           *
// *                                                                           *
// *    Determines if a component operation has been granted, i.e., if it      *
// * is used in the COMPONENT_PRIVILEGES table.                                *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentUIDString>            const std::string &             In       *
// *    is the component unique ID.                                            *
// *                                                                           *
// *  <operationCode>                 const std::string &             In       *
// *    is the two character code associated with the component operation.     *
// *                                                                           *
// *  <shouldExcludeGrantsBySystem>   const bool                      [In]     *
// *    if true, don't consider the system grant to the creator.               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: Component operation has been granted to one or more authIDs.        *
// * false: Component operation has not been granted, or there was an error    *
// * trying to read from the COMPONENT_PRIVILEGES table.                       *
// *                                                                           *
// *****************************************************************************
bool PrivMgrComponentPrivileges::isGranted(const std::string &componentUIDString, const std::string &operationCode,
                                           const bool shouldExcludeGrantsBySystem)

{
  MyRow row(fullTableName_);
  MyTable &myTable = static_cast<MyTable &>(myTable_);

  std::string whereClause("WHERE COMPONENT_UID = ");

  whereClause += componentUIDString;
  whereClause += " AND OPERATION_CODE = '";
  whereClause += operationCode;
  whereClause += "'";
  if (shouldExcludeGrantsBySystem) whereClause += " AND GRANTOR_ID <> -2";

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  PrivStatus privStatus = myTable.selectWhereUnique(whereClause, row);

  if (privStatus == STATUS_GOOD || privStatus == STATUS_WARNING) return true;

  pDiags_->rewind(diagsMark);

  return false;
}
//**************** End of PrivMgrComponentPrivileges::isGranted ****************

#if 0
// *****************************************************************************
// * Function: PrivMgrComponentPrivileges::revokeAllForGrantor                 *
// *                                                                           *
// *    Revokes grants from a specified grantor.                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Parameters:                                                               *
// *                                                                           *
// *  <grantorID>                     const int32_t                    In      *
// *    is the unique ID of the authID whose grants are to be revoked.         *
// *                                                                           *
// *  <roleID>                        const int32_t                    In      *
// *    is the ID of the role to be revoked.                                   *
// *                                                                           *
// *  <isGOFSpecified>             const bool                          In      *
// *    is true if admin rights are being revoked.                             *
// *                                                                           *
// *  <newGrantDepth>              const int32_t                       In      *
// *    is the number of levels this privilege may be granted by the grantee.  *
// *  Initially this is always 0 when revoking.                                *
// *                                                                           *
// *  <dropBehavior>                  PrivDropBehavior                 In      *
// *    indicates whether restrict or cascade behavior is requested.           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: All grants of the role by this grantor were revoked (or      *
// *              there were none).                                            *
// *           *: Could not revoke grants.  A CLI error is put into            *
// *              the diags area.                                              *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrComponentPrivileges::revokeAllForGrantor(
   const int32_t grantorID,
   const std::string componentName,
   const std::string componentUIDString,
   const std::string operationName,
   const std::string operationCode,
   const bool isGOFSpecified,
   const int32_t newGrantDepth,
   PrivDropBehavior dropBehavior)
{

// *****************************************************************************
// *                                                                           *
// *  First, get all the rows where the grantor has granted this operation     *
// * to any authorization ID.                                                  *
// *                                                                           *
// *****************************************************************************

std::string whereClause("WHERE COMPONENT_UID = ");

   whereClause += componentUIDString;
   whereClause += " AND OPERATION_CODE = '";
   whereClause += operationCode;
   whereClause += "' AND GRANTOR_ID = ";
   whereClause += authIDToString(grantorID);

std::string orderByClause;

std::vector<MyRow> rows;
MyTable &myTable = static_cast<MyTable &>(myTable_);

PrivStatus privStatus = myTable.selectAllWhere(whereClause,orderByClause,rows);

// *****************************************************************************
// *                                                                           *
// *   If the grantor has no active grants for this operation, we are done.    *
// *                                                                           *
// *****************************************************************************

   if (privStatus == STATUS_NOTFOUND)
      return STATUS_GOOD;

// *****************************************************************************
// *                                                                           *
// *   Unexpected problem, let the caller deal with it.                        *
// *                                                                           *
// *****************************************************************************

   if (privStatus != STATUS_GOOD && privStatus != STATUS_WARNING)
      return privStatus;

// *****************************************************************************
// *                                                                           *
// *   Expected NOTFOUND, but if empty list returned, return no error.         *
// *                                                                           *
// *****************************************************************************

   if (rows.size() == 0)
      return STATUS_GOOD;

// *****************************************************************************
// *                                                                           *
// *   If there are grants and drop behavior is RESTRICT, return an error.     *
// *                                                                           *
// *****************************************************************************

   if (dropBehavior == PrivDropBehavior::RESTRICT)
   {
      //TODO: Need better error message. 
      *pDiags_ << DgSqlCode(-CAT_DEPENDENT_OBJECTS_EXIST);
      return STATUS_ERROR;
   }

// *****************************************************************************
// *                                                                           *
// *   There are one more grants from the grantor of this operation.  Create a *
// * vector for the operationCode and call revokePrivilege.                    *
// *                                                                           *
// *****************************************************************************

std::vector<std::string> operationNames;

   operationNames.push_back(operationName);

   for (size_t r = 0; r < rows.size(); r++)
   {
      privStatus = revokePrivilege(componentName,operationNames,grantorID,
                                   rows[r].granteeName_,isGOFSpecified,
                                   newGrantDepth,dropBehavior);
      if (privStatus != STATUS_GOOD)
         return privStatus;
   }

   return STATUS_GOOD;

}
//****************** End of PrivMgrRoles::revokeAllForGrantor ******************

#endif

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::revokePrivilege                     *
// *                                                                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentName>                 const std::string &              In      *
// *    is the component name.                                                 *
// *                                                                           *
// *  <operations>                    const std::vector<std::string> & In      *
// *    is a list of component operations to be revoked.                       *
// *                                                                           *
// *  <grantorID>                     const int32_t                    In      *
// *    is the authID revoking the privilege.                                  *
// *                                                                           *
// *  <granteeName>                   const std::string &              In      *
// *    is the authname the privilege is being revoked from.                   *
// *                                                                           *
// *  <isGOFSpecified>             const bool                          In      *
// *    is true if admin rights are being revoked.                             *
// *                                                                           *
// *  <newGrantDepth>              const int32_t                       In      *
// *    is the number of levels this privilege may be granted by the grantee.  *
// *  Initially this is always 0 when revoking.                                *
// *                                                                           *
// *  <dropBehavior>                  PrivDropBehavior                 In      *
// *    indicates whether restrict or cascade behavior is requested.           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Component privilege(s) were revoked                          *
// *           *: One or more component privileges were not revoked.           *
// *              A CLI error is put into the diags area.                      *
// *                                                                           *
// *****************************************************************************
PrivStatus PrivMgrComponentPrivileges::revokePrivilege(const std::string &componentName,
                                                       const std::vector<std::string> &operations,
                                                       const int32_t grantorIDIn, const std::string &granteeName,
                                                       const bool isGOFSpecified, const int32_t newGrantDepth,
                                                       PrivDropBehavior dropBehavior)

{
  // Determine if the component exists.
  PrivMgrComponents component(metadataLocation_, pDiags_);

  std::string componentUIDString;
  int64_t componentUID;
  bool isSystemComponent;
  std::string componentDescription;

  if (component.fetchByName(componentName, componentUIDString, componentUID, isSystemComponent, componentDescription) !=
      STATUS_GOOD) {
    *pDiags_ << DgSqlCode(-CAT_OBJECT_DOES_NOT_EXIST_IN_TRAFODION) << DgString0(componentName.c_str());
    return STATUS_ERROR;
  }

  // Get roles assigned to the user
  PrivMgrRoles roles;
  std::vector<int32_t> roleIDs;

  // If the current user matches the grantorID, then get cached roles
  if (ComUser::getCurrentUser() == grantorIDIn) {
    NAList<int32_t> cachedRoleIDs(STMTHEAP);
    if (ComUser::getCurrentUserRoles(cachedRoleIDs, pDiags_) == -1) return STATUS_ERROR;

    // convert to std:string
    for (int32_t i = 0; i < cachedRoleIDs.entries(); i++) roleIDs.push_back(cachedRoleIDs[i]);
  } else {
    // get roles granted to grantorID by reading metadata, etc.
    std::vector<std::string> roleNames;
    std::vector<int32_t> grantDepths;
    std::vector<int32_t> grantees;
    if (roles.fetchRolesForAuth(grantorIDIn, roleNames, roleIDs, grantDepths, grantees) == STATUS_ERROR)
      return STATUS_ERROR;
  }

  // OK, the component is defined, what about the operations?
  PrivMgrComponentOperations componentOperations(metadataLocation_, pDiags_);
  std::vector<std::string> operationCodes;
  bool sendToRMS = false;

  for (size_t i = 0; i < operations.size(); i++) {
    std::string operationName = operations[i];
    std::string operationCode;
    bool isSystemOperation = FALSE;
    std::string operationDescription;
    if (componentOperations.fetchByName(componentUIDString, operationName, operationCode, isSystemOperation,
                                        operationDescription) != STATUS_GOOD) {
      *pDiags_ << DgSqlCode(-CAT_INVALID_COMPONENT_PRIVILEGE) << DgString0(operationName.c_str())
               << DgString1(componentName.c_str());
      return STATUS_ERROR;
    }

    // Component and this operation both exist. Save operation code.
    operationCodes.push_back(operationCode);

    // If revoking a MANAGE or DML_SELECT_METADATA privilege, send a schema security invalidation key.
    if (isSQLManageOperation(operationCode.c_str()) || operationCode == std::string("PM")) sendToRMS = true;
  }

  PrivStatus privStatus = STATUS_GOOD;
  int32_t grantorID = grantorIDIn;
  int32_t granteeID = NA_UserIdDefault;
  std::vector<int32_t> processedIDs;
  processedIDs.push_back(grantorID);

  // Check validity of each revoke operation per operation code
  // In most cases, there will only be one operation code specified
  for (size_t oc = 0; oc < operationCodes.size(); oc++) {
    int32_t grantDepth;

    // get rows for component and operation
    std::string whereClause("WHERE COMPONENT_UID = ");
    whereClause += componentUIDString;
    whereClause += " AND OPERATION_CODE = '";
    whereClause += operationCodes[oc];
    whereClause += "'";
    std::string orderByClause = "ORDER BY GRANTOR_ID, GRANTEE_ID";

    std::vector<MyRow> rows;
    MyTable &myTable = static_cast<MyTable &>(myTable_);
    if (myTable.selectAllWhere(whereClause, orderByClause, rows) == STATUS_ERROR) return STATUS_ERROR;

    // See if grantor has the WGO based on the list of grants (rows) or
    // with MANAGE_COMPONENT privilege
    // retcode: 0 - no WGO privilege
    //          1 - grantor has WGO privilege
    //          2 - has WGO privilege through MANAGE COMPONENTS privilege
    //          3 - one of grantor's roles has WGO privilege
    std::string roleName;
    int32_t retcode = myTable.hasWGO(grantorID, roleIDs, rows, this, roleName);

    // If current user does not have WGO, return an error
    if (retcode == 0) {
      *pDiags_ << DgSqlCode(-CAT_NOT_AUTHORIZED);
      return STATUS_ERROR;
    }

    // current user has privilege based on MANAGE_COMPONENT privilege
    // change grantorID/grantorName to operation owner
    if (retcode == 2) {
      // The first row in rows should be the operation owner.
      MyRow grantRow(fullTableName_);
      if (myTable.getOwnerRow(operationCodes[oc], rows, grantRow) == false) {
        PRIVMGR_INTERNAL_ERROR("failed to find operation owner");
        return STATUS_ERROR;
      }
      grantorID = grantRow.granteeID_;
    }

    // Current user has WGO through a role, a BY CLAUSE is required  to specify the role
    if (retcode == 3) {
      int32_t length;
      char grantorName[MAX_DBUSERNAME_LEN + 1];

      // Should not fail, grantor ID was derived from name provided by user.
      // But if grantorName not found in metadata, ComDiags is populated with
      // error 8732: Authorization ID <grantorID> is not a registered user or role
      Int16 retCode = ComUser::getAuthNameFromAuthID(grantorID, grantorName, sizeof(grantorName), length, FALSE);
      if (retCode != 0) return STATUS_ERROR;

      std::string msg("Please retry using the BY CLAUSE for role: ");
      msg += roleName;
      *pDiags_ << DgSqlCode(-CAT_PRIVILEGE_NOT_REVOKED) << DgString0(grantorName) << DgString1(msg.c_str());
      return STATUS_ERROR;
    }

    // If row is not found, the componentUID_ is 0
    MyRow row = myTable.findGrantRow(componentUID, grantorID, granteeName, operationCodes[oc], rows);
    if (row.componentUID_ == 0) {
      int32_t length;
      char grantorName[MAX_DBUSERNAME_LEN + 1];

      // Should not fail, grantor ID was derived from name provided by user.
      // But if grantorName not found in metadata, ComDiags is populated with
      // error 8732: Authorization ID <grantorID> is not a registered user or role
      Int16 retCode = ComUser::getAuthNameFromAuthID(grantorID, grantorName, sizeof(grantorName), length, FALSE);
      if (retCode != 0) return STATUS_ERROR;

      std::string operationOnComponent(operations[oc]);

      operationOnComponent += " on component ";
      operationOnComponent += componentName;

      *pDiags_ << DgSqlCode(-CAT_GRANT_NOT_FOUND) << DgString0(operationOnComponent.c_str()) << DgString1(grantorName)
               << DgString2(granteeName.c_str());
      return STATUS_ERROR;
    }
    granteeID = row.granteeID_;

    if (dropBehavior == PrivDropBehavior::RESTRICT) {
      if (myTable.dependentGrantsExist(grantorID, granteeID, isGOFSpecified, rows) == STATUS_ERROR) return STATUS_ERROR;
    }

#if 0
// TDB: this code gets called for CASCADE processing.  It needs to be updated
      else
      {
         // this code is needed to handle CASCADE behavior, comment out for now
         privStatus = revokeAllForGrantor(granteeID,
                                          componentName,
                                          componentUIDString,
                                          operations[oc],
                                          operationCodes[oc],
                                          isGOFSpecified,
                                          newGrantDepth,
                                          dropBehavior);
         if (privStatus != STATUS_GOOD)
         {
            return STATUS_ERROR;
         }

         if (isSQLDMLPriv(componentUID,operations[oc]))
         {   //TODO: QI only supports revoke from objects and users (roles)
            // Notify QI
         }
      }
#endif
  }

  MyTable &myTable = static_cast<MyTable &>(myTable_);

  std::string whereClauseHeader(" WHERE COMPONENT_UID = ");

  whereClauseHeader += componentUIDString;
  whereClauseHeader += " AND GRANTOR_ID = ";
  whereClauseHeader += authIDToString(grantorID);
  ;
  whereClauseHeader += " AND GRANTEE_ID = ";
  whereClauseHeader += authIDToString(granteeID);
  ;
  whereClauseHeader += " AND OPERATION_CODE = '";

  std::string setClause("SET GRANT_DEPTH = ");

  if (isGOFSpecified) {
    char grantDepthString[20];

    sprintf(grantDepthString, "%d", newGrantDepth);
    setClause += grantDepthString;
  }

  bool someNotRevoked = false;

  for (size_t c = 0; c < operationCodes.size(); c++) {
    std::string whereClause(whereClauseHeader);

    whereClause += operationCodes[c];
    whereClause += "'";

    if (isGOFSpecified) {
      std::string updateClause = setClause + whereClause;

      privStatus = myTable.update(updateClause);
    } else
      privStatus = myTable.deleteWhere(whereClause);

    if (privStatus == STATUS_NOTFOUND) {
      someNotRevoked = true;
      continue;
    }

    if (privStatus != STATUS_GOOD) return privStatus;
  }

  // Send a message to the Trafodion RMS process about the revoke operation.
  // RMS will contact all master executors and ask that cached privilege
  // information be re-calculated
  if (sendToRMS) {
    privStatus = sendSecurityKeysToRMS(granteeID);
    if (privStatus != STATUS_GOOD) {
      PRIVMGR_INTERNAL_ERROR("failed to generate security keys");
      return STATUS_ERROR;
    }
  }

  if (someNotRevoked) {
    *pDiags_ << DgSqlCode(CAT_NOT_ALL_PRIVILEGES_REVOKED);
    return STATUS_WARNING;
  }

  return STATUS_GOOD;
}
//************* End of PrivMgrComponentPrivileges::revokePrivilege *************

// ****************************************************************************
//  method:  dependentGrantsExist
//
//  This method traverses the grant tree that includes the revoke change.  If
//  a part of the tree is no longer available than revoke fails with a dependent
//  grant error.
//
//  Based on code found in PrivMgrPrivileges - checkRevokeRestrict.
//
//  For example:
//     system -> root
//     root   -> user1
//     root   -> user2
//     user1  -> user2
//     user1  -> user3
//     user2  -> user4
//     user3  -> user5
//
//  If user1 revokes the privilege from user2, it succeeds.
//    (user2 gets privilege from both root and user1, removing one path is okay)
//  If user1 revokes privilege from user3, it fails because of user3 -> user5 grant
//    (user3 get privilege only from user1, cannot revoke until the user3 -> user
//     grant is removed)
//
//  grantorID and granteeID identifies the revoke target
//  onlyWGO indicates that only WGO is being removed
//  rows contains a list of grantors/grantees for a specific component UID
//    and operation code ordered by grantorID/granteeID.
//
// returns true if dependent grants exist
// ****************************************************************************
bool MyTable::dependentGrantsExist(const int32_t grantorID, const int32_t granteeID, const bool onlyWGO,
                                   std::vector<MyRow> &rows) {
  // Adjust the row affected by the revoke, either the privilege is revoked
  // (set deleted_ to true) or remove WGO (set grantDepth_ to 0)
  for (int32_t i = 0; i < rows.size(); i++) {
    MyRow &currentRow = static_cast<MyRow &>(rows[i]);
    currentRow.visited_ = false;
    currentRow.deleted_ = false;
    if (currentRow.grantorID_ == grantorID && currentRow.granteeID_ == granteeID) {
      if (onlyWGO)
        currentRow.grantDepth_ = 0;
      else
        currentRow.deleted_ = true;
      break;
    }
  }

  // Reconstruct the privilege tree applying the revoke change
  // The privilege tree starts with the system grantor (-2)
  int32_t systemGrantor = SYSTEM_USER;
  scanBranch(systemGrantor, rows);

  // If a branch of the tree was not visited, then we have a broken
  // tree.  Therefore, revoke restrict will leave abandoned privileges
  // in the case, return true.
  bool notVisited = false;
  for (size_t i = 0; i < rows.size(); i++) {
    MyRow &currentRow = static_cast<MyRow &>(rows[i]);

    if (!currentRow.visited_) {
      *pDiags_ << DgSqlCode(-CAT_DEPENDENT_PRIV_EXISTS) << DgString0(currentRow.grantorName_.c_str())
               << DgString1(currentRow.granteeName_.c_str());

      notVisited = true;
      break;
    }
  }
  return notVisited;
}

// *****************************************************************************
// method: scanBranch
//
//  Scans the rows that match the component and operation code keeping track of
//  which entries have been encountered by setting "visited" flag in the entry.
//
//  Call scanBranch recursively with the current grantee as grantor if the
//  grantee has WGO and the branch has not already been visited.
//  (By observing the state of the visited flag we avoid redundantly exploring
//   the sub-tree rooted in a grantor which has already been discovered as having
//   WGO from some other ancestor grantor.)
//
//  This algorithm produces a depth-first scan of all nodes of the directed graph
//  which can currently be reached by an uninterrupted chain of WGO values.
//
//  The implementation is dependent on the fact that the rows are ordered by
//  Grantor and Grantee.  Entries for system grantor (_SYSTEM) are the  first
//  entries in the list.
//
// *****************************************************************************
void MyTable::scanBranch(const int32_t &grantor, std::vector<MyRow> &rows) {
  size_t i = 0;

  // find the first row where grantor is greater or equal to the grantorID_ in row
  while (i < rows.size()) {
    MyRow &currentRow = static_cast<MyRow &>(rows[i]);
    if (currentRow.grantorID_ < grantor)
      i++;
    else
      break;
  }

  // For matching Grantors, process each Grantee.
  while (i < rows.size()) {
    MyRow &currentRow = static_cast<MyRow &>(rows[i]);
    if (currentRow.grantorID_ == grantor) {
      // If already looked at this branch, continue
      if (currentRow.visited_) continue;
      currentRow.visited_ = true;

      // skip if: row is being deleted or WGO is not specified
      // no more branches of the tree exist
      if (currentRow.deleted_ || currentRow.grantDepth_ == 0) {
        i++;
        continue;
      }

      // Search the next branch
      scanBranch(currentRow.granteeID_, rows);
    }

    // we are done
    else
      break;

    i++;
  }
}

// ****************************************************************************
// method: sendSecurityKeysToRMS
//
// This method generates a security key for any applicable  privilege revoked
// for this grantee.  It then makes a cli call sending the keys.
// SQL_EXEC_SetSecInvalidKeys will send the security keys to RMS and RMS
// sends then to all the master executors.  The master executors check this
// list and recompile any queries to recheck privileges.
//
// input:
//    granteeID - the UID of the user/role losing privileges
//
// Returns: PrivStatus
//
// STATUS_GOOD: Privileges were granted
//           *: Unable to send keys,  see diags.
// ****************************************************************************
PrivStatus PrivMgrComponentPrivileges::sendSecurityKeysToRMS(const int32_t granteeID) {
  // If the privilege is revoked from a role, then generate QI keys for
  // all the role's grantees
  NAList<int32_t> roleGrantees(STMTHEAP);
  if (PrivMgr::isRoleID(granteeID)) {
    std::vector<int32_t> roleIDs;
    roleIDs.push_back(granteeID);
    std::vector<int32_t> granteeIDs;
    if (getGranteeIDsForRoleIDs(roleIDs, granteeIDs, false) == STATUS_ERROR) return STATUS_ERROR;
    for (size_t i = 0; i < granteeIDs.size(); i++) roleGrantees.insert(granteeIDs[i]);
  } else
    roleGrantees.insert(ComUser::getCurrentUser());

  // Get the list of schema UIDs for metadata schemas
  std::string whereClause(" WHERE OBJECT_TYPE IN ('PS') AND SCHEMA_NAME in  (");
  whereClause += "'_MD_', '_PRIVMGR_MD_', '_TENANT_MD_')";

  PrivMgrObjects objects(trafMetadataLocation_, pDiags_);
  std::vector<UIDAndType> schemaUIDs;
  PrivStatus privStatus = objects.fetchUIDandTypes(whereClause, schemaUIDs);
  if (privStatus == STATUS_ERROR) return privStatus;

  // Build security keys for metadata schemas since the user lost
  // the ability to select from metadata
  PrivMgrCoreDesc revokedPrivs;
  revokedPrivs.setPriv(SELECT_PRIV, true);
  NASet<ComSecurityKey> keyList(NULL);
  for (size_t s = 0; s < schemaUIDs.size(); s++) {
    if (!buildSecurityKeys(roleGrantees, granteeID, schemaUIDs[s].UID, true, false, revokedPrivs, keyList)) {
      PRIVMGR_INTERNAL_ERROR("ComSecurityKey is null");
      return STATUS_ERROR;
    }
  }

  // Create an array of SQL_QIKEYs
  int32_t numKeys = keyList.entries();
  SQL_QIKEY siKeyList[numKeys];

  for (size_t j = 0; j < keyList.entries(); j++) {
    ComSecurityKey key = keyList[j];
    siKeyList[j].revokeKey.subject = key.getSubjectHashValue();
    siKeyList[j].revokeKey.object = key.getObjectHashValue();
    std::string actionString;
    key.getSecurityKeyTypeAsLit(actionString);
    strncpy(siKeyList[j].operation, actionString.c_str(), 2);
  }

  // Redef time will not change for system metadata schemas so no need
  // to add specific schema keys.

  // Call the CLI to send details to RMS
  SQL_EXEC_SetSecInvalidKeys(numKeys, siKeyList);

  return STATUS_GOOD;
}

// *****************************************************************************
//    Private functions
// *****************************************************************************

// *****************************************************************************
// *                                                                           *
// * Function: PrivMgrComponentPrivileges::isSQLDMLPriv                        *
// *                                                                           *
// *     This function determines if a component-level privilege is a DML      *
// *  privilege in the SQL_OPERATIONS component.                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentUID>                  const int64_t                   In       *
// *    is the unique ID associated with the component.                        *
// *                                                                           *
// *  <operationCode>                 const std::string &             In       *
// *    is the two character code associated with the component operation.     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: bool                                                             *
// *                                                                           *
// * true: This is a SQL_OPERATIONS DML privilege.                             *
// * false: This is NOT a SQL_OPERATIONS DML privilege.                        *
// *                                                                           *
// *****************************************************************************
static bool isSQLDMLPriv(const int64_t componentUID, const std::string operationCode)

{
  if (componentUID != SQL_OPERATIONS_COMPONENT_UID) return false;

  for (SQLOperation operation = SQLOperation::FIRST_DML_PRIV;
       static_cast<int>(operation) <= static_cast<int>(SQLOperation::LAST_DML_PRIV);
       operation = static_cast<SQLOperation>(static_cast<int>(operation) + 1)) {
    if (PrivMgr::getSQLOperationCode(operation) == operationCode) return true;
  }

  return false;
}
//***************************** End of isSQLDMLPriv ****************************

// *****************************************************************************
//    MyTable methods
// *****************************************************************************

// *****************************************************************************
// *                                                                           *
// * Function: MyTable::describeGrantTree                                      *
// *                                                                           *
// *    Describes grants for the specified grantor                             *
// *    Recursively calls itself to describe the grants for each grantee       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  grantorID                        const int32_t                        In *
// *    all grants granted by the grantorID are described.                     *
// *                                                                           *
// *  operationName                    const std::string &                  In *
// *     used for generate grant statement as granted operation.               *
// *                                                                           *
// *  componentName                    const std::string &                  In *
// *    used for generate grant statement on the component.                    *
// *                                                                           *
// *  rows                             const std::vector<MyRow> &           In *
// *    all the rows that represent grants for the component operation.        *
// *                                                                           *
// *  outlines                         std::vector<std::string> &           Out*
// *      output generated GRANT statements to this array.                     *
// *                                                                           *
// *****************************************************************************
void MyTable::describeGrantTree(const int32_t grantorID, const std::string &operationName,
                                const std::string &componentName, const std::vector<MyRow> &rows,
                                std::vector<std::string> &outlines) {
  // Starts at the beginning of the list searching for grants attributed to
  // the grantorID
  int32_t nextRowID = 0;
  while (nextRowID >= 0 && nextRowID < rows.size()) {
    int32_t rowID = findGrantor(nextRowID, grantorID, rows);

    // If this grantor did not grant any requests, -1 is returned and we are
    // done traversing this branch
    if (rowID == -1) return;

    nextRowID = rowID;
    MyRow row = rows[rowID];

    // We found a grant, describe the grant
    row.describeGrant(operationName, componentName, outlines);

    // Traverser any grants that may have been performed by the grantee.
    // If grantDepth is 0, then the grantee does not have the WITH GRANT
    // OPTION so no reason to traverse this potential branch - there is none.
    if (row.grantDepth_ != 0) describeGrantTree(row.granteeID_, operationName, componentName, rows, outlines);

    // get next grant for this grantorID
    nextRowID++;
  }
}

//******************* End of MyTable::describeGrantTree ************************

// *****************************************************************************
// *                                                                           *
// * Function: MyTable::fetchCompPrivInfo                                      *
// *                                                                           *
// *    Reads from the COMPONENT_PRIVILEGES table and returns the              *
// *    SQL_OPERATIONS privileges associated with DML privileges.              *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <granteeID>                     int32_t &                       In       *
// *    is the authID whose system-level DML privileges are being fetched.     *
// *                                                                           *
// *  <roleIDs>                       const std::vector<int32_t> &    In       *
// *    is a list of roleIDs granted to the grantee.                           *
// *                                                                           *
// *  <privs>                         PrivObjectBitmap &              In       *
// *    passes back the system-level DML privileges granted to the grantee.    *
// *                                                                           *
// *  <hasManagePrivPriv>             bool &                          In       *
// *    passes back if the user has MANAGE_PRIVILEGES authority.               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Data returned.                                               *
// *           *: Error encountered.                                           *
// *                                                                           *
// *****************************************************************************
PrivStatus MyTable::fetchCompPrivInfo(const int32_t granteeID, const std::vector<int32_t> &roleIDs,
                                      PrivObjectBitmap &privs, bool &hasManagePrivPriv, bool &hasSelectMetadata,
                                      bool &hasAnyManagePriv)

{
  // Check the last grantee data read before reading metadata.
#if 0
   // If privileges change between calls, then cache is not refreshed
   // comment out this check for now
   if (userPrivs_.granteeID_ == granteeID && 
       userPrivs_.roleIDs_ == roleIDs)
   {
      privs = userPrivs_.DMLPrivs_;
      hasManagePrivPriv = userPrivs_.managePrivileges_;
      hasSelectMetadata = userPrivs_.selectMetadata_;
      return STATUS_GOOD;
   }
#endif
  // Not found in cache, look for the priv info in metadata.
  std::string whereClause("WHERE COMPONENT_UID = 1 ");

  whereClause += "AND GRANTEE_ID IN (";
  whereClause += PrivMgr::authIDToString(granteeID);
  whereClause += ",";
  for (size_t ri = 0; ri < roleIDs.size(); ri++) {
    whereClause += PrivMgr::authIDToString(roleIDs[ri]);
    whereClause += ",";
  }
  whereClause += PrivMgr::authIDToString(PUBLIC_USER);
  whereClause += ")";

  std::string orderByClause;

  std::vector<MyRow> rows;

  PrivStatus privStatus = selectAllWhere(whereClause, orderByClause, rows);

  if (privStatus != STATUS_GOOD && privStatus != STATUS_WARNING) return privStatus;

  // Initialize cache.
  userPrivs_.granteeID_ = granteeID;
  userPrivs_.roleIDs_ = roleIDs;
  userPrivs_.managePrivileges_ = false;
  userPrivs_.selectMetadata_ = false;
  userPrivs_.bitmap_.reset();

  hasAnyManagePriv = false;

  for (size_t r = 0; r < rows.size(); r++) {
    MyRow &row = rows[r];

    if (PrivMgr::isSQLManageOperation(row.operationCode_.c_str())) hasAnyManagePriv = true;

    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::DML_SELECT_METADATA)) {
      userPrivs_.selectMetadata_ = true;
      continue;
    }

    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::MANAGE_PRIVILEGES)) {
      userPrivs_.managePrivileges_ = true;
      continue;
    }

    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::DML_DELETE)) {
      userPrivs_.bitmap_.set(DELETE_PRIV);
      continue;
    }

    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::DML_INSERT)) {
      userPrivs_.bitmap_.set(INSERT_PRIV);
      continue;
    }
    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::DML_REFERENCES)) {
      userPrivs_.bitmap_.set(REFERENCES_PRIV);
      continue;
    }

    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::DML_SELECT)) {
      userPrivs_.bitmap_.set(SELECT_PRIV);
      continue;
    }

    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::DML_EXECUTE)) {
      userPrivs_.bitmap_.set(EXECUTE_PRIV);
      continue;
    }

    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::DML_UPDATE)) {
      userPrivs_.bitmap_.set(UPDATE_PRIV);
      continue;
    }

    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::DML_USAGE)) {
      userPrivs_.bitmap_.set(USAGE_PRIV);
      continue;
    }

    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::ALTER)) {
      userPrivs_.bitmap_.set(ALTER_PRIV);
      continue;
    }

    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::CREATE)) {
      userPrivs_.bitmap_.set(CREATE_PRIV);
      continue;
    }

    if (row.operationCode_ == PrivMgr::getSQLOperationCode(SQLOperation::DROP)) {
      userPrivs_.bitmap_.set(DROP_PRIV);
      continue;
    }
  }

  hasManagePrivPriv = userPrivs_.managePrivileges_;
  hasSelectMetadata = userPrivs_.selectMetadata_;
  privs = userPrivs_.bitmap_;

  return STATUS_GOOD;
}
//******************* End of MyTable::fetchCompPrivInfo*************************

// *****************************************************************************
// *                                                                           *
// * Function: MyTable::findGrantor                                            *
// *                                                                           *
// *    Search the list of grants for the component operation looking for the  *
// *    next row for the grantor.                                              *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  currentRowID                           int32_t                        In *
// *    where to start searching for the next row for the grantor.             *
// *                                                                           *
// *  grantorID                        const int32_t                        In *
// *    the grantor to search for.                                             *
// *                                                                           *
// *  rows                             const std::vector<MyRow> &           In *
// *    all the rows that represent grants for the component operation.        *
// *                                                                           *
// *                                                                           *
// * Returns                                                                   *
// *   >= 0 -- the index into rows where the next row for grantor is described *
// *     -1 -- no more row, done                                               *
// *                                                                           *
// *****************************************************************************
int32_t MyTable::findGrantor(int32_t currentRowID, const int32_t grantorID, const std::vector<MyRow> rows) {
  for (size_t i = currentRowID; i < rows.size(); i++) {
    if (rows[i].grantorID_ == grantorID) return i;

    // rows are sorted in grantorID order, so once the grantorID stored in
    // rows is greater than the requested grantorID, we are done.
    if (rows[i].grantorID_ > grantorID) return -1;
  }
  return -1;
}

//******************* End of MyTable::findGrantor ******************************

// *****************************************************************************
// *                                                                           *
// * Function: MyTable::fetchOwner                                             *
// *                                                                           *
// *    Reads from the COMPONENT_PRIVILEGES table and returns the authID       *
// *  granted the specified privilege by the system.                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <componentUID>                  const int64_t                   In       *
// *    is the unique ID associated with the component.                        *
// *                                                                           *
// *  <operationCode>                 const std::string &             In       *
// *    is the two digit code for the operation.                               *
// *                                                                           *
// *  <granteeID>                     int32_t &                       Out      *
// *    passes back the authID granted the privilege by the system.            *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Found row with system grantor, grantee returned.             *
// *           *: Row not found or error encountered.                          *
// *                                                                           *
// *****************************************************************************
PrivStatus MyTable::fetchOwner(const int64_t componentUID, const std::string &operationCode, int32_t &granteeID)

{
  // Check the last row read before reading metadata.

  if (lastRowRead_.grantorID_ == SYSTEM_USER && lastRowRead_.componentUID_ == componentUID &&
      lastRowRead_.operationCode_ == operationCode) {
    granteeID = lastRowRead_.granteeID_;
    return STATUS_GOOD;
  }

  // Not found in cache, look for the system grantor in metadata.
  std::string whereClause("WHERE COMPONENT_UID = ");

  whereClause += PrivMgr::UIDToString(componentUID);
  whereClause += " AND OPERATION_CODE = '";
  whereClause += operationCode;
  whereClause += "' AND GRANTOR_ID = -2";

  MyRow row(tableName_);

  PrivStatus privStatus = selectWhereUnique(whereClause, row);

  switch (privStatus) {
    // Return status to caller to handle
    case STATUS_NOTFOUND:
    case STATUS_ERROR:
      return privStatus;
      break;

    // Object exists
    case STATUS_GOOD:
    case STATUS_WARNING:
      granteeID = row.granteeID_;
      return STATUS_GOOD;
      break;

    // Should not occur, internal error
    default:
      PRIVMGR_INTERNAL_ERROR("Switch statement in PrivMgrComponentPrivileges::MyTable::fetchOwner()");
      return STATUS_ERROR;
      break;
  }

  return STATUS_GOOD;
}
//*********************** End of MyTable::fetchOwner ***************************

// *****************************************************************************
// *                                                                           *
// * Function: MyTable::getRowsForGrantee                                      *
// *                                                                           *
// *    Finds the list of rows (branch) that need to be removed if the         *
// *  grantee no longer has WGO.                                               *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <baseRow>                       const MyRow &                   In       *
// *    contains the starting point for the branch                             *
// *                                                                           *
// *  <masterRowList>                       std::vector<MyRow> &      In/Out   *
// *    contains the master list of privileges                                 *
// *    this list is updated to set the "visited_" flag for performance        *
// *                                                                           *
// *  <rowsToDelete>                        std::set<size_t> &        Out      *
// *    returns the list of privileges to be removed                           *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: No errors are generated                                          *
// *                                                                           *
// *****************************************************************************
void MyTable::getRowsForGrantee(const MyRow &baseRow, std::vector<MyRow> &masterRowList,
                                std::set<size_t> &rowsToDelete) {
  for (size_t i = 0; i < masterRowList.size(); i++) {
    // master list is ordered by component ID, grantorID, granteeID and operationCode
    // If done checking rows for the grantorID_ from the baseRow, just return
    if ((masterRowList[i].componentUID_ == baseRow.componentUID_) && (masterRowList[i].grantorID_ > baseRow.granteeID_))
      break;

    // If we have already processed the row or it is a row we are not
    // interested in - continue
    if (masterRowList[i].visited_ || (masterRowList[i].grantorID_ < baseRow.granteeID_)) continue;

    // If this is a row we are interested in, add to rowsToDelete
    if ((masterRowList[i].componentUID_ == baseRow.componentUID_) &&
        (masterRowList[i].grantorID_ == baseRow.granteeID_) &&
        (masterRowList[i].operationCode_ == baseRow.operationCode_)) {
      // no more leaves, done with the branch
      if (masterRowList[i].grantDepth_ == 0) {
        masterRowList[i].visited_ = true;
        rowsToDelete.insert(i);
        continue;
      }

      // Privilege was granted WITH GRANT OPTION, see if there is anything
      // left on the branch to remove. If there are more leaves, check to
      // see if grantee gets the priv from other grantors (WGO). If so, then
      // no need to remove rest of branch
      std::vector<size_t> grantList;
      for (size_t g = 0; g < masterRowList.size(); g++) {
        // see if this is a row we are interested in
        if ((masterRowList[g].visited_ == false) && (masterRowList[g].componentUID_ == baseRow.componentUID_) &&
            (masterRowList[g].granteeID_ == baseRow.granteeID_) &&
            (masterRowList[g].operationCode_ == baseRow.operationCode_)) {
          // If this is the base row, skip
          if (masterRowList[g] == baseRow) continue;

          // we are interested, save it
          grantList.push_back(g);
        }
      }

      // See if privilege has been granted by another grantor
      if (grantList.size() > 0) {
        for (size_t j = 0; j < grantList.size(); j++) {
          size_t grantNdx = grantList[j];
          if (masterRowList[grantNdx].grantDepth_ < 0) {
            // this authID has been granted WGO privilege from another user
            // no need to remove branch
            masterRowList[i].visited_ = true;
            break;
          }
        }
      }

      // Check the next branch of privileges
      getRowsForGrantee(masterRowList[i], masterRowList, rowsToDelete);

      // found a leaf to remove
      masterRowList[i].visited_;
      rowsToDelete.insert(i);
    }
  }
}

// ****************************************************************************
// method: findGrantRow
//
// Searches the list of rows for the operation to see if the specified grantor
// has already granted the privilege to the grantee.
//
// Returns: true if the grant exists
// ****************************************************************************
MyRow MyTable::findGrantRow(const int32_t componentUID, const int32_t grantorID, const std::string granteeName,
                            const std::string &operationCode, const std::vector<MyRow> &rows) {
  MyRow row(tableName_);
  for (size_t i = 0; i < rows.size(); i++) {
    if ((componentUID == rows[i].componentUID_) && (operationCode == rows[i].operationCode_) &&
        (granteeName == rows[i].granteeName_) && (grantorID == rows[i].grantorID_)) {
      row = rows[i];
      break;
    }
  }
  return row;
}

// ****************************************************************************
// method: getOwnerRow
//
// Searches the list of rows for the operation and returns the owner row for
// the operation.  The owner row is the row where the grantor is system.
// There should only be one system grantor for each operation.
//
// Returns: true if the owner row is found
// ****************************************************************************
bool MyTable::getOwnerRow(const std::string &operationCode, const std::vector<MyRow> &rows, MyRow &row) {
  for (size_t i = 0; i < rows.size(); i++) {
    if (rows[i].operationCode_ == operationCode && rows[i].grantorID_ == SYSTEM_USER) {
      row = rows[i];
      return true;
    }
  }
  return false;
}

// ****************************************************************************
// method: getTreeOfGrantors
//
// Starts from the current position in the grant tree and finds all the grantors
//
// For example:
//    system -> db__root
//    db__root -> user1
//    user1 -> user2
//    user2 ->user3
//
//    db__root -> user4
//    user4 -> user3
//
// if user3 is passed in as grantee, the returned grantorIDs return:
//    (user2, user1, db__root, system, user4)
// if user1 is passed in, grantorIDs is (db__root, system)
// ****************************************************************************
void MyTable::getTreeOfGrantors(const int32_t grantee, const std::vector<MyRow> &rows, std::set<int32_t> &grantorIDs) {
  for (size_t i = 0; i < rows.size(); i++) {
    if (grantee == rows[i].granteeID_) {
      // grantorIDs is a SET so duplicate rows are not inserted
      grantorIDs.insert(rows[i].grantorID_);

      // Process the parent
      getTreeOfGrantors(rows[i].grantorID_, rows, grantorIDs);
    }
  }
}

// *****************************************************************************
// *                                                                           *
// * Function: MyTable::insert                                                 *
// *                                                                           *
// *    Inserts a row into the COMPONENT_PRIVILEGES table.                     *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <rowIn>                         const PrivMgrMDRow &            In       *
// *    is a MyRow to be inserted.                                             *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Row inserted.                                                *
// *           *: Insert failed. A CLI error is put into the diags area.       *
// *                                                                           *
// *****************************************************************************
PrivStatus MyTable::insert(const PrivMgrMDRow &rowIn) {
  char insertStatement[1000];

  const MyRow &row = static_cast<const MyRow &>(rowIn);

  sprintf(insertStatement, "insert into %s values (%d, %d, %ld, '%s', '%s', '%s', %d)", tableName_.c_str(),
          row.granteeID_, row.grantorID_, row.componentUID_, row.operationCode_.c_str(), row.granteeName_.c_str(),
          row.grantorName_.c_str(), row.grantDepth_);

  return CLIImmediate(insertStatement);
}
//************************** End of MyTable::insert ****************************

// *****************************************************************************
// *                                                                           *
// * Function: MyTable::selectAllWhere                                         *
// *                                                                           *
// *    Selects rows from the COMPONENT_PRIVILEGES table based on the          *
// *  specified WHERE clause and sorted per an optional ORDER BY clause.       *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <whereClause>                   const std::string &             In       *
// *    is the WHERE clause specifying a unique row.                           *
// *                                                                           *
// *  <orderByClause>                 const std::string &             In       *
// *    is an optional ORDER BY clause.                                        *
// *                                                                           *
// *  <rowOut>                        PrivMgrMDRow &                  Out      *
// *    passes back a MyRow.                                                   *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Row returned.                                                *
// *           *: Select failed. A CLI error is put into the diags area.       *
// *                                                                           *
// *****************************************************************************
PrivStatus MyTable::selectAllWhere(const std::string &whereClause, const std::string &orderByClause,
                                   std::vector<MyRow> &rows)

{
  std::string selectStmt(
      "SELECT COMPONENT_UID, OPERATION_CODE, GRANTEE_ID, GRANTOR_ID, GRANTEE_NAME, GRANTOR_NAME, GRANT_DEPTH FROM ");
  selectStmt += tableName_;
  selectStmt += " ";
  selectStmt += whereClause;
  selectStmt += " ";
  selectStmt += orderByClause;

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, CmpCommon::context()->sqlSession()->getParentQid());

  Queue *tableQueue = NULL;

  PrivStatus privStatus = executeFetchAll(cliInterface, selectStmt, tableQueue);

  if (privStatus == STATUS_ERROR) return privStatus;

  tableQueue->position();
  for (int idx = 0; idx < tableQueue->numEntries(); idx++) {
    OutputInfo *pCliRow = (OutputInfo *)tableQueue->getNext();
    MyRow row(tableName_);
    setRow(*pCliRow, row);
    rows.push_back(row);
  }

  return STATUS_GOOD;
}
//********************** End of MyTable::selectAllWhere ************************

// *****************************************************************************
// *                                                                           *
// * Function: MyTable::selectWhereUnique                                      *
// *                                                                           *
// *    Selects a row from the COMPONENT_PRIVILEGES table based on the         *
// *  specified WHERE clause.                                                  *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <whereClause>                   const std::string &             In       *
// *    is the WHERE clause specifying a unique row.                           *
// *                                                                           *
// *  <rowOut>                        PrivMgrMDRow &                  Out      *
// *    passes back a MyRow.                                                   *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// * Returns: PrivStatus                                                       *
// *                                                                           *
// * STATUS_GOOD: Row returned.                                                *
// *           *: Select failed. A CLI error is put into the diags area.       *
// *                                                                           *
// *****************************************************************************
PrivStatus MyTable::selectWhereUnique(const std::string &whereClause, PrivMgrMDRow &rowOut)

{
  // Generate the select statement
  std::string selectStmt(
      "SELECT COMPONENT_UID, OPERATION_CODE, GRANTEE_ID, GRANTOR_ID, GRANTEE_NAME, GRANTOR_NAME, GRANT_DEPTH FROM ");
  selectStmt += tableName_;
  selectStmt += " ";
  selectStmt += whereClause;

  ExeCliInterface cliInterface(STMTHEAP);

  PrivStatus privStatus = CLIFetch(cliInterface, selectStmt);

  if (privStatus != STATUS_GOOD && privStatus != STATUS_WARNING) return privStatus;

  char *ptr = NULL;
  int len = 0;
  char value[500];

  MyRow &row = static_cast<MyRow &>(rowOut);

  // column 1:  component_uid
  cliInterface.getPtrAndLen(1, ptr, len);
  row.componentUID_ = *(reinterpret_cast<int64_t *>(ptr));

  // column 2:  operation_code
  cliInterface.getPtrAndLen(2, ptr, len);
  strncpy(value, ptr, len);
  value[len] = 0;
  row.operationCode_ = value;

  // column 3:  granteeID
  cliInterface.getPtrAndLen(3, ptr, len);
  row.granteeID_ = *(reinterpret_cast<int32_t *>(ptr));

  // column 4:  grantorID
  cliInterface.getPtrAndLen(4, ptr, len);
  row.grantorID_ = *(reinterpret_cast<int32_t *>(ptr));

  // column 5:  grantee_name
  cliInterface.getPtrAndLen(5, ptr, len);
  strncpy(value, ptr, len);
  value[len] = 0;
  row.granteeName_ = value;

  // column 6:  grantor_name
  cliInterface.getPtrAndLen(6, ptr, len);
  strncpy(value, ptr, len);
  value[len] = 0;
  row.grantorName_ = value;

  // column 7:  grant_depth
  cliInterface.getPtrAndLen(7, ptr, len);
  row.grantDepth_ = *(reinterpret_cast<int32_t *>(ptr));

  lastRowRead_ = row;

  return STATUS_GOOD;
}
//************************ End of MyTable::selectWhere *************************

// *****************************************************************************
// *                                                                           *
// * Function: MyTable::setRow                                                 *
// *                                                                           *
// *    Sets the fields of a row.                                              *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  <cliInterface>                  OutputInfo &                    In       *
// *    is a reference to CLI interface to the row data that was read.         *
// *                                                                           *
// *  <rowOut>                        PrivMgrMDRow &                  Out      *
// *    passes back a MyRow.                                                   *
// *                                                                           *
// *****************************************************************************
void MyTable::setRow(OutputInfo &cliInterface, PrivMgrMDRow &rowOut)

{
  MyRow &row = static_cast<MyRow &>(rowOut);
  char *ptr = NULL;
  int32_t length = 0;
  char value[500];
  std::string selectStmt(
      "SELECT COMPONENT_UID, OPERATION_CODE, GRANTEE_ID, GRANTOR_ID, GRANTEE_NAME, GRANTOR_NAME, GRANT_DEPTH FROM ");

  // column 1:  component_uid
  cliInterface.get(0, ptr, length);
  row.componentUID_ = *(reinterpret_cast<int64_t *>(ptr));

  // column 2:  operation_code
  cliInterface.get(1, ptr, length);
  strncpy(value, ptr, length);
  value[length] = 0;
  row.operationCode_ = value;

  // column 3:  grantee_ID
  cliInterface.get(2, ptr, length);
  row.granteeID_ = *(reinterpret_cast<int32_t *>(ptr));

  // column 4:  grantor_ID
  cliInterface.get(3, ptr, length);
  row.grantorID_ = *(reinterpret_cast<int32_t *>(ptr));

  // column 5:  grantee_name
  cliInterface.get(4, ptr, length);
  strncpy(value, ptr, length);
  value[length] = 0;
  row.granteeName_ = value;

  // column 6:  grantor_name
  cliInterface.get(5, ptr, length);
  strncpy(value, ptr, length);
  value[length] = 0;
  row.grantorName_ = value;

  // column 7:  grant_depth
  cliInterface.get(6, ptr, length);
  row.grantDepth_ = *(reinterpret_cast<int32_t *>(ptr));

  lastRowRead_ = row;
}
//************************** End of MyTable::setRow ****************************

// *****************************************************************************
//    MyRow methods
// *****************************************************************************

// *****************************************************************************
// *                                                                           *
// * Function: MyRow::describeGrant                                            *
// *                                                                           *
// *    Generates text for the grant based on the row, operationName, and      *
// *    componentName                                                          *
// *                                                                           *
// *****************************************************************************
// *                                                                           *
// *  Parameters:                                                              *
// *                                                                           *
// *  operationName                    const std::string &                  In *
// *     used for generate grant statement as granted operation.               *
// *                                                                           *
// *  componentName                    const std::string &                  In *
// *    used for generate grant statement on the component.                    *
// *                                                                           *
// *  outlines                         std::vector<std::string> &           Out*
// *      output generated GRANT statements to this array.                     *
// *                                                                           *
// *****************************************************************************
void MyRow::describeGrant(const std::string &operationName, const std::string &componentName,
                          std::vector<std::string> &outlines) {
  // generate grant statement
  std::string grantText;
  if (grantorID_ == SYSTEM_USER) grantText += "-- ";
  grantText += "GRANT COMPONENT PRIVILEGE \"";
  grantText += operationName;
  grantText += "\" ON \"";
  grantText += componentName;
  grantText += "\" TO \"";
  if (granteeID_ == -1)
    grantText += PUBLIC_AUTH_NAME;
  else
    grantText += granteeName_;
  grantText += '"';
  if (grantDepth_ != 0) {
    grantText += " WITH GRANT OPTION";
  }
  grantText += ";";
  outlines.push_back(grantText);
}

//************************** End of MyRow::describeGrant ***********************
