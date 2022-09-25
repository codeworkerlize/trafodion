//*****************************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
//*****************************************************************************
  
#include "PrivMgrSchemaPrivileges.h"
#include "PrivMgrRoles.h"
#include "PrivMgrComponentPrivileges.h"
#include "PrivMgrObjects.h"

#include <numeric>
#include <cstdio>
#include <algorithm>
#include <iterator>
#include <vector>
#include "sqlcli.h"
#include "ComSmallDefs.h"
#include "ComDiags.h"
#include "ComSecurityKey.h"
#include "NAUserId.h"
#include "ComUser.h"
#include "logmxevent_traf.h"
#include "ExExeUtilCli.h"
#include "ComQueue.h"
#include "CmpCommon.h"
#include "CmpContext.h"
#include "ComSecurityKey.h"
#include "NAUserId.h"
#include "ComUser.h"
#include "seabed/ms.h"


 
// ****************************************************************************
// File: PrivMgrSchemaPrivileges.h
//
// This file contains:
//   class SchemaPrivsMDRow
//   class SchemaPrivsMDTable
//   non inline methods for class PrivMgrSchemaPrivileges
// ****************************************************************************

class SchemaPrivsMDRow;
class SchemaPrivsMDTable;
class PrivMgrSchemaPrivileges;

// *****************************************************************************
// * Class:         SchemaPrivsMDRow
// * Description:  This class represents a row from the SCHEMA_PRIVILEGES table
// *                
// * An object row can be uniquely identified by its object UID, granteeID 
// * and grantorID.
// *****************************************************************************
class SchemaPrivsMDRow : public PrivMgrMDRow
{
public:

   // Constructors and destructors:
   SchemaPrivsMDRow()
   : PrivMgrMDRow(PRIVMGR_SCHEMA_PRIVILEGES, SCHEMA_PRIVILEGES_ENUM),
     schemaUID_(0),
     grantorID_(0),
     granteeID_(0)
   { };
   
   SchemaPrivsMDRow(const SchemaPrivsMDRow &other)
   : PrivMgrMDRow(other)
   {
      schemaUID_ = other.schemaUID_;
      schemaName_ = other.schemaName_;
      granteeID_ = other.granteeID_;
      granteeName_ = other.granteeName_;
      grantorID_ = other.grantorID_;
      grantorName_ = other.grantorName_;
      privsBitmap_ = other.privsBitmap_;
      grantableBitmap_ = other.grantableBitmap_;
      current_ = other.current_;
      visited_ = other.visited_;
   };
   virtual ~SchemaPrivsMDRow() {};
   
   // get methods
   PrivMgrRowInfo getPrivRowInfo()
   {
     PrivMgrRowInfo rowInfo(grantorID_);
     rowInfo.setGrantorName(grantorName_);
     rowInfo.setGranteeID(granteeID_);
     rowInfo.setGranteeName(granteeName_);
     rowInfo.setColumnOrdinal(-1);
     rowInfo.setPrivsBitmap(privsBitmap_);
     rowInfo.setGrantableBitmap(grantableBitmap_);

     return rowInfo;
   }

   // Methods used to determine changes after processing revoked privileges
   PrivMgrCoreDesc& accessCurrent() { return current_; }
   PrivMgrCoreDesc& accessVisited() { return visited_; }
   void clearVisited() { visited_.setAllPrivAndWgo(false); };
   bool isChanged() 
    { return (current_ == PrivMgrCoreDesc(privsBitmap_, grantableBitmap_)); }

   void setToOriginal() 
    { current_ = PrivMgrCoreDesc(privsBitmap_, grantableBitmap_); };

   // Return True iff some current flag is set, where visited is not.
   NABoolean anyNotVisited() const {return current_.anyNotSet( visited_ );}

   // Describe a row for tracing
   void describeRow (std::string &rowDetails);

// -------------------------------------------------------------------
// Data Members:
// -------------------------------------------------------------------
     
   int64_t            schemaUID_;
   std::string        schemaName_;
   int32_t            granteeID_;
   std::string        granteeName_;
   int32_t            grantorID_;
   std::string        grantorName_;
   PrivObjectBitmap   privsBitmap_;
   PrivObjectBitmap   grantableBitmap_;
   
   PrivMgrCoreDesc  visited_;
   PrivMgrCoreDesc  current_;
};


// *****************************************************************************
// * Class:         SchemaPrivsMDTable
// * Description:  This class represents the SCHEMA_PRIVILEGES table 
// *                
// *    A schema privileges row can be uniquely identified by:
// *       schemaUID
// *       granteeID
// *       grantorID
// *****************************************************************************
class SchemaPrivsMDTable : public PrivMgrMDTable
{
public:

   // constructors and destructors
   SchemaPrivsMDTable(
      const std::string & tableName,
      ComDiagsArea * pDiags = NULL) 
   : PrivMgrMDTable(tableName,SCHEMA_PRIVILEGES_ENUM, pDiags)
     {};

   virtual ~SchemaPrivsMDTable() 
   {};

   // DML operations
   PrivStatus deleteRow(const SchemaPrivsMDRow & row);
   virtual PrivStatus deleteWhere(const std::string & whereClause);

   virtual PrivStatus insert(const PrivMgrMDRow &row);
   PrivStatus insertSelect(
      const std::string & objectsLocation,
      const std::string & authsLocation);

   PrivStatus selectWhere(
      const std::string & whereClause,
      const std::string & orderByClause,
      std::vector<PrivMgrMDRow *> &rowList);
   virtual PrivStatus selectWhereUnique(
      const std::string & whereClause,
      PrivMgrMDRow & row);

   PrivStatus updateRow(
      const SchemaPrivsMDRow & row,
      int64_t &rowCount);

   PrivStatus updateWhere(
      const std::string & setClause,
      const std::string & whereClause,
      int64_t &rowCount);

private:   
   SchemaPrivsMDTable();
   void setRow(OutputInfo *pCliRow, SchemaPrivsMDRow &rowOut);

};

// *****************************************************************************
// *   PrivMgrSchemaPrivileges.cpp static function declarations                      *
// *****************************************************************************

static void deleteRowList(std::vector<PrivMgrMDRow *> & rowList);


// *****************************************************************************
//    PrivMgrSchemaPrivileges methods
// *****************************************************************************

// -----------------------------------------------------------------------
// Default Constructor
// -----------------------------------------------------------------------
PrivMgrSchemaPrivileges::PrivMgrSchemaPrivileges () 
: PrivMgr(),
  schemaUID_(0)
{ 
  schemaTableName_ = metadataLocation_ + "." + PRIVMGR_SCHEMA_PRIVILEGES;
} 

// ----------------------------------------------------------------------------
// Construct PrivMgrPrivileges object for getPrivilege statements
// ----------------------------------------------------------------------------
PrivMgrSchemaPrivileges::PrivMgrSchemaPrivileges (
   const std::string &metadataLocation,
   ComDiagsArea *pDiags)
: PrivMgr(metadataLocation, pDiags),
  schemaUID_(0)
{
  schemaTableName_  = metadataLocation + "." + PRIVMGR_SCHEMA_PRIVILEGES;
}

PrivMgrSchemaPrivileges::PrivMgrSchemaPrivileges (
   const std::string &trafMetadataLocation,
   const std::string &metadataLocation,
   ComDiagsArea *pDiags)
: PrivMgr(trafMetadataLocation, metadataLocation, pDiags),
  schemaUID_(0)
{
  schemaTableName_  = metadataLocation + "." + PRIVMGR_SCHEMA_PRIVILEGES;
}

// -----------------------------------------------------------------------
// Construct a PrivMgrSchemaPrivileges object for a new schema privilege.
// -----------------------------------------------------------------------
PrivMgrSchemaPrivileges::PrivMgrSchemaPrivileges (
   const int64_t schemaUID,
   const std::string &schemaName,
   const std::string &metadataLocation,
   ComDiagsArea * pDiags)
: PrivMgr(metadataLocation,pDiags),
  schemaUID_(schemaUID),
  schemaName_(schemaName)
{
  schemaTableName_  = metadataLocation + "." + PRIVMGR_SCHEMA_PRIVILEGES;
} 

// ----------------------------------------------------------------------------
// Construct PrivMgrPrivileges object for describe statements
// ----------------------------------------------------------------------------
PrivMgrSchemaPrivileges::PrivMgrSchemaPrivileges (
   const PrivMgrObjectInfo &objectInfo,
   const std::string &metadataLocation,
   ComDiagsArea *pDiags)
: PrivMgr(metadataLocation, pDiags),
  schemaUID_(((PrivMgrObjectInfo)objectInfo).getObjectUID()),
  schemaName_(((PrivMgrObjectInfo)objectInfo).getObjectName())
{
  schemaTableName_  = metadataLocation + "." + PRIVMGR_SCHEMA_PRIVILEGES;
}


// -----------------------------------------------------------------------
// Copy constructor
// -----------------------------------------------------------------------
PrivMgrSchemaPrivileges::PrivMgrSchemaPrivileges(const PrivMgrSchemaPrivileges &other)
: PrivMgr(other)
{
  schemaUID_ = other.schemaUID_;
  schemaName_ = other.schemaName_;
  schemaTableName_ = other.schemaTableName_;
  schemaRowList_ = other.schemaRowList_;
}

// -----------------------------------------------------------------------
// Destructor.
// -----------------------------------------------------------------------
PrivMgrSchemaPrivileges::~PrivMgrSchemaPrivileges() 
{ 
  deleteRowList(schemaRowList_);
}

// *****************************************************************************
// * Method: getPrivsOnSchema                                
// *                                                       
// * Creates a set of priv descriptors for all user grantees on a schema.
// * Used by Trafodion compiler to store as part of the table descriptor.
// * One privDesc is created per user or role granted at least one privilege.
// * These privileges are summarized across all grantors.
// *                                                       
// *  Parameters:    
// *                                                                       
// *  <schemaUID>   schema identifier
// *  <schemaOwner> schema owner
// *  <schemaType>  is the type of object PRIVATE or SHARED
// *  <privDescs>   is the returned list of privileges the on the object
// *                                                                  
// * Returns: PrivStatus                                               
// *                                                                  
// * STATUS_GOOD: privilege descriptors were built
// *           *: unexpected error occurred, see diags.     
// *  
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getPrivsOnSchema (
  const int64_t schemaUID,
  const int32_t schemaOwner,
  const ComObjectType schemaType,
  std::vector<PrivMgrDesc> & privDescs )
{
  PrivStatus retcode = STATUS_GOOD;

  if (schemaUID == 0)
  {
    PRIVMGR_INTERNAL_ERROR("schemaUID is 0 for getPrivOnSchema()");
    return STATUS_ERROR;
  }

  schemaUID_ = schemaUID;

  // generate the list of privileges granted to the schema and store in class
  if (generateSchemaRowList(schemaOwner) == STATUS_ERROR)
    return STATUS_ERROR;
  
  // Gets all the user grantees (userIDs) for the schema
  // Gets all the role grantees (roleIDs) for the schema
  // The public auth ID is included in the userIDs list
  std::vector<int32_t> userIDs;
  std::vector<int32_t> roleIDs;
  if (getDistinctIDs(schemaRowList_, userIDs, roleIDs) == STATUS_ERROR)
    return STATUS_ERROR;

  // Gather privilege descriptors for each user
  for (size_t i = 0; i < userIDs.size(); i++)
  {
    int32_t userID = userIDs[i];

    PrivMgrDesc privsOfTheUser(userID, true);
    bool hasManagePrivPriv = false;
    std::vector <int32_t> emptyRoleIDs;
 
    // getUserPrivs returns schema privileges summarized across all grantors
    // for the user only (role list is empty) 
    if (getUserPrivs(schemaType, userID, emptyRoleIDs, privsOfTheUser,
                     hasManagePrivPriv ) != STATUS_GOOD)
      return STATUS_ERROR;
    
    if (!privsOfTheUser.isNull())
      privDescs.push_back(privsOfTheUser);
  }
  
  // Attach roles to priv lists
  for (size_t i = 0; i < roleIDs.size(); i++)
  {
    int32_t grantee = roleIDs[i];
    
    PrivMgrDesc privsOfTheUser(grantee, true);
    bool hasManagePrivPriv = false;
    std::vector <int32_t> emptyRoleIDs;
    
    // getUserPrivs returns schema privileges summarized across all grantors. 
    if (getUserPrivs(schemaType, grantee, emptyRoleIDs, privsOfTheUser,
                     hasManagePrivPriv ) != STATUS_GOOD)
      return STATUS_ERROR;
    
    if (!privsOfTheUser.isNull())
      privDescs.push_back(privsOfTheUser);
  }

  return STATUS_GOOD;
}


// *****************************************************************************
// * Method: getDistinctIDs                                
// *
// * Finds all the userIDs that have been granted at least one privilege on
// * schema. This list includes the public authorization ID
// *
// * Finds all the roleIDs that have been granted at least one privilege on
// * schema
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getDistinctIDs(
  const std::vector <PrivMgrMDRow *> &schemaRowList,
  std::vector<int32_t> &userIDs,
  std::vector<int32_t> &roleIDs)
{
  int32_t authID;

  // DB__ROOTROLE is a special role.  If the current user has been granted 
  // this role, then they have privileges.  Add it to the roleIDs list for
  // processing.
  roleIDs.push_back(ROOT_ROLE_ID);

  // The direct schema grants are stored in memory (schemaRowList) so no I/O
  for (size_t i = 0; i < schemaRowList.size(); i++)
  {
    SchemaPrivsMDRow &row = static_cast<SchemaPrivsMDRow &> (*schemaRowList[i]);
    authID = row.granteeID_;

    // insert authID into correct list, if not already present
    if (ComUser::isPublicUserID(authID) || CmpSeabaseDDLauth::isUserID(authID)) 
    {
       if (std::find(userIDs.begin(), userIDs.end(), authID) == userIDs.end())
         userIDs.insert( std::upper_bound( userIDs.begin(), userIDs.end(), authID ), authID);
    }
    else
    {
       if (std::find(roleIDs.begin(), roleIDs.end(), authID) == roleIDs.end())
         roleIDs.insert( std::upper_bound( roleIDs.begin(), roleIDs.end(), authID ), authID);
    }
  }

  return STATUS_GOOD;
}
  
   
// ******************************************************************************
// method: getTreeOfGrantors
//
// Returns the list of grantors that have granted privileges to the grantee, 
// either directly or through another grantor in the tree.
//
// The list is determined by first looking at the direct grantors. For each 
// returned grantor, this function is called recursively to get the previous set
// of grantors until there are no more grantors.
//
// The list is returned in grantor ID order. 
//
// For example:
//   user1 (owner) grants to:
//      user6 who grants to:
//         user3
//         user4 who grants to:
//           user5
//           user2
//      user3 who grants to:
//         user4
//         user5
// The following grantors are returned for granteeID user4:
//    user1, user3, user6
// 
// Params:
//  granteeID - where to start the search
//  listOfGrantors - returns the list of grantors
// ******************************************************************************
void PrivMgrSchemaPrivileges::getTreeOfGrantors(
  const int32_t granteeID,
  std::set<int32_t> &listOfGrantors)
{
  // search the rowList for a match
  for (size_t i = 0; i < schemaRowList_.size(); i++)
  {
    SchemaPrivsMDRow &row = static_cast<SchemaPrivsMDRow &> (*schemaRowList_[i]);
    if (row.granteeID_ == granteeID)
    {
       // We found a grant to the granteeID
       // go up to the next level using the grantorID
       getTreeOfGrantors( row.grantorID_, listOfGrantors);
       listOfGrantors.insert(granteeID);
    }
  }
}


// *****************************************************************************
// * Method: grantSchemaPriv                                
// *                                                       
// *    Adds or updates a row in the SCHEMA_PRIVILEGES table.
// *                                                       
// *  Parameters:    
// *                                                                       
// *  <schemaType> is the type of the schema - either shared or private
// *  <grantorID> is the unique identifier for the grantor
// *  <grantorName> is the name of the grantor (upper cased)
// *  <granteeID> is the unique identifier for the grantee
// *  <granteeName> is the name of the grantee (upper cased)
// *  <schemaOwner> is the unique identifer of the schema owner
// *  <privsList> is the list of privileges to grant
// *  <isAllSpecified> if true then all privileges valid for the schema
// *                        type will be granted
// *  <isWGOSpecified> is true then also allow the grantee to grant the set
// *                   of privileges to other grantees
// *                                                                     
// * Returns: PrivStatus                                               
// *                                                                  
// * STATUS_GOOD: Privileges were granted
// *           *: Unable to grant privileges, see diags.     
// *                                                               
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::grantSchemaPriv(
      const ComObjectType schemaType,
      const int32_t grantorID,
      const std::string &grantorName,
      const int32_t granteeID,
      const std::string &granteeName,
      const int32_t schemaOwnerID,
      const std::vector<PrivType> &privsList,
      const bool isAllSpecified,
      const bool isWGOSpecified)
{
  PrivStatus retcode = STATUS_GOOD;
 
  std::string traceMsg;
  log (__FILE__, "****** GRANT schema operation begins ******", -1);

  // If the granting to self or DB__ROOT, return an error
  //if (grantorID == granteeID || granteeID == ComUser::getRootUserID())
  if (granteeID == ComUser::getRootUserID())
  {
    *pDiags_ << DgSqlCode(-CAT_CANT_GRANT_TO_SELF_OR_ROOT);
    return STATUS_ERROR;
  }

  // If DDL privilege, do not allow WITH GRANT OPTION
  if (isWGOSpecified)
  {
    for (int32_t i = 0; i < privsList.size();++i)
    {
      if (privsList[i] >= FIRST_DDL_PRIV && privsList[i] <= LAST_DDL_PRIV ||
          (isAllSpecified))
      {
        *pDiags_ << DgSqlCode(-CAT_WGO_NOT_ALLOWED);
        return STATUS_ERROR;
      }
    } // end for
  }

  // Generate the list of privilege descriptors that were requested 
  PrivMgrDesc privsToGrant(granteeID,true);
  if (!convertPrivsToDesc( isAllSpecified,
                           isWGOSpecified,
                           false, 
                           privsList, 
                           privsToGrant))
    return STATUS_ERROR;

  // generate the list of privileges granted to the schema and store in class
  if (generateSchemaRowList(schemaOwnerID) == STATUS_ERROR)
    return STATUS_ERROR;
    
  // get schema privileges across all grantors 
  std::vector<int_32> roleIDs;
  PrivMgrDesc privsOfTheGrantor(grantorID,true);
  bool hasManagePrivPriv;
  if (getUserPrivs(schemaType, grantorID, roleIDs, privsOfTheGrantor,
                         hasManagePrivPriv) != STATUS_GOOD)
    return STATUS_ERROR;
    
  // get roleIDs for the grantor
  retcode = getRoleIDsForUserID(grantorID,roleIDs);
  if (retcode == STATUS_ERROR)
    return retcode;

  // If null, the grantor has no privileges
  if ( privsOfTheGrantor.isNull() )
  {
    std::string rolesWithPrivs;
    if (getRolesToCheck(grantorID, roleIDs, schemaType, rolesWithPrivs)== STATUS_GOOD)
    {
      if (rolesWithPrivs.size() > 0)
      {
        *pDiags_ << DgSqlCode (-CAT_PRIVILEGE_NOT_GRANTED)
                 << DgString0 (grantorName.c_str())
                 << DgString1 (rolesWithPrivs.c_str());
        return STATUS_ERROR;
      }
    }

    *pDiags_ << DgSqlCode (-CAT_PRIVILEGE_NOT_GRANTED)
             << DgString0 (grantorName.c_str());
    return STATUS_ERROR;
  }

  // If tenants are enabled, do not allow grantor to grant privileges to roles
  // where they are not ADMIN
  bool enabled = msg_license_multitenancy_enabled();
  if (enabled && isRoleID(granteeID) && doTenantPrivChecks())
  {
    bool hasAdmin = false;
    PrivMgrRoles roles(trafMetadataLocation_,metadataLocation_,pDiags_);
    retcode = roles.hasRoleAdmin(granteeID, grantorID, granteeName, roleIDs, hasAdmin);
    if (retcode == STATUS_ERROR)
      return STATUS_ERROR;
    if (!hasAdmin)
    {
      *pDiags_ << DgSqlCode(-CAT_PRIV_NOT_GRANTED_TO_ROLE )
               << DgString0(granteeName.c_str());
      return STATUS_ERROR;
    }
  }

  // Make sure the grantor can grant at least one of the requested privileges
  // SQL Ansi states that privileges that can be granted should be done so
  // even if some requested privilege are not grantable.
  //   (origPrivsToGrant contains the original request, once the operation 
  //    completes, any priv specified in origPrivsToGrant that were not granted
  //    will generate a warning.)
  // Remove any privsToGrant which are not held GRANTABLE by the Grantor.
  bool warnNotAll = false;
  PrivMgrDesc origPrivsToGrant = privsToGrant;

  // if limitToGrantable true ==> some specified privs were not grantable.
  if ( privsToGrant.limitToGrantable( privsOfTheGrantor ) )
    warnNotAll = true;

  // If nothing left to grant, we are done.
  if ( privsToGrant.isNull() )
  {
    std::string rolesWithPrivs;
    if (getRolesToCheck(grantorID, roleIDs, schemaType, rolesWithPrivs)== STATUS_GOOD)
    {
      if (rolesWithPrivs.size() > 0)
      {
        *pDiags_ << DgSqlCode (-CAT_PRIVILEGE_NOT_GRANTED)
                 << DgString0 (grantorName.c_str())
                 << DgString1 (rolesWithPrivs.c_str());
        return STATUS_ERROR;
      }
    }

    *pDiags_ << DgSqlCode (-CAT_PRIVILEGE_NOT_GRANTED)
             << DgString0 (grantorName.c_str());
    return STATUS_ERROR;
  }

  // Check for circular dependency.  If USERX grants to USERY WGO, then USERY 
  // cannot grant back to USERX. Theoretically, USERX can grant select, update 
  // to USERY and USERY can grant delete, insert to USERX but for simplicity, 
  // we will reject the request independent on the set of privileges involved.
  // ??? - do we need to make this check?  Object & column operations do.
  std::set<int32_t> listOfGrantors;
  getTreeOfGrantors(grantorID, listOfGrantors);

  // If we find the grantee in the list of grantors, return an error
  if (listOfGrantors.find(granteeID) != listOfGrantors.end())
  {
    *pDiags_ << DgSqlCode(-CAT_CIRCULAR_PRIVS)
             << DgString0(grantorName.c_str())
             << DgString1(granteeName.c_str());
    return STATUS_ERROR;
  }

  // See if grantor has previously granted privileges to the grantee
  SchemaPrivsMDRow row;
  bool foundRow = false;
  retcode = getGrantedPrivs(grantorID, granteeID, row); 
  if (retcode == STATUS_ERROR)
    return retcode;
  else if (retcode == STATUS_GOOD)
    foundRow = true;
  else
    foundRow = false;

  // if privileges exist, set currentPrivs to existing list
  PrivMgrCoreDesc currentPrivs; 
  if (foundRow)
  {
    PrivMgrCoreDesc tempPrivs(row.privsBitmap_, row.grantableBitmap_);
    currentPrivs = tempPrivs;
  }

  // remove privileges that have already been granted
  PrivMgrDesc privsToAdd = privsToGrant;
  PrivMgrCoreDesc::PrivResult result = privsToAdd.grantSchemaPrivs(currentPrivs);

  // nothing to grant - everything is already granted
  if ( result == PrivMgrCoreDesc::NONE )
  {
    if (warnNotAll)
      reportPrivWarnings(origPrivsToGrant,
                         privsToGrant,
                         CAT_NOT_ALL_PRIVILEGES_GRANTED);
      return STATUS_GOOD;
  }

  // Internal consistency check.  We should have granted something.
  assert( result != PrivMgrCoreDesc::NEUTRAL );

  // There is something to grant, update/insert metadata

  // set up row if it does not exist and add it to the schemaRowList
  if (!foundRow)
  {
    SchemaPrivsMDRow *pRow = new SchemaPrivsMDRow();
    pRow->schemaUID_ = schemaUID_;
    pRow->schemaName_ = schemaName_;
    pRow->granteeID_ = granteeID;
    pRow->granteeName_ = granteeName;
    pRow->grantorID_ = grantorID;
    pRow->grantorName_ = grantorName;
    pRow->privsBitmap_ = privsToGrant.getSchemaPrivs().getPrivBitmap();
    pRow->grantableBitmap_ = privsToGrant.getSchemaPrivs().getWgoBitmap();
    schemaRowList_.push_back(pRow);
    row = *pRow;
  }

  // combine privsToGrant with existing privs
  else
  {
    privsToGrant.unionOfPrivs(currentPrivs);
    row.privsBitmap_ = privsToGrant.getSchemaPrivs().getPrivBitmap();
    row.grantableBitmap_ = privsToGrant.getSchemaPrivs().getWgoBitmap();
  }

  row.describeRow(traceMsg);
  traceMsg.insert(0, "updating existing privilege row ");
  log (__FILE__, traceMsg, -1);

  SchemaPrivsMDTable schemaPrivsTable (schemaTableName_, pDiags_);

  // update the row
  if (foundRow)
  {
    int64_t rowCount;
    if (schemaPrivsTable.updateRow(row, rowCount) == STATUS_ERROR)
      return STATUS_ERROR;

    if (rowCount == 0)
    {
      std::string rowDescription;
      row.describeRow(rowDescription);
      rowDescription.insert(0, ",  ");
      rowDescription.insert (0, row.schemaName_);
      rowDescription.insert(0, "Failed to grant (update) privileges for schema ");
      PRIVMGR_INTERNAL_ERROR(rowDescription.c_str());
      return STATUS_ERROR;
    }
  }

  // insert the row
  else
    retcode = schemaPrivsTable.insert(row);

  if (warnNotAll)
    reportPrivWarnings(origPrivsToGrant, 
                       privsToGrant, 
                       CAT_NOT_ALL_PRIVILEGES_GRANTED);

  log (__FILE__, "****** GRANT schema operation succeeded ******", -1);

  return retcode;
}


// ****************************************************************************
// method: generateSchemaRowList
//
// generate the list of privileges granted to the schema and 
// store in schemaRowList_ class member
//
// Returns:
//   STATUS_GOOD     - list of rows was generated
//   STATUS_NOTFOUND - in most cases, there should be at least one row in the
//                     SCHEMA_PRIVILEGES table but there maybe exceptions.  
//   STATUS_ERROR    - the diags area is populated with any errors
// ****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::generateSchemaRowList(const int32_t schemaOwnerID)
{
  // If schemaRowList_ already allocated, just return
  if (schemaRowList_.size() > 0)
    return STATUS_GOOD;
  PrivStatus privStatus = getSchemaRowList(schemaUID_, schemaOwnerID, schemaRowList_);
  if (privStatus == STATUS_ERROR)
    return privStatus;
  return STATUS_GOOD;
}

// ****************************************************************************
// method: getSchemaRowList
//
// generate the list of privileges granted to the passed in schemaUID and 
// returns in the schemaRowList parameter
//
// The list is ordered by grantor/grantee
//
// Returns:
//   STATUS_GOOD     - list of rows was generated
//   STATUS_NOTFOUND - in most cases, there should be at least one row in the
//                     SCHEMA_PRIVILEGES table but there maybe exceptions.
//   STATUS_ERROR    - the diags area is populated with any errors
// ****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getSchemaRowList(
   const int64_t schemaUID, 
   const int32_t schemaOwnerID,
   std::vector<PrivMgrMDRow *> &schemaRowList)
{ 
  // if the schema owner details are not stored, add it
  SchemaPrivsMDRow *ownerRow = new SchemaPrivsMDRow();
  ownerRow->schemaUID_ = schemaUID;
  ownerRow->schemaName_ = schemaName_;
  ownerRow->granteeID_ = schemaOwnerID;
  
  // If schemaOwnerID not found in metadata, ComDiags is populated with 
  // error 8732: Authorization ID <schemaOwnerID> is not a registered user or role
  int32_t length;
  char tempName[MAX_DBUSERNAME_LEN + 1];
  if (ComUser::getAuthNameFromAuthID(schemaOwnerID,tempName, sizeof(tempName),
                                     length, FALSE, pDiags_) < 0)
    return STATUS_ERROR;

  ownerRow->granteeName_ = tempName;
  ownerRow->grantorID_ = SYSTEM_USER;
  ownerRow->grantorName_ = SYSTEM_AUTH_NAME;
  ownerRow->privsBitmap_.set();
  ownerRow->grantableBitmap_.set();
  schemaRowList.push_back(ownerRow);

  // If the schema owner is a role, than create a second row granting all
  // privileges to DB__ROOT where the role is the grantor
  std::string whereClause ("where schema_uid = ");
  whereClause += UIDToString(schemaUID);
  std::string orderByClause(" order by grantor_id, grantee_id ");

  SchemaPrivsMDTable schemaPrivsTable(schemaTableName_,pDiags_);
  PrivStatus privStatus = 
    schemaPrivsTable.selectWhere(whereClause, orderByClause, schemaRowList);
  if (privStatus == STATUS_ERROR)
    return privStatus;

  std::string traceMsg ("getting schema privileges, number privleges is ");
  traceMsg += to_string((long long int)schemaRowList.size());
  log (__FILE__, traceMsg, -1);
  for (size_t i = 0; i < schemaRowList.size(); i++)
  {
    SchemaPrivsMDRow privRow = static_cast<SchemaPrivsMDRow &> (*schemaRowList[i]);
    privRow.describeRow(traceMsg);
    log (__FILE__, traceMsg, i);
  }
  return STATUS_GOOD;
}


// ******************************************************************************
// method: getGrantedPrivs
//
// This method searches the list of schema privs to get information for the
// object, grantor, and grantee.
//
// input:  granteeID
// output: a row from the object_privileges table describing privilege details
//
//  Returns: PrivStatus                                               
//                                                                   
//      STATUS_GOOD: row was found (and returned)
//  STATUS_NOTFOUND: no privileges have been granted
// ******************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getGrantedPrivs(
  const int32_t grantorID,
  const int32_t granteeID,
  PrivMgrMDRow &rowOut)
{
  SchemaPrivsMDRow & row = static_cast<SchemaPrivsMDRow &>(rowOut);

  for (size_t i = 0; i < schemaRowList_.size(); i++)
  {
    SchemaPrivsMDRow privRow = static_cast<SchemaPrivsMDRow &> (*schemaRowList_[i]); 
    if (privRow.grantorID_ == grantorID && privRow.granteeID_ == granteeID)
    {
      row = privRow;
      return STATUS_GOOD;
    } 
  }

  return STATUS_NOTFOUND;
}
 

// *****************************************************************************
// * Method: revokeSchemaPriv                                
// *                                                       
// *    Deletes or updates a row in the SCHEMA_PRIVILEGES table.
// *                                                       
// *  Parameters:    
// *                                                                       
// *  <schemaType> is the type of the schema - either shared or private
// *  <grantorID> is the unique identifier for the grantor
// *  <granteeID> is the unique identifier for the grantee
// *  <privsList> is the list of privileges to revoke
// *  <isAllSpecified> if true then all privileges valid for the object
// *                        type will be revoked
// *  <isGOFSpecified> if true then remove the ability for  the grantee 
// *                   to revoke the set of privileges to other grantees
// *                                                                     
// * Returns: PrivStatus                                               
// *                                                                  
// * STATUS_GOOD: Privileges were revoked
// *           *: Unable to revoke privileges, see diags.     
// *                                                               
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::revokeSchemaPriv (
  const ComObjectType schemaType,
  const int32_t grantorID,
  const std::string & grantorName,
  const int32_t granteeID,
  const std::string & granteeName,
  const int32_t schemaOwnerID,
  const std::vector<PrivType> &privsList,
  const bool isAllSpecified,
  const bool isGOFSpecified)
{
  PrivStatus retcode = STATUS_GOOD;

  std::string traceMsg;
  log (__FILE__, "****** REVOKE schema operation begins ******", -1);

  // Generate the list of privilege descriptors that were requested 
  PrivMgrDesc privsToRevoke(granteeID,true);
  if (!convertPrivsToDesc( isAllSpecified,
                           true,
                           isGOFSpecified,
                           privsList,
                           privsToRevoke))
    return STATUS_ERROR;

  // generate the list of privileges granted to the schema and store in class
  if (generateSchemaRowList(schemaOwnerID) == STATUS_ERROR)
    return STATUS_ERROR;

  // get schema privileges across all grantors, don't include roles are grantors 
  std::vector<int_32> roleIDs;
  PrivMgrDesc privsOfTheGrantor(grantorID,true);
  bool hasManagePrivPriv;
  if (getUserPrivs(schemaType, grantorID, roleIDs, privsOfTheGrantor,
                         hasManagePrivPriv) != STATUS_GOOD)
    return STATUS_ERROR;

  // get roleIDs for the grantor
  retcode = getRoleIDsForUserID(grantorID,roleIDs);
  if (retcode == STATUS_ERROR)
    return retcode;

  // If null, the grantor has no privileges
  if ( privsOfTheGrantor.isNull() )
  {
    std::string rolesWithPrivs;
    if (getRolesToCheck(grantorID, roleIDs, schemaType, rolesWithPrivs)== STATUS_GOOD)
    {
      if (rolesWithPrivs.size() > 0)
      {
        *pDiags_ << DgSqlCode (-CAT_PRIVILEGE_NOT_GRANTED)
                 << DgString0 (grantorName.c_str())
                 << DgString1 (rolesWithPrivs.c_str());
        return STATUS_ERROR;
      }
    }

    *pDiags_ << DgSqlCode (-CAT_PRIVILEGE_NOT_REVOKED);
    return STATUS_ERROR;
  }

  // Remove any privsToRevoke which are not held grantable by the Grantor.
  // If limitToGrantable returns true, some privs are not revokable.
  bool warnNotAll = false;
  PrivMgrDesc origPrivsToRevoke = privsToRevoke;
  if ( privsToRevoke.limitToGrantable( privsOfTheGrantor ) )
  {
     // This is ok.  Can specify ALL without having all privileges set.
     if (!isAllSpecified )
       warnNotAll = true;  // Not all the specified privs can be revoked
  }

  // If nothing left to revoke, we are done.
  if ( privsToRevoke.isNull() )
  {
    std::string rolesWithPrivs;
    if (getRolesToCheck(grantorID, roleIDs, schemaType, rolesWithPrivs)== STATUS_GOOD)
    {
      if (rolesWithPrivs.size() > 0)
      {
        *pDiags_ << DgSqlCode (-CAT_PRIVILEGE_NOT_GRANTED)
                 << DgString0 (grantorName.c_str())
                 << DgString1 (rolesWithPrivs.c_str());
        return STATUS_ERROR;
      }
    }

    *pDiags_ << DgSqlCode (-CAT_PRIVILEGE_NOT_REVOKED);
    return STATUS_ERROR;
  }

  // See if grantor has previously granted privileges to the grantee
  SchemaPrivsMDRow row;
  retcode = getGrantedPrivs(grantorID, granteeID, row);
  if (retcode == STATUS_ERROR)
    return retcode;

  if (retcode == STATUS_NOTFOUND)
  {
    // Set up parameters for the error message: privileges, grantor, & grantee
    // privilege list
    std::string privListStr;
    for (size_t i = 0; i < privsList.size(); i++)
      privListStr += PrivMgrUserPrivs::convertPrivTypeToLiteral(privsList[i]) + ", ";
    
    privListStr.erase(privListStr.length()-2, privListStr.length());
    if (isGOFSpecified)
      privListStr += " WITH GRANT OPTION";

    *pDiags_ << DgSqlCode (CAT_GRANT_NOT_FOUND)
             << DgString0 (privListStr.c_str())
             << DgString1 (grantorName.c_str())
             <<DgString2 (granteeName.c_str());
  
    return STATUS_WARNING;
  }

  // TDB:  if user privs have already been revoked, just return
  

  // Adjust the privileges
  PrivMgrCoreDesc currentPrivs(row.privsBitmap_, row.grantableBitmap_);
  PrivMgrCoreDesc adjustedPrivs = privsToRevoke.getSchemaPrivs();
  PrivMgrCoreDesc tempPrivs = currentPrivs;
  adjustedPrivs.complementPrivs();
  adjustedPrivs.intersectionOfPrivs(tempPrivs);

  // create descriptors for the original and updated privs
  PrivMgrDesc originalPrivs (row.granteeID_,true);
  originalPrivs.setSchemaPrivs(currentPrivs);
  PrivMgrDesc updatedPrivs(row.granteeID_,true);
  updatedPrivs.setSchemaPrivs(adjustedPrivs);

  // save adjusted privileges
  row.privsBitmap_ = adjustedPrivs.getPrivBitmap();
  row.grantableBitmap_ = adjustedPrivs.getWgoBitmap();

  // See if there are any dependencies that need to be removed before
  // removing the privilege
  ObjectUsage objectUsage;
  objectUsage.objectUID = schemaUID_;
  objectUsage.granteeID = granteeID;
  objectUsage.grantorIsSystem = false;
  objectUsage.objectType = schemaType;
  objectUsage.objectName = row.schemaName_;
  objectUsage.columnReferences = NULL;
  objectUsage.originalPrivs = originalPrivs;
  objectUsage.updatedPrivs = updatedPrivs;

  if (updateDependentObjects(objectUsage, PrivCommand::REVOKE_OBJECT_RESTRICT) == STATUS_ERROR)
    return STATUS_ERROR;

  // Go rebuild the privilege tree to see if it is broken
  // If it is broken, return an error
  if (checkRevokeRestrict (row, schemaRowList_))
    return STATUS_ERROR;

  SchemaPrivsMDTable schemaPrivsTable (schemaTableName_, pDiags_);
  if (adjustedPrivs.isNull())
  {
    row.describeRow(traceMsg);
    traceMsg.insert(0, "deleting privilege row ");
    log (__FILE__, traceMsg, -1);

    // delete the row
    retcode = schemaPrivsTable.deleteRow(row);
  }
  else
  {
    row.describeRow(traceMsg);
    traceMsg.insert(0, "updating existing privilege row ");
    log (__FILE__, traceMsg, -1);

    // update the row
    int64_t rowCount;
    if (schemaPrivsTable.updateRow(row, rowCount) == STATUS_ERROR)
      return STATUS_ERROR;

    if (rowCount == 0)
    {
      std::string rowDescription;
      row.describeRow(rowDescription);
      rowDescription.insert(0,", ");
      rowDescription.insert(0,row.schemaName_);
      rowDescription.insert(0,"Failed to revoke (update) privileges for schema ");
      PRIVMGR_INTERNAL_ERROR(rowDescription.c_str());
      return STATUS_ERROR;
    }
  }

  // Send a message to the Trafodion RMS process about the revoke operation.
  // RMS will contact all master executors and ask that cached privilege 
  // information be re-calculated
  retcode = sendSecurityKeysToRMS(granteeID, privsToRevoke);

  // SQL Ansi states that privileges that can be revoked should be done so
  // even if some requested privilege are not revokable.
  // TDB:  report which privileges were not revoked
  if (warnNotAll)
      reportPrivWarnings(origPrivsToRevoke,
                         updatedPrivs,
                         CAT_NOT_ALL_PRIVILEGES_REVOKED);

  log (__FILE__, "****** REVOKE schema operation succeeded ******", -1);

  return retcode;
}

// *****************************************************************************
// * Method: revokeSchemaPriv                                
// *                                                       
// *    Deletes rows in the SCHEMA_PRIVILEGES table associated with the schema
// *    This code assumes that all dependent and referencing objects such as
// *    views have been (or will be) dropped.  No extra checks are performed.
// *                                                       
// * Returns: PrivStatus                                               
// *                                                                  
// * STATUS_GOOD: Privileges were revoked
// *           *: Unable to revoke privileges, see diags.     
// *                                                               
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::revokeSchemaPriv ()
{
  PrivStatus retcode = STATUS_GOOD;

  if (schemaUID_ == 0)
  {
    PRIVMGR_INTERNAL_ERROR("schemaUID is 0 for revoke command");
    return STATUS_ERROR;
  }

  char buf[100];
  sprintf(buf, "where schema_uid = %ld", schemaUID_);
  std::string whereClause  = buf;

  // delete all the rows for this object
  SchemaPrivsMDTable schemaPrivsTable (schemaTableName_, pDiags_);
  retcode = schemaPrivsTable.deleteWhere(whereClause);

  return retcode;
}

// ******************************************************************************
// method: checkRevokeRestrict
//
// This method starts at the beginning of the privilege tree and rebuilds
// it from top to bottom.  If the revoke causes part of the tree to be 
// unaccessible (a broken branch), it returns true; otherwise, revoke can 
// proceed - returns false.
//
// Params:
//     rowIn -   the row containing proposed changes from the requested
//               revoke statement.
//     rowList - a list of all the rows associated with the object
//
//  true - unable to perform revoke because of dependencies
//  false - able to perform revoke.privileges
//
// The diags area is set up with where the tree was broken
// ******************************************************************************
bool PrivMgrSchemaPrivileges::checkRevokeRestrict ( 
  PrivMgrMDRow &rowIn,
  std::vector <PrivMgrMDRow *> &rowList )
{
  // Search the list of privileges associated with the schemnaand replace 
  // the bitmaps of the current row with the bitmaps of the row sent in (rowIn).  
  // At the same time, clear visited_ and set current_ to row values
  SchemaPrivsMDRow updatedRow = static_cast<SchemaPrivsMDRow &>(rowIn);

  std::string traceMsg;
  log (__FILE__, "checking grant tree for broken branches", -1);

  for (int32_t i = 0; i < rowList.size(); i++)
  {
    //  if rowIn matches this row, then update the bitmaps to use the 
    // updated bitmaps
    SchemaPrivsMDRow &currentRow = static_cast<SchemaPrivsMDRow &> (*rowList[i]);

    if (updatedRow.granteeID_ == currentRow.granteeID_ &&
        updatedRow.grantorID_ == currentRow.grantorID_ )
    {
      currentRow.privsBitmap_ = updatedRow.privsBitmap_;
      currentRow.grantableBitmap_ = updatedRow.grantableBitmap_;
    }
    // reset visited_ and current_ PrivMgrCoreDesc
    currentRow.clearVisited();
    currentRow.setToOriginal();
  }

 // Reconstruct the privilege tree 
  // Each privilege tree starts with the root - system grantor (-2)
  for ( size_t i = 0; i < NBR_OF_PRIVS; i++ )
  {
    PrivType pType = PrivType(i);

    int32_t systemGrantor = SYSTEM_USER;
    scanSchemaBranch (pType, systemGrantor, rowList);
  }

  // If a branch of the tree was not visited, then we have a broken
  // tree.  Therefore, revoke restrict will leave abandoned privileges
  // in the case, return true.
  bool  notVisited = false;
  for (size_t i = 0; i < rowList.size(); i++)
  {
    SchemaPrivsMDRow &currentRow = static_cast<SchemaPrivsMDRow &> (*rowList[i]);
    currentRow.describeRow(traceMsg);
    log (__FILE__, traceMsg, i);

    if (currentRow.anyNotVisited())
    {
      *pDiags_ << DgSqlCode(-CAT_DEPENDENT_PRIV_EXISTS)
               << DgString0(currentRow.grantorName_.c_str())
               << DgString1(currentRow.granteeName_.c_str());

      log (__FILE__, "found a branch that is not accessible", -1);
      notVisited = true;
      break;
    }
  }
  return notVisited;

}


// ******************************************************************************
//  method:  scanSchemaBranch 
// 
//   scans the privsList entries for match on Grantor,
//   keeping track of which priv/wgo entries have been encountered
//   by setting "visited" flag in the entry.
//
//   For each entry discovered, set visited flag to indicate that
//   priv and wgo were seen.  For wgo, if the wgo visited flag has not
//   already been set, call scanObjectBranch recursively with this grantee
//   as grantor.  (By observing the state of the wgo visited flag
//   we avoid redundantly exploring the sub-tree rooted in a grantor
//   which has already been discovered as having wgo from some other
//   ancestor grantor.)
//
//   This algorithm produces a depth-first scan of all nodes of the 
//   directed graph of privilege settings which can currently be reached
//   by an uninterrupted chain of wgo values.
//
//   The implementation is dependent on the fact that PrivsList 
//   entries are ordered by Grantor, Grantee, and within each of these 
//   by Primary uid value, type.  Entries for system grantor (_SYSTEM) are the
//   first entries in the list.
// ******************************************************************************
void PrivMgrSchemaPrivileges::scanSchemaBranch( 
  const PrivType pType, // in
  const int32_t& grantor,              // in
  const std::vector<PrivMgrMDRow *> & privsList  )   // in
{

  // The PrivMgrMDRow <list> is maintained in order by
  //  Grantee within Grantor value - through an order by clause.

  // Skip over Grantors lower than the specified one.
  size_t i = 0;
  while (  i < privsList.size() )
  {
    SchemaPrivsMDRow &currentRow = static_cast<SchemaPrivsMDRow &> (*privsList[i]);
    if (currentRow.grantorID_ < grantor)
     i++;
   else
     break;
  }

 // For matching Grantor, process each Grantee.
  while (  i < privsList.size() )
  {
    SchemaPrivsMDRow &privEntry = static_cast<SchemaPrivsMDRow &> (*privsList[i]);
    if (privEntry.grantorID_ == grantor)
    {
      PrivMgrCoreDesc& current = privEntry.accessCurrent();
      if ( current.getPriv(pType) )
      {
         // This grantee has priv.  Set corresponding visited flag.
         PrivMgrCoreDesc& visited = privEntry.accessVisited();
         visited.setPriv(pType, true);

         if ( current.getWgo(pType))
         {
              // This grantee has wgo.  
            if ( visited.getWgo(pType) )
              {   // Already processed this subtree.
              }
            else
              {
                visited.setWgo(pType, true);

                int32_t thisGrantee( privEntry.granteeID_ );
                if ( ComUser::isPublicUserID(thisGrantee) )
                  scanPublic( pType, //  Deal with PUBLIC grantee wgo.
                              privsList );
                else
                  {
                    int32_t granteeAsGrantor(thisGrantee);
                    scanSchemaBranch( pType, // Scan for this grantee as grantor.
                                 granteeAsGrantor,
                                 privsList );
                  }
              }
         }  // end this grantee has wgo
      }  // end this grantee has this priv

      i++;  // on to next privsList entry
    }
    else
      break;
  }  // end scan privsList over Grantees for this Grantor
}  // end scanCurrent


/* *******************************************************************
   scanPublic --  a grant wgo to PUBLIC has been encountered for the 
   current privilege type, so *all* users are able to grant this privilege.
   Scan the privsList for all grantees who have this priv from any grantor,
   marking each such entry as visited.

****************************************************************** */

void PrivMgrSchemaPrivileges::scanPublic( 
  const PrivType pType,
  const std::vector<PrivMgrMDRow *>& privsList )    // in
{
   // PUBLIC has a priv wgo.  So *every* grant of this priv
   //   is allowed, by any Grantor.
   for ( size_t i = 0; i < privsList.size(); i++ )
   {
      SchemaPrivsMDRow &privEntry = static_cast<SchemaPrivsMDRow &> (*privsList[i]);
      const PrivMgrCoreDesc& current = privEntry.accessCurrent();
      if ( current.getPriv(pType) )
      {
           // This grantee has priv.  Set corresponding visited flag.
         PrivMgrCoreDesc& visited = privEntry.accessVisited();
         visited.setPriv(pType, true);

         if ( current.getWgo(pType) )
           visited.setWgo(pType, true);
      }
   }  // end scan privsList over all Grantees/Grantors
} // end scanPublic


// ****************************************************************************
// method: sendSecurityKeysToRMS
//
// This method generates a security key for each privilege revoked for the
// grantee.  It then makes a cli call sending the keys.
// SQL_EXEC_SetSecInvalidKeys will send the security keys to RMS and RMS
// sends then to all the master executors.  The master executors check this
// list and recompile any queries to recheck privileges.
//
// input:
//    granteeID - the UID of the user losing privileges
//       the granteeID is stored in the PrivMgrDesc class - extra?
//    revokedPrivs - the list of privileges that were revoked
//
// Returns: PrivStatus                                               
//                                                                  
// STATUS_GOOD: Privileges were granted
//           *: Unable to send keys,  see diags.     
// ****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::sendSecurityKeysToRMS(
  const int32_t granteeID, 
  const PrivMgrDesc &revokedPrivs)
{
  // If the privilege is revoked from a role, then need to generate QI keys for 
  // all the role's grantees
  NAList<int32_t> roleGrantees (STMTHEAP);
  if (PrivMgr::isRoleID(granteeID))
  {
    std::vector<int32_t> roleIDs;
    roleIDs.push_back(granteeID);
    std::vector<int32_t> granteeIDs;
    if (getGranteeIDsForRoleIDs(roleIDs,granteeIDs,false) == STATUS_ERROR)
      return STATUS_ERROR;
    for (size_t i = 0; i < granteeIDs.size(); i++)
      roleGrantees.insert(granteeIDs[i]);
  }
  else
    roleGrantees.insert(ComUser::getCurrentUser());

  // Go through the list of schema privileges and generate SQL_QIKEYs
  // Always generate an OBJECT_IS_SCHEMA key independent of grantee type
  NASet<ComSecurityKey> keyList(NULL);
  const PrivMgrCoreDesc &privs = revokedPrivs.getSchemaPrivs();
  if (!buildSecurityKeys(roleGrantees,
                         granteeID,
                         schemaUID_,
                         true,
                         false,
                         revokedPrivs.getSchemaPrivs(),
                         keyList))
  {
    PRIVMGR_INTERNAL_ERROR("ComSecurityKey is null");
    return STATUS_ERROR;
  }

  // Create an array of SQL_QIKEYs
  int32_t numKeys = keyList.entries()+1;
  SQL_QIKEY siKeyList[numKeys];

  for (size_t j = 0; j < keyList.entries(); j++)
  {
    ComSecurityKey key = keyList[j];
    siKeyList[j].revokeKey.subject = key.getSubjectHashValue();
    siKeyList[j].revokeKey.object = key.getObjectHashValue();
    std::string actionString;
    key.getSecurityKeyTypeAsLit(actionString);
    strncpy(siKeyList[j].operation, actionString.c_str(), 2);
  }
  
  // add a security key for the schema itself
  siKeyList[numKeys-1].ddlObjectUID = schemaUID_;
  strncpy(siKeyList[numKeys-1].operation, 
          COM_QI_SCHEMA_REDEF_LIT,
          sizeof(siKeyList[numKeys-1].operation));
  
  // Call the CLI to send details to RMS
  SQL_EXEC_SetSecInvalidKeys(numKeys, siKeyList);

  return STATUS_GOOD;
}


// *****************************************************************************
// * Method: getPrivBitmaps                                
// *                                                       
// *    Reads the SCHEMA_PRIVILEGES table to get the privilege bitmaps for 
// *    rows matching a where clause.
// *                                                       
// *  Parameters:    
// *                                                                       
// *  <whereClause> specifies the rows to be returned
// *  <orderByClause> specifies the order of the rows to be returned
// *  <privBitmaps> passes back a vector of bitmaps.
// *                                                                     
// * Returns: PrivStatus                                               
// *                                                                  
// * STATUS_GOOD: Privileges were returned
// *           *: Unable to read privileges, see diags.     
// *                                                               
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getPrivBitmaps(
   const std::string & whereClause,
   const std::string & orderByClause,
   std::vector<PrivObjectBitmap> & privBitmaps)
   
{
  std::vector<PrivMgrMDRow *> rowList;
  SchemaPrivsMDTable schemaPrivsTable(schemaTableName_,pDiags_);
 
  PrivStatus privStatus = schemaPrivsTable.selectWhere(whereClause, orderByClause,rowList);
  if (privStatus != STATUS_GOOD)
  {
    deleteRowList(rowList);
    return privStatus;
  }
   
  for (size_t r = 0; r < rowList.size(); r++)
  {
    SchemaPrivsMDRow &row = static_cast<SchemaPrivsMDRow &>(*rowList[r]);
    privBitmaps.push_back(row.privsBitmap_);
  }
  deleteRowList(rowList);
   
  return STATUS_GOOD;
}



// *****************************************************************************
// * Method: getPrivTextForSchema                                
// *                                                       
// *    returns GRANT statements describing all the privileges that have been
// *    granted on the schema
// *                                                       
// *  Parameters:    
// *                                                                       
// *  <schemaInfo> Metadata details for schema.
// *  <privilegeText> The resultant GRANT statement(s)
// *                                                                     
// * Returns: PrivStatus                                               
// *                                                                  
// * STATUS_GOOD    : Grants were found
// * STATUS_NOTFOUND: No grants were found
// *               *: Unable to access privileges, see diags.     
// *                                                               
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getPrivTextForSchema(
   const PrivMgrObjectInfo & schemaInfo,
   std::string & privilegeText)
{
  PrivStatus retcode = STATUS_GOOD;

  if (schemaUID_ == 0)
  {
    PRIVMGR_INTERNAL_ERROR("schemaUID is 0 for describe privileges command");
    return STATUS_ERROR;
  }

  // generate the list of privileges granted to the schema and store in class
  PrivMgrObjectInfo &ncSchemaInfo = (PrivMgrObjectInfo &)schemaInfo;
  if (generateSchemaRowList(ncSchemaInfo.getSchemaOwner()) == STATUS_ERROR)
    return STATUS_ERROR;

  // No failures possible for schemas, all information in rowList.  
  buildPrivText(schemaRowList_, schemaInfo, 
                PrivLevel::SCHEMA, pDiags_, privilegeText);

  return retcode;
}


// *****************************************************************************
// * Method: getPrivsOnSchemaForUser                                
// *                                                       
// *    returns privileges granted to the requested user for the requested 
// *    schema
// *                                                       
// *  Parameters:    
// *                                                                       
// *  <schemaUID> identifies the schema
// *  <userID> identifies the user
// *  <privsOfTheUser> the list of privileges is returned
// *                                                                     
// * Returns: PrivStatus                                               
// *                                                                  
// * STATUS_GOOD: Privileges were gathered
// *           *: Unable to gather privileges, see diags.     
// *                                                               
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getPrivsOnSchemaForUser(
  const int64_t schemaUID,
  ComObjectType schemaType,
  const int32_t userID,
  PrivMgrDesc &privsOfTheUser)
{
  PrivStatus retcode = STATUS_GOOD;
  
  schemaUID_ = schemaUID;
  if (schemaUID == 0)
  {
    PRIVMGR_INTERNAL_ERROR("schemaUID is 0 for get privileges command");
    return STATUS_ERROR;
  }

  // generate the list of privileges granted to the object and store in class
  if (generateSchemaRowList(ROOT_USER_ID) == STATUS_ERROR)
    return STATUS_ERROR;

  // generate the list of roles for the user
  std::vector<int32_t> roleIDs;
  if (getRoleIDsForUserID(userID,roleIDs) == STATUS_ERROR)
    return STATUS_ERROR;

  // generate the list of privileges for the user
  privsOfTheUser.setGrantee(userID);
  bool hasManagePrivPriv = false;
  
  retcode = getUserPrivs(schemaType, userID, roleIDs, privsOfTheUser, 
                         hasManagePrivPriv);

  return retcode;
}


// *****************************************************************************
// * Method: getSchemasForRole
// *                                                       
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getSchemasForRole(
   const int32_t roleID,
   std::vector<int64_t> &schemaList)
{
  PrivStatus retcode = STATUS_GOOD;

  std::string whereClause ("where grantee_id = ");
  whereClause += UIDToString(roleID);
  std::string orderByClause(" order by grantor_id, grantee_id ");

  std::vector<PrivMgrMDRow *> schemaRowList;
  SchemaPrivsMDTable schemaPrivsTable(schemaTableName_,pDiags_);
  PrivStatus privStatus =
    schemaPrivsTable.selectWhere(whereClause, orderByClause, schemaRowList);
  if (privStatus == STATUS_ERROR)
    return privStatus;
 
  // return the list of schema_UIDs
  for (size_t i = 0; i < schemaRowList.size(); i++)
  {
    SchemaPrivsMDRow &row = static_cast<SchemaPrivsMDRow &> (*schemaRowList[i]);
    schemaList.push_back(row.schemaUID_);
  }

  return STATUS_GOOD;
}

// *****************************************************************************
// * Method: getUserPrivs                                
// *                                                       
// *    Accumulates privileges for a user summarized over all grantors
// *    including PUBLIC
// *                                                       
// *  Parameters:    
// *                                                                       
// *  <schemaType> is the type of the schema - shared or private
// *  <granteeID> specifies the userID to accumulate
// *  <roleIDs> specifies a list of roles granted to the grantee
// *  <summarizedPrivs> contains the summarized privileges
// *  <hasManagePrivPriv> returns whether the grantee has MANAGE_PRIVILEGES priv
// *                                                                     
// * Returns: PrivStatus                                               
// *                                                                  
// * STATUS_GOOD: Privileges were gathered
// *           *: Unable to gather privileges, see diags.     
// *                                                               
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getUserPrivs(
  ComObjectType schemaType,
  const int32_t granteeID,
  const std::vector<int32_t> & roleIDs,
  PrivMgrDesc &summarizedPrivs,
  bool & hasManagePrivPriv)
{
   PrivStatus retcode = STATUS_GOOD;
   PrivMgrDesc temp(granteeID,true);

   retcode = getPrivsFromAllGrantors( schemaUID_, granteeID, roleIDs,
                                      temp, hasManagePrivPriv);
   if (retcode != STATUS_GOOD)
    return retcode;

   summarizedPrivs = temp;


  return retcode;
}

// *****************************************************************************
// * Method: getPrivsFromAllGrantors                                
// *                                                       
// *    Accumulates privileges for a specified userID
// *    Does the actual accumulation orchestrated by getUserPrivs
// *                                                       
// *  Parameters:    
// *                                                                       
// *  <schemaUID> object to gather privileges for
// *  <schemaType> is the type of the subject object.
// *  <granteeID> specifies the userID to accumulate
// *  <roleIDs> is vector of roleIDs granted to the grantee
// *  <hasManagePrivPriv> returns whether the grantee has MANAGE_PRIVILEGES priv
// *  <summarizedPrivs> contains the summarized privileges
// *                                                                     
// * Returns: PrivStatus                                               
// *                                                                  
// * STATUS_GOOD: Privileges were accumulated
// *           *: Unable to accumulate privileges, see diags.     
// *                                                               
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getPrivsFromAllGrantors(
   const int64_t schemaUID,
   const int32_t granteeID,
   const std::vector<int32_t> & roleIDs,
   PrivMgrDesc &summarizedPrivs,
   bool & hasManagePrivPriv)
{
  PrivStatus retcode = STATUS_GOOD;
  hasManagePrivPriv = false;
  bool hasPublicGrantee = false;
  bool hasSelect = false;
  bool hasAnyManagePriv = false;
  
  // Check to see if the granteeID is the system user
  // if so, the system user has all privileges.  Set up appropriately
  if (ComUser::isSystemUserID(granteeID))
  {
    PrivObjectBitmap bitmap;
    bitmap.set();
    PrivMgrCoreDesc coreSchemaPrivs(bitmap, bitmap);
    summarizedPrivs.setSchemaPrivs(coreSchemaPrivs);
    hasManagePrivPriv = true;
    return STATUS_GOOD;
  }
  
  std::vector<PrivMgrMDRow *> rowList;
  PrivObjectBitmap systemPrivs;
  PrivMgrComponentPrivileges componentPrivileges;
  
  componentPrivileges.getSQLCompPrivs(granteeID,roleIDs,systemPrivs,
                                      hasManagePrivPriv, hasSelect,
                                      hasAnyManagePriv);

  bool hasAllPrivs = false; 
  if (systemPrivs.test(DELETE_PRIV) && systemPrivs.test(INSERT_PRIV) &&
      systemPrivs.test(REFERENCES_PRIV) && systemPrivs.test(SELECT_PRIV) &&
      systemPrivs.test(UPDATE_PRIV) && systemPrivs.test(USAGE_PRIV) &&
      systemPrivs.test(EXECUTE_PRIV)&& systemPrivs.test(CREATE_PRIV) && 
      systemPrivs.test(ALTER_PRIV) && systemPrivs.test(DROP_PRIV))
    hasAllPrivs =  true;

  if (hasManagePrivPriv && hasAllPrivs)
  {
    PrivMgrCoreDesc coreSchemaPrivs(systemPrivs,systemPrivs);
    summarizedPrivs.setSchemaPrivs(coreSchemaPrivs);
  }
  
  // Accumulate schema level privileges
  else
  {
    retcode = getRowsForGrantee(schemaUID, granteeID, roleIDs, rowList);
    if (retcode == STATUS_ERROR)
      return retcode; 

    // schema privileges table does not contain the privileges granted to the
    // schema owner.  Add them first

    PrivMgrCoreDesc coreSchemaPrivs;

    // Get the privileges for the object granted to the grantee
    for (int32_t i = 0; i < rowList.size();++i)
    {
      SchemaPrivsMDRow &row = static_cast<SchemaPrivsMDRow &> (*rowList[i]);
    
      if (ComUser::isPublicUserID(row.granteeID_))
        hasPublicGrantee = true;

      PrivMgrCoreDesc temp (row.privsBitmap_, row.grantableBitmap_);
      coreSchemaPrivs.unionOfPrivs(temp);
    }
  
    PrivObjectBitmap grantableBitmap;
  
    if (hasManagePrivPriv)
       grantableBitmap = systemPrivs;
  
    PrivMgrCoreDesc temp2(systemPrivs,grantableBitmap);
    coreSchemaPrivs.unionOfPrivs(temp2);
  
    summarizedPrivs.setSchemaPrivs(coreSchemaPrivs);
  }

  summarizedPrivs.setHasPublicPriv(hasPublicGrantee);

  // If authID has SELECT_METADATA component privilege and is a metadata
  // object, set SELECT_PRIV to true
  // Mantis case 4660 - if the user has any MANAGE privilege, then allow
  // metadata to be selected from a subset of metadata tables. 
  if (hasSelect || hasAnyManagePriv)
  {
    PrivMgrObjects objects(trafMetadataLocation_,pDiags_);
    std::string objectName;
    std::string schemaName;
    retcode = objects.fetchQualifiedName(schemaUID, objectName);
    if (retcode == STATUS_GOOD)
    {
      if (isMetadataObject(objectName, schemaName))
      {
        if (hasSelect ||
            (schemaName == std::string(SEABASE_PRIVMGR_SCHEMA) ||
             schemaName == std::string(SEABASE_MD_SCHEMA)  ||
             schemaName == std::string(SEABASE_TENANT_SCHEMA )))
        {
          PrivMgrCoreDesc temp;
          temp.setPriv(SELECT_PRIV, true);
          summarizedPrivs.unionOfPrivs(temp);
        }
      }
    }
  }

  return STATUS_GOOD;
}


// ----------------------------------------------------------------------------
// method: getRolesToCheck
//
// This method checks all the roles granted to the user and returns a comma
// separated list of those roles that have privileges on the target schema.
// ----------------------------------------------------------------------------
PrivStatus PrivMgrSchemaPrivileges::getRolesToCheck(
  const int32_t grantorID,
  const std::vector<int32_t> & roleIDs,
  const ComObjectType schemaType,
  std::string &rolesWithPrivs)
{
  int32_t length;
  char roleName[MAX_DBUSERNAME_LEN + 1];
  std::vector<int_32> emptyRoleIDs;
  bool hasManagePrivPriv = false;

  for (size_t r = 0; r < roleIDs.size(); r++)
  {
    PrivMgrDesc privsOfTheRole(roleIDs[r],true);
    if (getUserPrivs(schemaType, roleIDs[r], emptyRoleIDs, privsOfTheRole,
                     hasManagePrivPriv) != STATUS_GOOD)
      return STATUS_ERROR;

    if (!privsOfTheRole.isNull())
    {
      // just return what getAuthNameFromAuthID returns
      ComUser::getAuthNameFromAuthID(roleIDs[r],roleName, sizeof(roleName),length,TRUE);
      if (r > 0)
        rolesWithPrivs += ", ";
      rolesWithPrivs += roleName;
    }
  }

  if (rolesWithPrivs.size() > 0)
  {
    rolesWithPrivs.insert (0,"Please retry using the BY clause for one of the following roles (");
    rolesWithPrivs += ").";
  }

  return STATUS_GOOD;
}

// *****************************************************************************
// * Method: getRowsForGrantee                                
// *                                                       
// *    Searches schemaRowList_ to obtain all  privileges granted to the
// *    specified granteeID for the object (schemaUID)
// *                                                       
// *  Parameters:    
// *                                                                       
// *  <schemaUID> schema to gather privileges for
// *  <granteeID> specifies the userID to gather privileges
// *  <roleIDs> the list of roles granted to the userID
// *  <rowList> returns the list of granted privileges as a vector list
// *    consisiting of the grantor, grantee, and privileges for the object
// *                                                                     
// * Returns: PrivStatus                                               
// *                                                                  
// * STATUS_GOOD: Privileges were retrieved
// *           *: Unable to retrieve privileges, see diags.     
// *                                                               
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getRowsForGrantee(
  const int64_t schemaUID,
  const int32_t granteeID,
  const std::vector<int_32> &roleIDs,
  std::vector<PrivMgrMDRow *> &rowList)
{
  PrivStatus retcode = STATUS_GOOD;

  // create the list of row pointers 
  std::vector<int32_t> authIDs = roleIDs;
  authIDs.push_back(granteeID);
  authIDs.push_back(PUBLIC_USER);
  std::vector<int32_t>::iterator it;
  std::vector<PrivMgrMDRow *> privRowList;

  if (!schemaUID == schemaUID_)
  {
    PRIVMGR_INTERNAL_ERROR("Schema UID's do not match = getRowsForGrantee");
    return STATUS_ERROR;
  }
 
  for (size_t i = 0; i < schemaRowList_.size(); i++)
  {
    SchemaPrivsMDRow &row = static_cast<SchemaPrivsMDRow &> (*schemaRowList_[i]);
     it = std::find(authIDs.begin(), authIDs.end(), row.granteeID_);
   if (it != authIDs.end())
     rowList.push_back(schemaRowList_[i]);
  }

  return STATUS_GOOD;
}

// *****************************************************************************
// * Method: updateDependentObjects
// *                                                       
// * This code gets the list of dependent objects that have had their privileges
// * changed because of the ongoing grant/revoke request.  It then updates
// * the privileges tables to change any dependencies.
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::updateDependentObjects(
  const ObjectUsage &objectUsage,
  const PrivCommand command)
{
  // At this time, creating views (SELECT), constraints (REFERENCES)  and UDRs
  // (USAGE) based on schema level privileges are not supported; creation of 
  // these objects require necessary privileges at the object or column level. 
  //
  // The following comments describe what needs to be done to allow views,
  // constraints and UDR's to be created based on schema privileges. 
  //
  // - For REVOKE only: get the list of all underlying tables for RI constraints 
  //   referenced in the schema.  For each constraint, recalculate privileges 
  //   based on object, column, and adjusted schema level.  If no longer have 
  //   REFERENCES privilege  -  for restrict behavior cause the operation to 
  //   fail and for cascade behavior, drop the constraint.
  //
  // - For REVOKE only: get the list of UDRs associated with all library objects 
  //   referenced in the schema. For each UDR, recalculate privileges based on 
  //   object, column, and adjusted schema level.  If no longer have USAGE 
  //   privilege - for restrict, cause the operation to fail and for cascade, 
  //   drop the UDR.
  //
  // - For both GRANT and REVOKE: get list of all views that reference an object 
  //   in the schema.  For each view find any views references this first 
  //   set of views, then find any views that reference the next set of views 
  //   until there are no more views (down to base table).  For each view, 
  //   recalculate privileges based on object, column, and adjusted schema 
  //   level.  For GRANT add the item to the list of affected objects. For
  //   REVOKE if no longer have SELECT privilege - restrict, cause the
  //   the operation to fail and cascade, drop the view.
  //
  // - For GRANT: Once the list of affected objects is determined update the 
  //   affected rows - this would be the list produced by views granting 
  //   additional privileges to referencing views. 
  //
  return STATUS_GOOD;
}


// *****************************************************************************
// * Method: convertPrivsToDesc                                
// *                                                       
// *    Converts the list of requested privileges into a PrivMgrDesc
// *    This code also checks for duplicate entries in the privilege list
// *    and that the list of privileges is compatible with the schema type.
// *                                                       
// *  Parameters:    
// *                                                                       
// *  <isAllSpecified> if true then all privileges valid for the schema
// *                        type will be revoked
// *  <isWGOSpecified> for grants, include the with grant option
// *                   for revokes (always true), if revoke the priv, also
// *                   revoke the with grant option
// *  <isGofSpecified> for grants - alway false (not applicable)
// *                   for revokes - only revoke the with grant option
// *  <privsList> is the list of privileges to check
// *  <PrivMgrCoreDesc>  the core descriptor containing privileges
// *                                                                     
// *****************************************************************************
bool PrivMgrSchemaPrivileges::convertPrivsToDesc( 
  const bool isAllSpecified,
  const bool isWgoSpecified,
  const bool isGofSpecified,
  const std::vector<PrivType> privsList,
  PrivMgrDesc &privsToProcess)
{

  // If all is specified, set bits appropriate for the object type and return
  if (isAllSpecified)
  {
    privsToProcess.setAllSchemaGrantPrivileges(isWgoSpecified, true /*all_dml*/, true/*all_ddl*/);
    return true;
  }

  PrivMgrCoreDesc schemaCorePrivs;

  // For each privilege specified in the privsList:
  //    remove duplicates
  //    make sure it is an appropriate type
  for (int32_t i = 0; i < privsList.size();++i)
  {
    if ((privsList[i] >= FIRST_DML_PRIV && privsList[i] <= LAST_DML_PRIV) ||
         privsList[i] >= FIRST_DDL_PRIV && privsList[i] <= LAST_DDL_PRIV)
      schemaCorePrivs.testAndSetBit(privsList[i],isWgoSpecified,isGofSpecified);
    else
    {
        *pDiags_ << DgSqlCode(-CAT_PRIVILEGE_NOT_ALLOWED_FOR_THIS_OBJECT_TYPE)
                  << DgString0(PrivMgrUserPrivs::convertPrivTypeToLiteral(privsList[i]).c_str());
        return false;
    }
  } // end for

  privsToProcess.setSchemaPrivs(schemaCorePrivs);
  return true;
}


// *****************************************************************************
//    PrivMgrSchemaPrivileges.cpp static functions                                   *
// *****************************************************************************


// *****************************************************************************
// * Function: deleteRowList                                                 
// *    Deletes elements from a vector of PrivMgrMDRows.                    
// *                                                                       
// *  Parameters:                                                         
// *                                                                     
// *  <rowList> list of rows to delete.                                 
// *                                                                   
// *****************************************************************************
static void deleteRowList(std::vector<PrivMgrMDRow *> & rowList)
{
  while(!rowList.empty())
    delete rowList.back(), rowList.pop_back();
}


// *****************************************************************************
// method: reportPrivWarnings
//
// Ansi states that when a grant statement is executed, a set of privilege 
// descriptors (CPD) is created based on existing privileges for the object and 
// object’s columns. Each CPD contains the grantee, action (privileges), object, 
// column and grantor. A similar list of privilege descriptors is created based 
// on the grant/revoke statement (GPD).
//
// If there is an element in the GPD (what the user requested) that is not in 
// the  CPD (what was actually granted/revoked), then a warning – privilege not 
// granted/revoked is displayed.
// 
// This method compares the list of actual privileges granted/revoked 
// (actualPrivs)to the list privileges requested (origPrivs).  If a privilege 
// was requested but not granted/revoked report a warning.
// *****************************************************************************
void PrivMgrSchemaPrivileges::reportPrivWarnings(
    const PrivMgrDesc &origPrivs,
    const PrivMgrDesc &actualPrivs,
    const CatErrorCode warningCode)
{
  PrivMgrCoreDesc objPrivsNotApplied = origPrivs.getSchemaPrivs();
  objPrivsNotApplied.suppressDuplicatedPrivs(actualPrivs.getSchemaPrivs());
  if (!objPrivsNotApplied.isNull())
  {
    for ( size_t i = FIRST_DML_PRIV; i <= LAST_DML_PRIV; i++ )
    {
      PrivType privType = PrivType(i);
      if (objPrivsNotApplied.getPriv(privType))
      {
        *pDiags_ << DgSqlCode(warningCode)
                 << DgString0(PrivMgrUserPrivs::convertPrivTypeToLiteral(privType).c_str());
      }
    }
  }
}

// *****************************************************************************
//    SchemaPrivsMDRow methods
// *****************************************************************************

void SchemaPrivsMDRow::describeRow (std::string &rowDetails)
{
  rowDetails = "SCHEMA_PRIVILEGES row: UID is ";
  rowDetails += to_string((long long int) schemaUID_);
  rowDetails += ", grantor is ";
  rowDetails += to_string((long long int)grantorID_);
  rowDetails += ", grantee is ";
  rowDetails += to_string((long long int) granteeID_);
}


// *****************************************************************************
//    SchemaPrivsMDTable methods
// *****************************************************************************

// *****************************************************************************
// * method: SchemaPrivsMDTable::selectWhereUnique
// *                                      
// *  Select the row from the SCHEMA_PRIVILEGES table based on the specified
// *  WHERE clause - where clause should only return a single row
// *                                                                 
// *  Parameters:                                                   
// *                                                               
// *  <whereClause> is the WHERE clause specifying a unique row.             
// *  <rowOut>  passes back a set of SCHEMA_PRIVILEGES rows
// *                                                         
// * Returns: PrivStatus                                   
// *                                                      
// * STATUS_GOOD: Row returned.                          
// *           *: Select failed. A CLI error is put into the diags area. 
// *****************************************************************************
PrivStatus SchemaPrivsMDTable::selectWhereUnique(
   const std::string & whereClause,
   PrivMgrMDRow & rowOut)
{
   SchemaPrivsMDRow & row = static_cast<SchemaPrivsMDRow &>(rowOut);

   PrivStatus retcode = STATUS_GOOD;
   // Should space be allocated and deleted from the heap for the rowList?
   //   -- how many rows will be returned?
   std::vector<PrivMgrMDRow* > rowList;
   std::string orderByClause;
   retcode = selectWhere(whereClause, orderByClause, rowList);
   if (retcode == STATUS_GOOD)
   {
     // The I/O should be performed on a primary key so only one row returned
     // If not, return an internal error
     if (rowList.size() != 1)
     {
       while(!rowList.empty())
         delete rowList.back(), rowList.pop_back();
       PRIVMGR_INTERNAL_ERROR("Select unique for object_privileges table returned more than 1 row");
       return STATUS_ERROR;
     }
     row = static_cast<SchemaPrivsMDRow &>(*rowList[0]);
   }
   while(!rowList.empty())
     delete rowList.back(), rowList.pop_back();
   return retcode;
}

// *****************************************************************************
// * method: SchemaPrivsMDTable::selectWhere
// *                                      
// *  Selects rows from the SCHEMA_PRIVILEGES table based on the specified
// *  WHERE clause.                                                    
// *                                                                 
// *  Parameters:                                                   
// *                                                               
// *  <whereClause> is the WHERE clause
// *  <orderByClause> is the ORDER BY clause defining returned row order.
// *  <rowOut>  passes back a set of SCHEMA_PRIVILEGES rows
// *                                                         
// * Returns: PrivStatus                                   
// *                                                      
// * STATUS_GOOD: Row returned.                          
// *           *: Select failed. A CLI error is put into the diags area. 
// *****************************************************************************
PrivStatus SchemaPrivsMDTable::selectWhere(
   const std::string & whereClause,
  const std::string & orderByClause,
   std::vector<PrivMgrMDRow *> &rowList)
{
  std::string selectStmt ("SELECT SCHEMA_UID, SCHEMA_NAME, ");
  selectStmt += ("GRANTEE_ID, GRANTEE_NAME, ");
  selectStmt += ("GRANTOR_ID, GRANTOR_NAME, ");
  selectStmt += ("PRIVILEGES_BITMAP, GRANTABLE_BITMAP FROM ");
  selectStmt += tableName_;
  selectStmt += " ";
  selectStmt += whereClause;
  selectStmt += orderByClause;

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
    CmpCommon::context()->sqlSession()->getParentQid());
  Queue * tableQueue = NULL;
  int32_t cliRC =  cliInterface.fetchAllRows(tableQueue, (char *)selectStmt.c_str(), 0, false, false, true);

  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return STATUS_ERROR;
    }
  if (cliRC == 100) // did not find the row
  {
    pDiags_->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  tableQueue->position();
  for (int idx = 0; idx < tableQueue->numEntries(); idx++)
  {
    OutputInfo * pCliRow = (OutputInfo*)tableQueue->getNext();
    SchemaPrivsMDRow *pRow = new SchemaPrivsMDRow();
    setRow(pCliRow, *pRow);
    rowList.push_back(pRow);
  }    

  return STATUS_GOOD;
}

// *****************************************************************************
// * method: SchemaPrivsMDTable::setRow
// *                                      
// *  Create an SchemaPrivsMDRow object from the information returned from the
// *  cli.
// *                                                                 
// *  Parameters:                                                   
// *                                                               
// *  <OutputInfo> row destails from the cli
// *  <rowOut>  passes back the SchemaPrivsMDRow row
// *                                                         
// * no errors should be generated
// *****************************************************************************
// Row read successfully.  Extract the columns.
void SchemaPrivsMDTable::setRow (OutputInfo *pCliRow,
                                 SchemaPrivsMDRow &row)
{
  char * ptr = NULL;
  Int32 len = 0;
  char value[500];

  // column 1:  schema uid
  pCliRow->get(0,ptr,len);
  row.schemaUID_ = *(reinterpret_cast<int64_t*>(ptr));

   // column 2:  schema name
  pCliRow->get(1,ptr,len);
  assert (len < 257);
  strncpy(value, ptr, len);
  value[len] = 0;
  row.schemaName_ = value;

  // column 3: grantee uid
  pCliRow->get(2,ptr,len);
  row.granteeID_ = *(reinterpret_cast<int32_t*>(ptr));

  // column 4: grantee name
  pCliRow->get(3,ptr,len);
  assert (len < 257);
  strncpy(value, ptr, len);
  value[len] = 0;
  row.granteeName_ = value;

  // column 5: grantor uid
  pCliRow->get(4,ptr,len);
  row.grantorID_ = *(reinterpret_cast<int32_t*>(ptr));

  //column 6: grantor name
  pCliRow->get(5,ptr,len);
  assert (len < 257);
  strncpy(value, ptr, len);
  value[len] = 0;
  row.grantorName_ = value;

  // column 7: privileges bitmap   
  pCliRow->get(6,ptr,len);
  int64_t bitmapInt = *(reinterpret_cast<int64_t*>(ptr));
  row.privsBitmap_ = bitmapInt;

  // column 8: grantable bitmap
  pCliRow->get(7,ptr,len);
  bitmapInt = *(reinterpret_cast<int64_t*>(ptr));
  row.grantableBitmap_ = bitmapInt;

  // set current_
  PrivMgrCoreDesc tempDesc (row.privsBitmap_, row.grantableBitmap_);
  row.current_= tempDesc; 
  row.visited_.setAllPrivAndWgo(false);
}

// *****************************************************************************
// * method: SchemaPrivsMDTable::insert
// *                                  
// *    Inserts a row into the SCHEMA_PRIVILEGES table.     
// *                                               
// *  Parameters:                                 
// *                                             
// *  <rowIn> is a SchemaPrivsMDRow to be inserted.  
// *                                                                    
// * Returns: PrivStatus
// *                   
// * STATUS_GOOD: Row inserted. 
// *           *: Insert failed. A CLI error is put into the diags area. 
// *****************************************************************************
PrivStatus SchemaPrivsMDTable::insert(const PrivMgrMDRow &rowIn)
{

  char insertStmt[2000];
  const SchemaPrivsMDRow &row = static_cast<const SchemaPrivsMDRow &>(rowIn);

  int64_t privilegesBitmapLong = row.privsBitmap_.to_ulong();
  int64_t grantableBitmapLong = row.grantableBitmap_.to_ulong();
  
  sprintf(insertStmt, "insert into %s values (%ld, '%s', %d, '%s', %d, '%s', %ld, %ld)",
              tableName_.c_str(),
              row.schemaUID_,
              row.schemaName_.c_str(),
              row.granteeID_,
              row.granteeName_.c_str(),
              row.grantorID_,
              row.grantorName_.c_str(),
              privilegesBitmapLong,
              grantableBitmapLong);

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());
  int32_t cliRC = cliInterface.executeImmediate(insertStmt);

   if (cliRC < 0)
   {
      cliInterface.retrieveSQLDiagnostics(pDiags_);
      return STATUS_ERROR;
   }
  
   // For some reason, insert sometimes returns error even though
   // the row is inserted, so unless an errors, return STATUS_GOOD
   return STATUS_GOOD;

}

// *****************************************************************************
// * method: SchemaPrivsMDTable::deleteRow
// *                                  
// *    Deletes a row from the SCHEMA_PRIVILEGES table based on the primary key
// *    contents of the row.
// *                                               
// *  Parameters:                                 
// *                                             
// *  <row> defines what row should be deleted
// *                                                                    
// * Returns: PrivStatus
// *                   
// * STATUS_GOOD: Row deleted. 
// *           *: Insert failed. A CLI error is put into the diags area. 
// *****************************************************************************
PrivStatus SchemaPrivsMDTable::deleteRow(const SchemaPrivsMDRow & row)

{

char whereClause[1000];

   sprintf(whereClause," WHERE schema_uid = %ld AND grantor_id = %d AND grantee_id = %d ",
           row.schemaUID_,row.grantorID_,row.granteeID_);
           
   return deleteWhere(whereClause);

}


// *****************************************************************************
// * method: SchemaPrivsMDTable::deleteWhere
// *                                  
// *    Deletes a row from the SCHEMA_PRIVILEGES table based on the where clause
// *                                               
// *  Parameters:                                 
// *                                             
// *  <whereClause> defines what rows should be deleted
// *                                                                    
// * Returns: PrivStatus
// *                   
// * STATUS_GOOD: Row(s) deleted. 
// *           *: Insert failed. A CLI error is put into the diags area. 
// *****************************************************************************
PrivStatus SchemaPrivsMDTable::deleteWhere(const std::string & whereClause)
{
  std::string deleteStmt ("DELETE FROM ");
  deleteStmt += tableName_;
  deleteStmt += " ";
  deleteStmt += whereClause;

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());

  int32_t cliRC = cliInterface.executeImmediate(deleteStmt.c_str());
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return STATUS_ERROR;
    }


  if (cliRC == 100) // did not find any rows
  {
    pDiags_->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  if (cliRC > 0)
    return STATUS_WARNING;

  return STATUS_GOOD;
}


// *****************************************************************************
// * method: SchemaPrivsMDTable::updateRow
// *                                  
// *    Updates grantor and bitmaps for a row in the SCHEMA_PRIVILEGES table 
// *    based on the contents of the row.
// *                                               
// *  Parameters:                                 
// *                                             
// *  <row> defines what row should be updated
// *                                                                    
// * Returns: PrivStatus
// *                   
// * STATUS_GOOD: Row(s) deleted. 
// *           *: Insert failed. A CLI error is put into the diags area. 
// *****************************************************************************
PrivStatus SchemaPrivsMDTable::updateRow(
   const SchemaPrivsMDRow & row,
   int64_t &rowCount)
{
  char buf[500];

  sprintf(buf," SET privileges_bitmap = %ld, grantable_bitmap = %ld ",
          row.privsBitmap_.to_ulong(),row.grantableBitmap_.to_ulong());
  std::string setClause (buf);

  sprintf(buf," WHERE schema_uid = %ld AND grantor_id = %d AND grantee_id = %d ",
          row.schemaUID_,row.grantorID_,row.granteeID_);
  std::string whereClause(buf);

  return updateWhere(setClause,whereClause, rowCount);
}

// ******************************************************************************
// method: updateWhere
//
// This method updates one or more rows from the SCHEMA_PRIVILEGES table
// The number of rows affected depend on the passed in set clause
//
// Input:  setClause
//         whereClause
// Output:  number rows updated
//          status of the operation
//
// The ComDiags area is populated in case of an unexpected error
// ******************************************************************************
PrivStatus SchemaPrivsMDTable::updateWhere(
  const std::string & setClause,
  const std::string & whereClause,
  int64_t &rowCount)
{
  std::string updateStmt ("UPDATE ");
  updateStmt += tableName_;
  updateStmt += " ";
  updateStmt += setClause;
  updateStmt += " ";
  updateStmt += whereClause;

  // set pointer in diags area
  int32_t diagsMark = pDiags_->mark();

  rowCount = 0;
  ExeCliInterface cliInterface(STMTHEAP, 0, NULL, 
  CmpCommon::context()->sqlSession()->getParentQid());
  int32_t cliRC = cliInterface.executeImmediate(updateStmt.c_str(), NULL, NULL, TRUE, &rowCount);
  if (cliRC < 0)
    {
      cliInterface.retrieveSQLDiagnostics(CmpCommon::diags());
      return STATUS_ERROR;
    }

  // update can return error 100 and successfully update rows, instead check
  // to see if rows are updated.
  if (rowCount == 0)
  {
    pDiags_->rewind(diagsMark);
    return STATUS_NOTFOUND;
  }

  return STATUS_GOOD;
}


// *****************************************************************************
// * Method: getPrivRowsForSchema                                
// *                                                       
// *    returns rows describing all the privileges that have been
// *    granted on the schema
// *                                                       
// *  Parameters:    
// *                                                                       
// *  <schemaPrivsRows> Zero or more rows of grants for the schema.
// *                                                                     
// * Returns: PrivStatus                                               
// *                                                                  
// * STATUS_GOOD    : Rows were returned
// * STATUS_NOTFOUND: No rows were returned
// *               *: Unable to read privileges, see diags.     
// *                                                               
// *****************************************************************************
PrivStatus PrivMgrSchemaPrivileges::getPrivRowsForSchema(
   const int64_t schemaUID,
   std::vector<ObjectPrivsRow> & schemaPrivsRows)
   
{
  PrivStatus retcode = STATUS_GOOD;

  if (schemaUID_ == 0)
  {
    PRIVMGR_INTERNAL_ERROR("schemaUID is 0 for getPrivRowsForSchema()");
    return STATUS_ERROR;
  }

  // generate the list of privileges granted to the object and store in class
  if (generateSchemaRowList(ROOT_USER_ID) == STATUS_ERROR)
    return STATUS_ERROR;

  for (size_t i = 0; i < schemaRowList_.size(); i++)
  {
    SchemaPrivsMDRow &row = static_cast<SchemaPrivsMDRow &> (*schemaRowList_[i]);
    if (row.grantorID_ != SYSTEM_USER)
    {
      ObjectPrivsRow newRow;
    
      strcpy(newRow.objectName,row.schemaName_.c_str());
      newRow.objectType = COM_PRIVATE_SCHEMA_OBJECT;
      newRow.granteeID = row.granteeID_;
      strcpy(newRow.granteeName,row.granteeName_.c_str());
      newRow.grantorID = row.grantorID_;
      strcpy(newRow.grantorName,row.grantorName_.c_str());
      newRow.privilegesBitmap = row.privsBitmap_.to_ulong();
      newRow.grantableBitmap = row.grantableBitmap_.to_ulong();
    
      schemaPrivsRows.push_back(newRow);
    }
  }

  return retcode;
}

// *****************************************************************************
// * Method: isAuthIDGrantedPrivs                                
// *                                                       
// *    Determines if the specified authorization ID has been granted one or   
// * more schema privileges.                                                
// *                                                       
// *  Parameters:    
// *                                                                       
// *  IN:  <authID> identifies the user or role.
// *  OUT: <objectUIDs> list of objects that have been granted privileges
// *                                                                     
// * Returns: bool                                               
// *                                                                  
// *  true: Authorization ID has been granted one or more object privileges.
// * false: Authorization ID has not been granted any object privileges.     
// *                                                               
// *****************************************************************************
bool PrivMgrSchemaPrivileges::isAuthIDGrantedPrivs(
  const int32_t authID,
  std::vector<int64_t> &objectUIDs)
{
   std::string whereClause(" WHERE GRANTEE_ID = ");
   std::string orderByClause (" ORDER BY schema_name ");

   char authIDString[20];

   sprintf(authIDString,"%d",authID);

   whereClause += authIDString;

   // set pointer in diags area
   int32_t diagsMark = pDiags_->mark();

   std::vector<PrivMgrMDRow *> rowList;
   SchemaPrivsMDTable myTable(schemaTableName_,pDiags_);

   PrivStatus privStatus = myTable.selectWhere(whereClause, orderByClause ,rowList);
   if (privStatus == STATUS_NOTFOUND ||
       (privStatus == STATUS_GOOD && rowList.size() == 0))
   {
      deleteRowList(rowList);
      return false;
   }
   
   if (privStatus != STATUS_GOOD)
   {
      deleteRowList(rowList);
      return true;
   }

   int32_t rowCount = rowList.size();
   for (size_t i = 0; i < rowCount; i++)
   {
      SchemaPrivsMDRow &row = static_cast<SchemaPrivsMDRow &> (*rowList[i]);
      objectUIDs.push_back(row.schemaUID_);
   }
   deleteRowList(rowList);

   // TBD - check for granted column level privs
   pDiags_->rewind(diagsMark);
   return true;
}


