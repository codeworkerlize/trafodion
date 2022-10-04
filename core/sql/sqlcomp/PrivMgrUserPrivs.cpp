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

// ==========================================================================
// Contains non inline methods in the following classes
//   PrivMgrObjectInfo
//   PrivMgrCommands
// ==========================================================================

#include "sqlcomp/PrivMgrCommands.h"
#include "sqlcomp/PrivMgrMD.h"
#include "common/DgBaseType.h"
#include "optimizer/NATable.h"
#include "optimizer/NAColumn.h"
#include "common/ComSecurityKey.h"
#include "common/ComUser.h"
#include "common/ComMisc.h"
#include "sqlcomp/CmpSeabaseDDL.h"
#include <cstdio>
#include <algorithm>

// ****************************************************************************
// Class: PrivMgrObjectInfo
// ****************************************************************************
PrivMgrObjectInfo::PrivMgrObjectInfo(const NATable *naTable)
    : objectOwner_(naTable->getOwner()),
      schemaOwner_(naTable->getSchemaOwner()),
      objectUID_(naTable->objectUid().get_value()),
      objectType_(naTable->getObjectType()) {
  // Map hbase map table to external name
  if (ComIsHBaseMappedIntFormat(naTable->getTableName().getCatalogName(), naTable->getTableName().getSchemaName())) {
    NAString newCatName;
    NAString newSchName;
    ComConvertHBaseMappedIntToExt(naTable->getTableName().getCatalogName(), naTable->getTableName().getSchemaName(),
                                  newCatName, newSchName);
    CONCAT_CATSCH(objectName_, newCatName, newSchName);
    objectName_ += std::string(".") + naTable->getTableName().getUnqualifiedObjectNameAsAnsiString().data();
  } else
    objectName_ = naTable->getTableName().getQualifiedNameAsAnsiString().data();

  const NAColumnArray &colNameArray = naTable->getNAColumnArray();
  for (size_t i = 0; i < colNameArray.entries(); i++) {
    const NAColumn *naCol = colNameArray.getColumn(i);
    std::string columnName(naCol->getColName().data());
    columnList_.push_back(columnName);
  }
}

// ****************************************************************************
// Class: PrivMgrUserPrivs
// ****************************************************************************

// ----------------------------------------------------------------------------
// method: initUserPrivs
//
// Creates a PrivMgrUserPrivs object from a PrivMgrDescs list
// ----------------------------------------------------------------------------
bool PrivMgrUserPrivs::initUserPrivs(const NAList<Int32> &roleIDs, PrivMgrDescList *privDescs, const int32_t userID,
                                     const int64_t objectUID, ComSecurityKeySet *secKeySet) {
  hasPublicPriv_ = false;

  // create a subset of PrivDesc containing privs applicable for the userID
  PrivMgrDescList userDescList(privDescs->getHeap());
  for (CollIndex i = 0; i < privDescs->entries(); i++) {
    const PrivMgrDesc *privs = privDescs->operator[](i);
    Int32 grantee = privs->getGrantee();
    bool addDesc = false;
    if (ComUser::isPublicUserID(grantee)) {
      hasPublicPriv_ = true;
      addDesc = true;
    } else if (grantee == userID)
      addDesc = true;

    else if (PrivMgr::isRoleID(grantee)) {
      Int32 entry;
      if (roleIDs.find(grantee, entry)) addDesc = true;
    }
    if (addDesc) {
      PrivMgrDesc *myPrivs = new (userDescList.getHeap()) PrivMgrDesc(*privs);
      userDescList.insert(myPrivs);
    }
  }

  if (userDescList.entries() > 0) return setPrivInfoAndKeys(userDescList, userID, objectUID, secKeySet);
  return true;
}

// ----------------------------------------------------------------------------
// method: initUserPrivs
//
// Creates a PrivMgrUserPrivs object from a PrivMgrDesc object
// ----------------------------------------------------------------------------
void PrivMgrUserPrivs::initUserPrivs(PrivMgrDesc &privsOfTheUser) {
  objectBitmap_ = privsOfTheUser.getTablePrivs().getPrivBitmap();
  grantableBitmap_ = privsOfTheUser.getTablePrivs().getWgoBitmap();

  for (int32_t i = 0; i < privsOfTheUser.getColumnPrivs().entries(); i++) {
    const int32_t columnOrdinal = privsOfTheUser.getColumnPrivs()[i].getColumnOrdinal();
    colPrivsList_[columnOrdinal] = privsOfTheUser.getColumnPrivs()[i].getPrivBitmap();
    colGrantableList_[columnOrdinal] = privsOfTheUser.getColumnPrivs()[i].getWgoBitmap();
  }
  hasPublicPriv_ = privsOfTheUser.getHasPublicPriv();
}

// ----------------------------------------------------------------------------
// method: setPrivInfoAndKeys
//
// Creates a security keys for a list of PrivMgrDescs
// ----------------------------------------------------------------------------
bool PrivMgrUserPrivs::setPrivInfoAndKeys(PrivMgrDescList &descList, const int32_t userID, const int64_t objectUID,
                                          NASet<ComSecurityKey> *secKeySet) {
  // Get the list of roleIDs and grantees from cache
  NAList<int32_t> roleIDs(descList.getHeap());
  NAList<int32_t> grantees(descList.getHeap());
  if (ComUser::getCurrentUserRoles(roleIDs, grantees, NULL /*don't update diags*/) != 0) return false;

  for (int i = 0; i < descList.entries(); i++) {
    PrivMgrDesc *privs = descList[i];

    // Set up schema level privileges
    if (privs->isSchemaObject()) {
      schemaPrivBitmap_ |= privs->getSchemaPrivs().getPrivBitmap();
      schemaGrantableBitmap_ |= privs->getSchemaPrivs().getWgoBitmap();
    } else {
      // Set up schema level privileges
      schemaPrivBitmap_ |= privs->getSchemaPrivs().getPrivBitmap();
      schemaGrantableBitmap_ |= privs->getSchemaPrivs().getWgoBitmap();

      // Set up object level privileges
      objectBitmap_ |= privs->getTablePrivs().getPrivBitmap();
      grantableBitmap_ |= privs->getTablePrivs().getWgoBitmap();

      // Set up column level privileges
      NAList<PrivMgrCoreDesc> columnPrivs = privs->getColumnPrivs();
      std::map<size_t, PrivColumnBitmap>::iterator it;
      for (int j = 0; j < columnPrivs.entries(); j++) {
        PrivMgrCoreDesc colDesc = columnPrivs[j];
        Int32 columnOrdinal = colDesc.getColumnOrdinal();
        it = colPrivsList_.find(columnOrdinal);
        if (it == colPrivsList_.end()) {
          colPrivsList_[columnOrdinal] = colDesc.getPrivBitmap();
          colGrantableList_[columnOrdinal] = colDesc.getWgoBitmap();
        } else {
          colPrivsList_[columnOrdinal] |= colDesc.getPrivBitmap();
          colGrantableList_[columnOrdinal] |= colDesc.getWgoBitmap();
        }
      }
    }

    // set up security invalidation keys
    Int32 grantee = privs->getGrantee();
    NAList<Int32> roleGrantees(descList.getHeap());

    // If the grantee is a role, then get all users and user's groups that
    // have been granted the role.  Create a security key for each.
    if (PrivMgr::isRoleID(grantee)) {
      for (Int32 j = 0; j < grantees.entries(); j++) {
        if (grantee == roleIDs[j]) roleGrantees.insert(grantees[j]);
      }
    }

    if (secKeySet == NULL) return true;

    if (privs->isSchemaObject()) {
      if (!buildSecurityKeys(roleGrantees, grantee, objectUID, privs->isSchemaObject(), false, privs->getSchemaPrivs(),
                             *secKeySet))
        return false;
    } else {
      // add schema security keys
      if (!buildSecurityKeys(roleGrantees, grantee, privs->getSchemaUID(), true, false, privs->getSchemaPrivs(),
                             *secKeySet))
        return false;

      // add object security keys
      if (!buildSecurityKeys(roleGrantees, grantee, objectUID, privs->isSchemaObject(), false, privs->getTablePrivs(),
                             *secKeySet))
        return false;

      // add column security keys
      NAList<PrivMgrCoreDesc> colPrivs = privs->getColumnPrivs();
      for (int k = 0; k < colPrivs.entries(); k++) {
        Int32 columnOrdinal = colPrivs[k].getColumnOrdinal();
        PrivMgrCoreDesc colDesc(colPrivsList_[columnOrdinal], colGrantableList_[columnOrdinal]);
        if (!buildSecurityKeys(roleGrantees, grantee, objectUID, privs->isSchemaObject(), true, colDesc, *secKeySet))
          return false;
      }
    }
  }

  return true;
}

// ----------------------------------------------------------------------------
// method: setGetPrivilegesTime
//
// Intended to be called only by PrivMgrCommands::getPrivileges; writes the
// current time into the getPrivilegesTime_ member.
// ----------------------------------------------------------------------------
void PrivMgrUserPrivs::setGetPrivilegesTime() {
  int result = gettimeofday(&getPrivilegesTime_, NULL);
  if (result != 0)  // gettimeofday failed?!??? shouldn't happen
  {
    int saveErrNo = errno;  // save it before it changes!
    std::string traceMsg = "PrivMgrUserPrivs::setGetPrivilegesTime gettimeofday call returned errno = ";
    traceMsg += to_string((long long int)saveErrNo);
    PrivMgr::log(__FILE__, traceMsg.c_str(), -1);

    getPrivilegesTime_.tv_sec = 0;
    getPrivilegesTime_.tv_usec = 0;
  }
}

// ----------------------------------------------------------------------------
// method: isStale
//
// Determines if this PrivMgrUserPrivs object is stale, according to criteria
// provided by the caller.
// ----------------------------------------------------------------------------
bool PrivMgrUserPrivs::isStale(struct timeval *timeBase, int32_t incrementInSeconds) {
  // an unset getPrivilegesTime_ is always considered stale
  if ((getPrivilegesTime_.tv_sec == 0) && (getPrivilegesTime_.tv_usec == 0)) return TRUE;

  if (timeBase->tv_sec + incrementInSeconds > getPrivilegesTime_.tv_sec) return TRUE;

  if ((timeBase->tv_sec + incrementInSeconds == getPrivilegesTime_.tv_sec) &&
      (timeBase->tv_usec > getPrivilegesTime_.tv_usec))
    return TRUE;

  return FALSE;
}

// ----------------------------------------------------------------------------
// method: reInit
//
// Reinitializes the object to its initial (constructor-time) state, so it
// can be reused in a getPrivileges call.
// ----------------------------------------------------------------------------
void PrivMgrUserPrivs::reInit() {
  // reset all the data members to constructor-time state
  getPrivilegesTime_.tv_sec = 0;
  getPrivilegesTime_.tv_usec = 0;
  objectBitmap_.reset();
  grantableBitmap_.reset();
  colPrivsList_.clear();
  colGrantableList_.clear();
  schemaPrivBitmap_.reset();
  schemaGrantableBitmap_.reset();
  emptyBitmap_.reset();
  hasPublicPriv_ = false;
}

// -----------------------------------------------------------------------------
// Method:: print
//
// Prints out the bitmaps for the current user
// -----------------------------------------------------------------------------
std::string PrivMgrUserPrivs::print() {
  std::string privList("Obj: ");
  privList += objectBitmap_.to_string<char, std::string::traits_type, std::string::allocator_type>();

  privList += ", Col: ";
  Int32 bufSize = 100;
  char buf[bufSize];
  for (size_t i = 0; i < colPrivsList_.size(); i++) {
    std::string bits = colPrivsList_[i].to_string<char, std::string::traits_type, std::string::allocator_type>();
    // Ignore potential buffer overruns
    snprintf(buf, bufSize, "%d %s ", (int)i, bits.c_str());
    privList += buf;
  }

  privList += ", Sch: ";
  privList += schemaPrivBitmap_.to_string<char, std::string::traits_type, std::string::allocator_type>();
  return privList;
}
