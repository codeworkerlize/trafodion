/**********************************************************************
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
**********************************************************************/

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpSeabaseBackupAttr.cpp
 * Description:  Implements methods to store and retrieve a set of attributes
 *               from Backup metadata.
 *
 *               These attributes can be used to test various attributes
 *               at restore time to verify that the restore or other backup
 *               related request can be completed successfully.
 *
 *  TBD:
 *    do we need to save other things such license bits - e.g. multi-tenancy?
 *****************************************************************************
 */

#include "sqlcomp/CmpSeabaseDDL.h"
#include "sqlcomp/CmpSeabaseBackupAttrs.h"

#define KVKey   "KEY"
#define KVValue "VAL"
#define KVDelim "+"

class BackupAttr;
class BackupAttrList;

// ----------------------------------------------------------------------------
// method: convertEnumToLiteral
//
// Converts the key type enum identifying the attribute to a literal.
// ----------------------------------------------------------------------------
NAString BackupAttr::convertEnumToLiteral(BACKUP_ATTR_KEY enumVal) {
  NAString litVal(BACKUP_ATTR_UNKNOWN_LIT);
  switch (enumVal) {
    case BACKUP_ATTR_ENTRIES:
      litVal = BACKUP_ATTR_ENTRIES_LIT;
      break;
    case BACKUP_ATTR_OWNER:
      litVal = BACKUP_ATTR_OWNER_LIT;
      break;
    case BACKUP_ATTR_VERSION_DB:
      litVal = BACKUP_ATTR_VERSION_DB_LIT;
      break;
    case BACKUP_ATTR_VERSION_MD:
      litVal = BACKUP_ATTR_VERSION_MD_LIT;
      break;
    case BACKUP_ATTR_VERSION_PR:
      litVal = BACKUP_ATTR_VERSION_PR_LIT;
      break;
    case BACKUP_ATTR_USER_ROLES:
      litVal = BACKUP_ATTR_USER_ROLES_LIT;
      break;
    default:
      litVal = BACKUP_ATTR_UNKNOWN_LIT;
  }

  return litVal;
}

// ----------------------------------------------------------------------------
// method: convertLiteralToEnum
//
// Converts the key type literal stored in the backup metadata to an enum
// ----------------------------------------------------------------------------
BackupAttr::BACKUP_ATTR_KEY BackupAttr::convertLiteralToEnum(const char *litVal) {
  if (strcmp(litVal, BACKUP_ATTR_ENTRIES_LIT) == 0) return BACKUP_ATTR_ENTRIES;
  if (strcmp(litVal, BACKUP_ATTR_OWNER_LIT) == 0) return BACKUP_ATTR_OWNER;
  if (strcmp(litVal, BACKUP_ATTR_VERSION_DB_LIT) == 0) return BACKUP_ATTR_VERSION_DB;
  if (strcmp(litVal, BACKUP_ATTR_VERSION_MD_LIT) == 0) return BACKUP_ATTR_VERSION_MD;
  if (strcmp(litVal, BACKUP_ATTR_VERSION_PR_LIT) == 0) return BACKUP_ATTR_VERSION_PR;
  if (strcmp(litVal, BACKUP_ATTR_USER_ROLES_LIT) == 0) return BACKUP_ATTR_USER_ROLES;
  return BACKUP_ATTR_UNKNOWN;
}

// ----------------------------------------------------------------------------
// method:  add Owner
//
// Add the owner of the backup (person who executed the backup request) to
// the list.
// ----------------------------------------------------------------------------
void BackupAttrList::addOwner(const char *owner) {
  BackupAttr *ownerAttr = new (getHeap()) BackupAttr(BackupAttr::BACKUP_ATTR_OWNER, owner, getHeap());
  insert(ownerAttr);
}

void BackupAttrList::addUserAndRoles(const char *userAndRoles) {
  BackupAttr *urAttr = new (getHeap()) BackupAttr(BackupAttr::BACKUP_ATTR_USER_ROLES, userAndRoles, getHeap());
  insert(urAttr);
}

// ----------------------------------------------------------------------------
// method: addVersions
//
// This code stores the current product versions as attributes in the list.
// It includes:
//    Database version - the version of the software base
//    Metadata version - the version where metadata was last upgraded
//    Product version  - the version of the product such as core bank/data lake
//
// Returns:
//      0 - successful
//     -1 - unexpected error returned, unable to retrieve version info
// ----------------------------------------------------------------------------
short BackupAttrList::addVersions() {
  // get version string
  VersionInfo vInfo;
  if (CmpSeabaseDDL::getVersionInfo(vInfo) != 0) return -1;

  BackupAttr *dbVersion =
      new (getHeap()) BackupAttr(BackupAttr::BACKUP_ATTR_VERSION_DB, vInfo.dbVersionStr_.data(), getHeap());
  insert(dbVersion);

  // metadata version
  BackupAttr *mdVersion =
      new (getHeap()) BackupAttr(BackupAttr::BACKUP_ATTR_VERSION_MD, vInfo.mdVersionStr_.data(), getHeap());
  insert(mdVersion);

  // product version
  BackupAttr *prVersion =
      new (getHeap()) BackupAttr(BackupAttr::BACKUP_ATTR_VERSION_PR, vInfo.prVersionStr_.data(), getHeap());
  insert(prVersion);

  return 0;
}

// ----------------------------------------------------------------------------
// Method: getAttrByKey
//
// Searches the attribute list for the specified key.
//
// returns NULL if attribute is not found
// ----------------------------------------------------------------------------
char *BackupAttrList::getAttrByKey(const BackupAttr::BACKUP_ATTR_KEY attrKey) {
  for (CollIndex i = 0; i < entries(); i++) {
    BackupAttr *attr = operator[](i);
    if (attr->getAttrKey() == attrKey) return attr->getAttrValue();
  }
  return NULL;
}

// ----------------------------------------------------------------------------
// Method pack
//
// Generates an attribute string (buffer) containing:
//   String header with the backup format
//   Number of attributes in string
//   Attributes
//
// The header should remain the same unless we decide to change the format,
// for example, change the delimiter between releases.
//
// There is delimiter ('+') after the header and each attribute
//
// Example:  "BKFormat-1.0+
//             KEY+E +VAL+4+
//             KEY+MV+VAL+2.6.0+
//             KEY+SV+VAL+2.7.1+
//             KEY+PV+VAL+1.0+
//             KEY+O +VAL+SQL_USER1+
//
// The caller passes in a buffer (char *) set to NULL.  The code creates the
// list, allocates the buffer from the specified heap, and returns the buffer.
// This way the caller does not have to know the size of the string ahead of time.
//
// returns:
//    0 - successful
//   -1 - error occurred generating list
//
// If an error is detected, errorMsg is returned with details.
// ----------------------------------------------------------------------------
short BackupAttrList::pack(char *&buffer, NAString &errorMsg) {
  if (buffer != NULL) {
    errorMsg = "internal error (backup attribute string already created).";
    return -1;
  }

  // The current BackupAttrList must contain entries
  if (entries() == 0) {
    errorMsg = "internal error (backup attribute list is empty).";
    return -1;
  }

  NAString tempBuffer;

  // Add the header (BKFormat):
  tempBuffer += BKFormat;
  tempBuffer += KVDelim;

  // Add the number of entries as the first attribute: KEY+E +VAL+4+
  char intStr[20];
  BackupAttr entryAttr(BackupAttr::BACKUP_ATTR_ENTRIES, str_itoa(entries(), intStr), getHeap());
  packIntoBuffer(tempBuffer, &entryAttr);

  // Add entries
  for (CollIndex i = 0; i < entries(); i++) {
    const BackupAttr *backupAttr = operator[](i);
    packIntoBuffer(tempBuffer, (BackupAttr *)backupAttr);
  }

  // return buffer
  buffer = new (getHeap()) char[tempBuffer.length() + 1];
  strcpy(buffer, tempBuffer.data());

  return 0;
}

// ----------------------------------------------------------------------------
// method:  packIntoBuffer
//
// Helper method to setup a key/value pair
// ----------------------------------------------------------------------------
void BackupAttrList::packIntoBuffer(NAString &buffer, BackupAttr *attr) {
  // KEY+key+
  buffer += KVKey;
  buffer += KVDelim;
  buffer += attr->convertEnumToLiteral(attr->getAttrKey());
  buffer += KVDelim;

  // VAL+value+
  buffer += KVValue, buffer += KVDelim;
  buffer += attr->getAttrValue();
  buffer += KVDelim;
}

// ----------------------------------------------------------------------------
// Unpack the attribute string and creates a list of BackupAttrs
//
// Returns
//     0 - successful
//    -1 - unable to convert SQL string
//
// If an error is detected, errorMsg is returned with details.
// ----------------------------------------------------------------------------
short BackupAttrList::unpack(const char *buffer, NAString &errorMsg) {
  // Should be an empty list
  if (entries() != 0) {
    errorMsg = "internal error (backup attribute list already exists).";
    return -1;
  }

  // Verify that the passed in string contains data
  if (!buffer) {
    errorMsg = "internal error (backup attribute string is empty).";
    return -1;
  }

  char *ptr = (char *)buffer;
  char *nxt = ptr;

  // Unpack the format header
  nxt = strstr(ptr, KVDelim);
  if (nxt == NULL) {
    errorMsg = "format header missing from backup attribute string.";
    return -1;
  }

  NAString bkFormat(ptr, nxt - ptr);
  setBKFormat(bkFormat);
  ptr = nxt + strlen(KVDelim);

  // the first Key/value pair must be the number of entries ('E ')
  BackupAttr *entriesAttr = NULL;
  ptr = unpackFromBuffer(ptr, entriesAttr);
  if (ptr == NULL || entriesAttr == NULL || entriesAttr->attrInvalid()) {
    errorMsg = "internal error (backup attribute string is corrupt).";
    return -1;
  }

  if (entriesAttr->getAttrKey() != BackupAttr::BACKUP_ATTR_ENTRIES) {
    errorMsg += "internal error (number of entries missing from backup attribute string).";
    return -1;
  }

  // insert user defined key/value pairs
  CollIndex entries = atoi(entriesAttr->getAttrValue());
  if (entries == 0) {
    errorMsg += "internal error (no attributes found in backup attribute string)";
    return -1;
  }

  for (CollIndex i = 0; i < entries; i++) {
    if (strncmp(ptr, KVDelim, strlen(KVDelim) != 0)) {
      errorMsg += "internal error (unable to parse backup string, separater is missing).";
      return -1;
    }
    ptr += strlen(KVDelim);
    BackupAttr *attr = NULL;
    ptr = unpackFromBuffer(ptr, attr);
    if (ptr == NULL || attr->attrInvalid()) {
      errorMsg += "internal error (unable to parse backup attribute string).";
      return -1;
    }
    insert(attr);
  }
  return 0;
}

// ----------------------------------------------------------------------------
// method:  unpackFromBuffer
//
// Creates a BackupAttr
//   buffer points to next: "KEY+key+VAL+value+" entry
//
// Returns:
//    char * - next entry in the buffer or
//    NULL   - error found unpacking the buffer
// ----------------------------------------------------------------------------
char *BackupAttrList::unpackFromBuffer(const char *buffer, BackupAttr *&attr) {
  char *ptr = (char *)buffer;
  char *nxt = (char *)buffer;

  // Verify keyword KVKey: ptr-> KEY
  if (strncmp(ptr, KVKey, strlen(KVKey)) != 0) return NULL;
  ptr = ptr + (strlen(KVKey) + strlen(KVDelim));
  nxt = strstr(ptr, KVDelim);
  if (nxt == NULL) return NULL;

  // Store key: ptr -> key  nxt -> +VAL
  NAString keyStr(ptr, nxt - ptr);
  ptr = nxt + strlen(KVDelim);
  nxt = strstr(ptr, KVDelim);
  if (nxt == NULL) return NULL;

  // Verify keyword KVValue: ptr -> VAL  nxt -> +value
  if (strncmp(ptr, KVValue, strlen(KVValue)) != 0) return NULL;
  ptr = nxt + strlen(KVDelim);
  nxt = strstr(ptr, KVDelim);
  if (nxt == NULL) return NULL;

  // Store value: ptr -> value  nxt -> + (either next entry or end of buffer)
  NAString valStr(ptr, nxt - ptr);
  char *value = new (getHeap()) char[valStr.length() + 1];
  strcpy(value, valStr.data());

  attr = new (getHeap()) BackupAttr((attr->convertLiteralToEnum(keyStr.data())), value, getHeap());

  return nxt;
}

// -----------------------------------------------------------------------------
// Method:  backupCompatible
//
// returns:
//    0 - versions compatible
//   -1 - DB version mismatch
//   -2 - MD version mismatch
//   -3 - PR version mismatch
// -----------------------------------------------------------------------------
short BackupAttrList::backupCompatible(VersionInfo vInfo) {
  NAString versionStr;
  const char *attrVersion = NULL;

  // Verify database version (DB)
  //   don't allow a backup taken from an Advance license be restored on a
  //   system with an Enterprise license
  attrVersion = getAttrByKey(BackupAttr::BACKUP_ATTR_VERSION_DB);
  if (attrVersion == NULL) return -1;
  versionStr = attrVersion;
  if (versionStr.contains("Advanced") && vInfo.dbVersionStr_.contains("Enterprise")) return -1;

  // Verify metadata version (MD)
  //   don't allow a backup taken on a system with a higher metadata version
  //   than the current system.
  attrVersion = getAttrByKey(BackupAttr::BACKUP_ATTR_VERSION_MD);
  if (attrVersion == NULL) return -2;

  // split version into parts
  // Skip past version header to version number
  std::string mdStr(attrVersion);
  std::string matches("0123456789");
  size_t offset = mdStr.find_first_of(matches);
  mdStr = mdStr.substr(offset);

  // major version
  char *tok = strtok((char *)mdStr.c_str(), ".");
  if (tok == NULL) return -2;
  Int64 bkValue = atol(tok);
  if (bkValue > vInfo.mdMajorVersion_) return -2;

  // minor version
  tok = strtok(NULL, ".");
  if (tok == NULL) return -2;
  bkValue = atol(tok);
  if (bkValue > vInfo.mdMinorVersion_) return -2;

  // update version
  tok = strtok(NULL, ".");
  if (tok == NULL) return -2;
  bkValue = atol(tok);
  if (bkValue > vInfo.mdUpdateVersion_) return -2;

    // Verify product version (PR)
    //   don't allow a backup taken on a core banking system to be restored on a
    //   data lake system (or vice versa)
    // Actual version number can be different
#if 0
  // Core banking name is changing, ignore check
  attrVersion = getAttrByKey(BackupAttr::BACKUP_ATTR_VERSION_PR);
  if (attrVersion == NULL)
     return -3;
  mdStr = attrVersion;

  // Remove version number from backup value
  offset = mdStr.find_first_of(matches);
  mdStr = (mdStr.substr(0, offset)).c_str();
  versionStr = mdStr.c_str();

  // Remove version number from current value
  mdStr = vInfo.prVersionStr_.data();
  offset = mdStr.find_first_of(matches);
  mdStr = (mdStr.substr(0, offset)).c_str();
  NAString prodStr (mdStr.c_str());

  if (!prodStr.contains(versionStr))
    return -3;
#endif

  return 0;
}

void BackupAttrList::replaceAttr(const BackupAttr::BACKUP_ATTR_KEY attrKey, const char *value) {
  for (CollIndex i = 0; i < entries(); i++) {
    BackupAttr *oldAttr = operator[](i);
    if (oldAttr->getAttrKey() == attrKey) {
      BackupAttr *newAttr = new (getHeap()) BackupAttr(attrKey, value, getHeap());
      remove(oldAttr);
      insert(newAttr);
      return;
    }
  }
}
