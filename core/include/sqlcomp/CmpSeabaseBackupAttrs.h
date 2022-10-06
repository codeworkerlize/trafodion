

#ifndef _CMP_SEABASE_BACKUP_H_
#define _CMP_SEABASE_BACKUP_H_

/*
 ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
 +
 + File: CmpSeabaseBackupAttrs.h
 + Description:
 +   Implements methods to store and retrieve a set of attributes from
 +   Backup metadata.
 +
 ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
*/

class BackupAttr;
class BackupAttrList;
struct VersionInfo;

#include "sqlcomp/CmpSeabaseDDL.h"

#define BKFormat "BKFormat-1.0"

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Class: BackupAttr
//
// Defines an attribute stored as part of Backup metadata
// An attribute consists of a key and value separated by a delimiter (+):
//     KEY+key+VALUE+value
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
class BackupAttr {
 public:
  // identifies backup attributes
  enum BACKUP_ATTR_KEY {
    BACKUP_ATTR_UNKNOWN = 0,
    BACKUP_ATTR_ENTRIES,
    BACKUP_ATTR_OWNER,
    BACKUP_ATTR_VERSION_DB,
    BACKUP_ATTR_VERSION_MD,
    BACKUP_ATTR_VERSION_PR,
    BACKUP_ATTR_USER_ROLES
  };

#define BACKUP_ATTR_UNKNOWN_LIT    "  "
#define BACKUP_ATTR_ENTRIES_LIT    "E "
#define BACKUP_ATTR_OWNER_LIT      "O "
#define BACKUP_ATTR_VERSION_DB_LIT "DV"
#define BACKUP_ATTR_VERSION_MD_LIT "MV"
#define BACKUP_ATTR_VERSION_PR_LIT "PV"
#define BACKUP_ATTR_USER_ROLES_LIT "UR"

  // constructors
  BackupAttr() : attrKey_(BACKUP_ATTR_UNKNOWN), attrValue_(NULL), heap_(NULL) {}

  BackupAttr(const BACKUP_ATTR_KEY attrKey, const char *attrValue, NAHeap *heap) : attrKey_(attrKey), heap_(heap) {
    int length = strlen(attrValue) + 1;
    attrValue_ = new (heap) char[length];
    strcpy(attrValue_, attrValue);
  }

  // destructor
  ~BackupAttr() {
    if (attrValue_ != NULL) {
      NADELETEBASIC(attrValue_, heap_);
      attrValue_ = NULL;
    }
  }

  BACKUP_ATTR_KEY getAttrKey() { return attrKey_; }
  char *getAttrValue() { return attrValue_; }
  NAHeap *getHeap() { return heap_; }

  void setAttrKey(BACKUP_ATTR_KEY attrKey) { attrKey_ = attrKey; }
  void setAttrValue(char *attrValue) { attrValue_ = attrValue; }

  NAString convertEnumToLiteral(BACKUP_ATTR_KEY enumVal);
  BACKUP_ATTR_KEY convertLiteralToEnum(const char *litVal);

  bool attrInvalid() { return (attrKey_ == BACKUP_ATTR_UNKNOWN || attrValue_ == NULL); }

 private:
  BACKUP_ATTR_KEY attrKey_;
  char *attrValue_;
  NAHeap *heap_;
};

// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
// Class: BackupAttrList
//
// Defines a list of BackupAttr (attributes)
// ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
class BackupAttrList : public LIST(BackupAttr *) {
 public:
  // constructor
  BackupAttrList() : LIST(BackupAttr *)(NULL), BKFormat_(BKFormat), heap_(NULL) {}

  BackupAttrList(NAHeap *heap) : LIST(BackupAttr *)(heap), BKFormat_(BKFormat), heap_(heap) {}

  // virtual destructor
  virtual ~BackupAttrList() {
    for (CollIndex i = 0; i < entries(); i++) NADELETE(operator[](i), BackupAttr, heap_);
    clear();
  }

  void addOwner(const char *owner);
  // should add a api to add general attr.
  void addUserAndRoles(const char *userAndRoles);
  short addVersions();

  short backupCompatible(VersionInfo vInfo);
  bool formatsMatch() { return BKFormat == BKFormat_; }

  char *getAttrByKey(const BackupAttr::BACKUP_ATTR_KEY attrKey);
  NAString getFormat() { return BKFormat_; }
  NAHeap *getHeap() { return heap_; }

  void setBKFormat(const NAString &bkFormat) { BKFormat_ = bkFormat; }

  short pack(char *&buffer, NAString &errorMsg);
  void replaceAttr(const BackupAttr::BACKUP_ATTR_KEY attrKey, const char *value);
  short unpack(const char *buffer, NAString &errorMsg);

 private:
  void packIntoBuffer(NAString &buffer, BackupAttr *attr);
  char *unpackFromBuffer(const char *buffer, BackupAttr *&attr);

  NAString BKFormat_;
  NAHeap *heap_;
};
#endif
