
#ifndef ELEMDDLFILEATTRMISCOPTIONS_H
#define ELEMDDLFILEATTRMISCOPTIONS_H
/* -*-C++-*-
*****************************************************************************
*
* File:         ElemDDLFileAttrMisc.h
* Description:  misc file attributes
*
*
* Created:
* Language:     C++
*
*

*
*
*****************************************************************************
 */

#include "ElemDDLFileAttr.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLFileAttrNamespace;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None.

// -----------------------------------------------------------------------
// Namespace File Attribute (parse node) elements in DDL statements
// -----------------------------------------------------------------------

class ElemDDLFileAttrNamespace : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrNamespace(NAString &namespace1)
      : ElemDDLFileAttr(ELM_FILE_ATTR_NAMESPACE_ELEM), namespace_(namespace1) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrNamespace() {}

  // cast
  virtual ElemDDLFileAttrNamespace *castToElemDDLFileAttrNamespace() { return this; }

  // accessor
  const NAString &getNamespace() const { return namespace_; }

 private:
  NAString namespace_;

};  // class ElemDDLFileAttrNamespace

// -----------------------------------------------------------------------
// Encrypt File Attribute (parse node) elements in DDL statements
// -----------------------------------------------------------------------

class ElemDDLFileAttrEncrypt : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrEncrypt(NAString *encryptionOptions, CollHeap *heap)
      : ElemDDLFileAttr(ELM_FILE_ATTR_ENCRYPT_ELEM), encryptionOptions_(NULL) {
    if (encryptionOptions) encryptionOptions_ = new (heap) NAString(*encryptionOptions);
  }

  // virtual destructor
  virtual ~ElemDDLFileAttrEncrypt() {}

  // cast
  virtual ElemDDLFileAttrEncrypt *castToElemDDLFileAttrEncrypt() { return this; }

  // accessor
  const NAString *getEncryptionOptions() const { return encryptionOptions_; }

 private:
  // encryptionOptions_ == NULL indicates explicit 'no encryption' option
  NAString *encryptionOptions_;

};  // class ElemDDLFileAttrEncrypt

class ElemDDLFileAttrIncrBackup : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrIncrBackup(NABoolean e) : ElemDDLFileAttr(ELM_FILE_ATTR_INCR_BACKUP_ELEM), enabled_(e) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrIncrBackup() {}

  // cast
  virtual ElemDDLFileAttrIncrBackup *castToElemDDLFileAttrIncrBackup() { return this; }

  // accessor
  const NABoolean incrBackupEnabled() const { return enabled_; }

 private:
  // TRUE: incr backup enabled. FALSE: not enabled
  NABoolean enabled_;

};  // class ElemDDLFileAttrIncrBackup

class LobStorageOption : public NABasicObject {
 public:
  LobStorageOption(const char *key, const long val) {
    key_ = key;
    val_ = val;
  }

  LobStorageOption(LobStorageOption &hbo) {
    key_ = hbo.key();
    val_ = hbo.val();
  }

  NAString &key() { return key_; }
  long &val() { return val_; }
  void setVal(long val) { val_ = val; }

 private:
  NAString key_;
  long val_;
};

class ElemDDLLobStorageOptions : public ElemDDLNode {
 public:
  ElemDDLLobStorageOptions(NAList<LobStorageOption *> *lobStorageOptions, CollHeap *heap);

  // virtual destructor
  virtual ~ElemDDLLobStorageOptions() {}

  // cast
  virtual ElemDDLLobStorageOptions *castToElemDDLLobStorageOptions() { return this; }

  short synthesize(NAString &invalidOption);

  NAList<LobStorageOption *> &getLobStorageOptions() { return lobStorageOptions_; }

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  const long inlineDataMaxBytes() { return inlineDataMaxBytes_; };
  const long inlineDataMaxChars() { return inlineDataMaxChars_; };
  const long hbaseDataMaxLen() { return hbaseDataMaxLen_; };
  const long chunksColMaxLen() { return chunksColMaxLen_; };
  const long numChunksPartitions() { return numChunksPartitions_; };

 private:
  NAList<LobStorageOption *> lobStorageOptions_;

  long inlineDataMaxBytes_;
  long inlineDataMaxChars_;
  long hbaseDataMaxLen_;
  long chunksColMaxLen_;
  long numChunksPartitions_;

  NABoolean isError_;
};  // class ElemDDLLobStorageOptions

class ElemDDLFileAttrReadOnly : public ElemDDLFileAttr {
 public:
  // default constructor
  ElemDDLFileAttrReadOnly(NABoolean e) : ElemDDLFileAttr(ELM_FILE_ATTR_READ_ONLY_ELEM), enabled_(e) {}

  // virtual destructor
  virtual ~ElemDDLFileAttrReadOnly() {}

  // cast
  virtual ElemDDLFileAttrReadOnly *castToElemDDLFileAttrReadOnly() { return this; }

  // accessor
  const NABoolean readOnlyEnabled() const { return enabled_; }

 private:
  // TRUE: read_only enabled. FALSE: not enabled
  NABoolean enabled_;

};  // class ElemDDLFileAttrReadOnly

#endif
