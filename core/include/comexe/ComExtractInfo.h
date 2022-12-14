

/*
 * File:         ComExtractInfo.h
 * Description:  NAVersionedObject subclasses to store information in plans
 *               specific to parallel extract producer and consumer queries.
 * Created:      May 2007
 * Language:     C++
 *
 */

#ifndef COMEXTRACTINFO_H
#define COMEXTRACTINFO_H

#include "export/NAVersionedObject.h"

// -----------------------------------------------------------------------
// A class for information related to parallel extract producer
// queries. The top-level split top and split bottom TDBs for a
// producer query will each have a pointer to an instance of this
// class.
// -----------------------------------------------------------------------
class ComExtractProducerInfo : public NAVersionedObject {
 public:
  // Redefine virtual functions required for versioning
  ComExtractProducerInfo() : NAVersionedObject(-1) {}
  virtual unsigned char getClassVersionID() { return 1; }
  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }
  virtual short getClassSize() { return (short)sizeof(ComExtractProducerInfo); }
  Long pack(void *space);
  int unpack(void *base, void *reallocator);

  // Accessor functions
  const char *getSecurityKey() const { return securityKey_; }

  // Mutator functions
  void setSecurityKey(char *s) { securityKey_ = s; }

 protected:
  NABasicPtr securityKey_;  // 00-07
  char filler_[56];         // 08-63
};



// -----------------------------------------------------------------------
// A class for information related to parallel extract consumer
// queries. The send top TDB for a consumer query will have a pointer
// to an instance of this class.
// -----------------------------------------------------------------------
class ComExtractConsumerInfo : public NAVersionedObject {
 public:
  // Redefine virtual functions required for versioning
  ComExtractConsumerInfo() : NAVersionedObject(-1) {}
  virtual unsigned char getClassVersionID() { return 1; }
  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }
  virtual short getClassSize() { return (short)sizeof(ComExtractConsumerInfo); }
  Long pack(void *space);
  int unpack(void *base, void *reallocator);

  // Accessor functions
  const char *getEspPhandle() const { return espPhandle_; }
  const char *getSecurityKey() const { return securityKey_; }

  // Mutator functions
  void setEspPhandle(char *s) { espPhandle_ = s; }
  void setSecurityKey(char *s) { securityKey_ = s; }

 protected:
  NABasicPtr espPhandle_;   // 00-07
  NABasicPtr securityKey_;  // 08-15
  char filler_[48];         // 16-63
};

// -----------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ComExtractConsumerInfo
// -----------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<ComExtractConsumerInfo> ComExtractConsumerInfoPtr;

#endif  // COMEXTRACTINFO_H
