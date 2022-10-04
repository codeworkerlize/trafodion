// @@@ START COPYRIGHT @@@
//
// (c) Copyright 2017 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
#ifndef COM_ASNODES_H
#define COM_ASNODES_H

#include "export/NAVersionedObject.h"

// a class that wraps an NAWNodeSet object for inclusion in query plans
class ComASNodes : public NAVersionedObject {
 public:
  ComASNodes() : serializedNodes_(NULL) {}

  ComASNodes(char *serializedNodes) : serializedNodes_(serializedNodes) {}

  const char *getSerializedNodes() const { return serializedNodes_.getPointer(); }

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual char *findVTblPtr(short classID);
  virtual unsigned char getClassVersionID();
  virtual void populateImageVersionIDArray();
  virtual short getClassSize();
  virtual Long pack(void *space);
  virtual Lng32 unpack(void *base, void *reallocator);

 private:
  NABasicPtr serializedNodes_;  // 00-07
  char filler_[12];             // 08-19
};

typedef NAVersionedObjectPtrTempl<ComASNodes> ComASNodesPtr;

#endif
