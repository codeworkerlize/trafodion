// @@@ START COPYRIGHT @@@
//
// (c) Copyright 2017 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@

#include "comexe/ComASNodes.h"

char *ComASNodes::findVTblPtr(short classID) {
  char *vtblPtr;
  GetVTblPtr(vtblPtr, ComASNodes);

  return vtblPtr;
}

unsigned char ComASNodes::getClassVersionID() { return 1; }

void ComASNodes::populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

short ComASNodes::getClassSize() { return (short)sizeof(ComASNodes); }

Long ComASNodes::pack(void *space) {
  serializedNodes_.pack(space);
  return NAVersionedObject::pack(space);
}

int ComASNodes::unpack(void *base, void *reallocator) {
  if (serializedNodes_.unpack(base)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}
