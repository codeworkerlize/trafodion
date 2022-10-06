

/*
 * File:         ComExtractInfo.cpp
 * Description:  NAVersionedObject subclasses to store information in plans
 *               specific to parallel extract producer and consumer queries.
 * Created:      May 2007
 * Language:     C++
 *
 */

#include "comexe/ComExtractInfo.h"

Long ComExtractProducerInfo::pack(void *space) {
  securityKey_.pack(space);
  return NAVersionedObject::pack(space);
}

int ComExtractProducerInfo::unpack(void *base, void *reallocator) {
  if (securityKey_.unpack(base)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}

Long ComExtractConsumerInfo::pack(void *space) {
  espPhandle_.pack(space);
  securityKey_.pack(space);
  return NAVersionedObject::pack(space);
}

int ComExtractConsumerInfo::unpack(void *base, void *reallocator) {
  if (espPhandle_.unpack(base)) return -1;
  if (securityKey_.unpack(base)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}
