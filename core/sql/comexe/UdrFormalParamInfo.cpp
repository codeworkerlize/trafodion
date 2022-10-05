
/* -*-C++-*-
****************************************************************************
*
* File:         UdrFormalParamInfo.cpp
* Description:  Metadata for UDR parameters
* Created:      10/10/2000
* Language:     C++
*
****************************************************************************
*/

#include "comexe/UdrFormalParamInfo.h"

UdrFormalParamInfo::UdrFormalParamInfo(Int16 type, Int16 precision, Int16 scale, Int16 flags, Int16 encodingCharSet,
                                       Int16 collation, char *paramName)
    : NAVersionedObject(-1),
      flags_(flags),
      type_(type),
      precision_(precision),
      scale_(scale),
      encodingCharSet_(encodingCharSet),
      collation_(collation),
      paramName_(paramName) {}

Long UdrFormalParamInfo::pack(void *space) {
  paramName_.pack(space);
  return NAVersionedObject::pack(space);
}

int UdrFormalParamInfo::unpack(void *base, void *reallocator) {
  if (paramName_.unpack(base)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}
