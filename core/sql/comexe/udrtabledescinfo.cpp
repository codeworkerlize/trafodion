
/* -*-C++-*-
****************************************************************************
*
* File:         UdrTableDescInfo.h
* Description:  Metadata for child table descriptor info of a TMUDF tcb
* Created:      2/10/2010
* Language:     C++
*
****************************************************************************
*/

#include "comexe/udrtabledescinfo.h"

//
// Constructor
//

UdrTableDescInfo::UdrTableDescInfo(const char *corrName, Int16 numColumns, UInt32 outputRowLength,
                                   UInt16 outputTuppIndex, UdrColumnDescInfoPtrPtr colDescList)
    : NAVersionedObject(-1) {
  corrName_ = corrName;
  numColumns_ = numColumns;
  outputRowLen_ = outputRowLength;
  outputTuppIndex_ = outputTuppIndex;
  colDescList_ = colDescList;
}

Long UdrTableDescInfo::pack(void *space) {
  corrName_.pack(space);
  if (numColumns_ > 0) colDescList_.pack(space, numColumns_);
  return NAVersionedObject::pack(space);
}

int UdrTableDescInfo::unpack(void *base, void *reallocator) {
  if (corrName_.unpack(base)) return -1;
  if (colDescList_.unpack(base, numColumns_, reallocator)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}
