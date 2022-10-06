

#include "ExpAlignedFormat.h"

#include "common/Platform.h"
#include "common/str.h"

int ExpAlignedFormat::setupHeaderAndVOAs(UInt32 numFields, UInt32 numNullableFields, UInt32 numVarcharFields) {
  UInt32 hdrSize = getHdrSize();
  firstFixed_ = hdrSize;
  bitmapOffset_ = hdrSize;

  if (numVarcharFields > 0) {
    firstFixed_ += numVarcharFields * sizeof(UInt32);
    bitmapOffset_ += numVarcharFields * sizeof(UInt32);
  }

  if (numNullableFields > 0) {
    UInt32 bitmapSize = getNeededBitmapSize(numNullableFields);
    firstFixed_ += bitmapSize;
  }

  return 0;
}

int ExpAlignedFormat::copyData(UInt32 fieldNum, char *dataPtr, UInt32 dataLen, NABoolean isVarchar) {
  if (isVarchar) return -1;

  clearNullValue(fieldNum);

  int currEndOffset = getFirstFixedOffset() + (fieldNum - 1) * dataLen;
  char *eafDataPtr = (char *)this + currEndOffset;
  str_cpy_all(eafDataPtr, dataPtr, dataLen);

  currEndOffset += dataLen;
  return currEndOffset;
}
