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

#include "common/Platform.h"
#include "common/str.h"
#include "ExpAlignedFormat.h"

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
