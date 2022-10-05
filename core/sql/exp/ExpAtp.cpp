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
/* -*-C++-*-
****************************************************************************
*
* File:         ExpAtp.cpp (previously /executor/ex_atp.cpp)
* Description:
*
* Created:      5/6/98
* Language:     C++
*
*
*
****************************************************************************
*/

#include "common/Platform.h"

#include "exp/ExpAtp.h"
#include "comexe/ComPackDefs.h"
#include "exp/exp_attrs.h"
#include "common/str.h"
#include "qmscommon/QRLogger.h"

// constructor (Allocate and initialize) for an Atp
atp_struct *allocateAtp(ex_cri_desc *criDesc, CollHeap *space) {
  // Allocate space for the atp_struct (which has room for one tupp in
  // the tupp array) plus room for the remainder of the tupps.
  //
  int atp_size = sizeof(atp_struct) + (criDesc->noTuples() - 1) * sizeof(tupp);

  atp_struct *atp;
  atp = (atp_struct *)space->allocateMemory(atp_size);

  // set the pointer to the descriptor
  atp->setCriDesc(criDesc);

  // initialize tag data member.
  atp->set_tag(0 /* FALSE*/);

  // no errors yet.
  atp->initDiagsArea(0);

  // Initialize each tupp.
  //
  int j;
  for (j = 0; j < criDesc->noTuples(); j++) {
    atp->getTupp(j).init();
  }

  return atp;
}

// Create an atp inside a pre-allocated buffer instead of allocating it from
// a space object. Used by ExpDP2Expr.
atp_struct *createAtpInBuffer(ex_cri_desc *criDesc, char *&buf) {
  atp_struct *atp = (atp_struct *)buf;
  // set the pointer to the descriptor
  atp->setCriDesc(criDesc);

  // initialize tag data member.
  atp->set_tag(0 /* FALSE*/);

  // no errors yet.
  atp->initDiagsArea(0);

  // Initialize each tupp.
  //
  int j;
  for (j = 0; j < criDesc->noTuples(); j++) {
    atp->getTupp(j).init();
  }

  buf += sizeof(atp_struct) + (criDesc->noTuples() - 1) * sizeof(tupp);
  return atp;
}

atp_struct *allocateAtp(int numTuples, CollHeap *space) {
  // Allocate space for the atp_struct (which has room for one tupp in
  // the tupp array) plus room for the remainder of the tupps.
  //
  int atp_size = sizeof(atp_struct) + (numTuples - 1) * sizeof(tupp);

  atp_struct *atp;
  atp = (atp_struct *)space->allocateMemory(atp_size);

  // set the pointer to the descriptor
  atp->setCriDesc(NULL);

  // initialize tag data member.
  atp->set_tag(0 /* FALSE*/);

  // no errors yet.
  atp->initDiagsArea(0);

  // Initialize each tupp.
  //
  int j;
  for (j = 0; j < numTuples; j++) {
    atp->getTupp(j).init();
  }

  return atp;
}

void deallocateAtp(atp_struct *atp, CollHeap *space) {
  if (space) space->deallocateMemory((char *)atp);
}

atp_struct *allocateAtpArray(ex_cri_desc *criDesc, int cnt, int *atpSize, CollHeap *space,
                             NABoolean failureIsFatal) {
  const int numTuples = criDesc->noTuples();

  // Alocate space for the atp_struct (which has room for one tupp in
  // the tupp array) plus room for the remainder of the tupps,
  // times the count requested.
  *atpSize = sizeof(atp_struct) + numTuples * sizeof(tupp);
  char *atpSpace = (char *)space->allocateMemory(*atpSize * cnt, failureIsFatal);

  if (atpSpace == NULL) return NULL;

  atp_struct *atpStart = (atp_struct *)atpSpace;
  atp_struct *atp;

  for (int i = 0; i < cnt; i++) {
    atp = (atp_struct *)atpSpace;
    atpSpace += *atpSize;

    // set the pointer to the descriptor
    atp->setCriDesc(criDesc);

    // initialize tag data member.
    atp->set_tag(0 /* FALSE*/);

    // no errors yet.
    atp->initDiagsArea(0);

    // Initialize each tupp.
    //
    for (int j = 0; j < numTuples; j++) {
      atp->getTupp(j).init();
    }
  }
  return atpStart;
}

void deallocateAtpArray(atp_struct **atp, CollHeap *space) {
  if (space) space->deallocateMemory((char *)*atp);
}

Long atp_struct::pack(void *space) {
  for (int i = 0; i < criDesc_->noTuples(); i++) {
    tuppArray_[i].pack(space);
  }

  criDesc_ = (ex_cri_desc *)(criDesc_->drivePack(space));
  return ((Space *)space)->convertToOffset(this);
}

int atp_struct::unpack(int base) {
  ex_cri_desc obj;

  if (criDesc_) {
    criDesc_ = (ex_cri_desc *)CONVERT_TO_PTR(criDesc_, base);
    if (criDesc_->driveUnpack((void *)((long)base), &obj, NULL) == NULL) return -1;
    for (int i = 0; i < criDesc_->noTuples(); i++) {
      tuppArray_[i].unpack(base);
    }
  }
  return 0;
}

void atp_struct::display(const char *title, ex_cri_desc *cri) {
  cout << title << ", ";
  if (!cri) cri = getCriDesc();

  char subtitle[100];
  unsigned short tuples = numTuples();

  cout << "#tuples in ATP=" << tuples << endl;

  for (int i = 0; i < tuples; i++) {
    ExpTupleDesc *tDesc = cri->getTupleDescriptor(i);

    cout << "tupleDesc[" << i << "]=" << tDesc << endl;

    if (tDesc && tDesc->attrs()) {
      tupp &tup = getTupp(i);

      // get the pointer to the composite row
      char *dataPtr = tup.getDataPointer();

      // cout << "dataPtr=" << dataPtr << endl;
      cout << "attrs:" << endl;
      UInt32 attrs = tDesc->numAttrs();
      for (int j = 0; j < attrs; j++) {
        Attributes *attr = tDesc->getAttr(j);
        Int16 dt = attr->getDatatype();
        UInt32 len = attr->getLength();

        NABoolean isVarChar = attr->getVCIndicatorLength() > 0;

        char *realDataPtr = dataPtr + attr->getOffset();
        if (isVarChar) {
          realDataPtr += attr->getVCIndicatorLength();
        }

        sprintf(subtitle, "    %dth field: ", j);
        print(subtitle, dt, realDataPtr, len);

        // dataPtr = realDataPtr + len;
      }
    }
  }
  cout << endl;
}

void atp_struct::print(char *title, Int16 dt, char *ptr, UInt32 len) {
  dumpATupp(cout, title, dt, ptr, len);
  cout << endl;
}

void atp_struct::dumpATupp(ostream &out, const char *title, Int16 dt, char *ptr, UInt32 len) {
  out << title << " len=" << len << ", data=";
  switch (dt) {
    case REC_DECIMAL_LSE:
      out << ptr;
      break;

    case REC_BYTE_V_ASCII:
    case REC_BYTE_F_ASCII: {
      /*
                 for (int i=0; i<len; i++) {
                   out << (int)ptr[i];

                   if ( i<len-1 )
                    out << " ";
                 }

                 out << ", ";
      */

      out << "\"";
      for (int i = 0; i < len; i++) out << ptr[i];

      out << "\"";

    } break;
    case REC_BIN16_UNSIGNED: {
      out << *(unsigned short *)ptr;
    } break;
    case REC_BIN16_SIGNED: {
      out << *(short *)ptr;
    } break;
    case REC_BIN32_UNSIGNED: {
      out << *(unsigned int *)ptr;
    } break;
    case REC_BIN32_SIGNED: {
      out << *(int *)ptr;
    } break;
    case REC_BIN64_SIGNED: {
      out << *(long *)ptr;
    } break;

    default:
      out << "unimplemented, skip for now";
      break;
  }
}

void atp_struct::dump(ostream &out, const char *title, int idx, ex_cri_desc *cri) {
  if (title) out << title << ", ";

  unsigned short tuples = numTuples();

  int start = 0;
  int end = tuples - 1;

  if (idx != -1) {
    if (idx < 0 || idx > tuples - 1) {
      out << "index " << idx;
      out << " out of range [" << 0 << "," << tuples - 1 << "]" << endl;
      return;
    }
    start = idx;
    end = idx;
  }

  if (!cri) cri = getCriDesc();

  for (int i = start; i <= end; i++) {
    ExpTupleDesc *tDesc = cri->getTupleDescriptor(i);
    cout << "tDesc[" << i << "]=" << tDesc << endl;

    if (!tDesc || tDesc->numAttrs() == 0) {
      continue;
    }

    Attributes *theAttr = tDesc->getAttr(0);

    if (theAttr->getTupleFormat() != ExpTupleDesc::SQLARK_EXPLODED_FORMAT &&
        theAttr->getTupleFormat() != ExpTupleDesc::SQLMX_ALIGNED_FORMAT) {
      out << "Format is not in EXPLODED or ALIGNED FORMAT." << endl;
    }

    Int16 dt = theAttr->getDatatype();
    UInt32 length = 0;

    tupp &tup = getTupp(i);

    tupp_descriptor *desc = tup.get_tupp_descriptor();

    if (desc) out << "Tupp[" << i << "]: allocatedSize=" << tup.getAllocatedSize() << endl;

    char *ptr = tup.getDataPointer();

    if (!ptr) {
      out << "Tupp[" << i << "]: NULL data pointer." << endl;
      continue;
    }

    switch (dt) {
      case REC_BIN16_SIGNED:
      case REC_BIN16_UNSIGNED:
      case REC_DECIMAL_LSE:
      case REC_BIN64_SIGNED:
      case REC_BIN32_UNSIGNED:
      case REC_BIN32_SIGNED: {
        out << "data type=" << dt;
        dumpATupp(out, ", data:", dt, ptr + theAttr->getOffset(), theAttr->getLength());
        break;
      }
      case REC_BYTE_V_ASCII: {
        out << "varying char(n),";

        length = theAttr->getLength(ptr + theAttr->getVCLenIndOffset());
        ptr += theAttr->getOffset();

        out << ", vcIndOffset=" << theAttr->getVCLenIndOffset();
        out << ", vcIndLen=" << theAttr->getVCIndicatorLength();

        dumpATupp(out, ", ", dt, ptr, length);

        break;
      }
      case REC_BYTE_F_ASCII: {
        out << "fixed char(n)";
        dumpATupp(out, ", data:", dt, ptr + theAttr->getOffset(), theAttr->getLength());

        break;
      }
      default:
        break;
    }
  }

  out << endl;
}

void atp_struct::dump(const char *title, int idx, ex_cri_desc *cri) { dump(cout, title, idx, cri); }

void atp_struct::dumpMinMaxTupp(ostream &out, const char *title, int idx, ex_cri_desc *cri) {
  if (title) out << title << ", " << endl;

  unsigned short tuples = numTuples();

  int start = 0;
  int end = tuples - 1;

  if (idx != -1) {
    if (idx < 0 || idx > tuples - 1) {
      out << "index " << idx;
      out << " out of range [" << 0 << "," << tuples - 1 << "]" << endl;
      return;
    }
    start = idx;
    end = idx;
  }

  if (!cri) cri = getCriDesc();

  for (int i = start; i <= end; i++) {
    ExpTupleDesc *tDesc = cri->getTupleDescriptor(i);
    char *ptr = (getTupp(i)).getDataPointer();

    out << "tDesc[" << i << "]=" << static_cast<void *>(tDesc);
    out << ", dataPointer[" << i << "]=" << static_cast<void *>(ptr);

    if ((getTupp(i)).isAllocated()) {
      out << ", allocatedSize[" << i << "]=" << (getTupp(i)).getAllocatedSize();
    } else
      out << ", tupp[" << i << "] not allocated";

    if (!tDesc || !ptr) {
      out << endl;
      continue;
    }

    out << ", # of fields in the tupp=" << tDesc->numAttrs() << endl;

    if (tDesc->numAttrs() < 2) {
      continue;
    }

    Attributes *minAttr = tDesc->getAttr(0);  // for min
    Attributes *maxAttr = tDesc->getAttr(1);  // for min

    if (minAttr->getTupleFormat() != ExpTupleDesc::SQLARK_EXPLODED_FORMAT &&
        maxAttr->getTupleFormat() != ExpTupleDesc::SQLARK_EXPLODED_FORMAT) {
      out << "Format is not in SQLARK_EXPLODED_FORMAT." << endl;
    }

    Int16 dt = minAttr->getDatatype();
    UInt32 length = 0;

    switch (dt) {
      case REC_BIN16_SIGNED:
      case REC_BIN16_UNSIGNED:
      case REC_DECIMAL_LSE:
      case REC_BIN64_SIGNED:
      case REC_BIN32_UNSIGNED:
      case REC_BIN32_SIGNED: {
        out << "data type=" << dt;
        dumpATupp(out, ", min:", dt, ptr + minAttr->getOffset(), minAttr->getLength());
        dumpATupp(out, ", max:", dt, ptr + maxAttr->getOffset(), maxAttr->getLength());
        break;
      }
      case REC_BYTE_V_ASCII: {
        out << "varying char(n),";

        // min
        length = minAttr->getLength(ptr + minAttr->getVCLenIndOffset());
        ptr += minAttr->getOffset();

        out << ", min: vcIndOffset=" << minAttr->getVCLenIndOffset();
        out << ", vcIndLen=" << minAttr->getVCIndicatorLength();

        dumpATupp(out, ", ", dt, ptr, length);

        // max
        // ptr = (getTupp(idx)).getDataPointer() + maxAttr->getVCLenIndOffset();
        ptr = (getTupp(i)).getDataPointer();
        length = maxAttr->getLength(ptr + maxAttr->getVCLenIndOffset());

        out << ", max: vcIndOffset=" << maxAttr->getVCLenIndOffset();
        out << ", vcIndLen=" << maxAttr->getVCIndicatorLength();

        ptr += maxAttr->getOffset();

        dumpATupp(out, ", ", dt, ptr, length);
        break;
      }
      case REC_BYTE_F_ASCII: {
        out << "fixed char(n)";
        dumpATupp(out, ", min:", dt, ptr + minAttr->getOffset(), minAttr->getLength());
        dumpATupp(out, ", max:", dt, ptr + maxAttr->getOffset(), maxAttr->getLength());
        // out << ", len=" << length << ", data=";
        // for (int i=0; i<length; i++)
        //  out << ptr[i];

        break;
      }
      default:
        break;
    }
  }

  out << endl;
}

void atp_struct::dumpMinMaxTupp(const char *title, int idx, ex_cri_desc *cri) {
  fstream &fout = getPrintHandle();
  dumpMinMaxTupp(fout, title, idx, cri);
  fout.close();
}

void atp_struct::dumpMinMaxTupp2Log(logLevel level, const char *title, int idx, ex_cri_desc *cri) {
  std::ostringstream out;
  dumpMinMaxTupp(out, title, idx, cri);
  QRLogger::log(CAT_SQL_EXE_MINMAX, level, "%s", out.str().c_str());
}

void *atp_struct::checkAndGetMetaData(int i, Int16 &dt) {
  unsigned short tuples = numTuples();
  if (i < 0 || i > tuples - 1) return NULL;

  ex_cri_desc *cri = getCriDesc();
  if (!cri) return NULL;

  ExpTupleDesc *tDesc = cri->getTupleDescriptor(i);
  if (!tDesc) return NULL;

  Attributes *theAttr = tDesc->getAttr(0);
  dt = theAttr->getDatatype();

  return (getTupp(i)).getDataPointer() + theAttr->getOffset();
}

NABoolean atp_struct::getRegularIntSigned(int i, int &result) {
  Int16 dt = 0;
  char *ptr = (char *)checkAndGetMetaData(i, dt);

  if (!ptr) return FALSE;

  switch (dt) {
    case REC_BIN8_SIGNED:
      result = *(Int8 *)ptr;
      return TRUE;

    case REC_BIN16_SIGNED:
      result = *(Int16 *)ptr;
      return TRUE;

    case REC_BIN32_SIGNED:
      result = *(int *)ptr;
      return TRUE;

    default:
      break;
  }

  return FALSE;
}

NABoolean atp_struct::getRegularIntUnsigned(int i, UInt32 &result) {
  Int16 dt = 0;
  char *ptr = (char *)checkAndGetMetaData(i, dt);

  if (!ptr) return FALSE;

  switch (dt) {
    case REC_BIN8_UNSIGNED:
      result = *(UInt8 *)ptr;
      return TRUE;

    case REC_BIN16_UNSIGNED:
      result = *(UInt16 *)ptr;
      return TRUE;

    case REC_BIN32_UNSIGNED:
      result = *(UInt32 *)ptr;
      return TRUE;

    default:
      break;
  }

  return FALSE;
}
