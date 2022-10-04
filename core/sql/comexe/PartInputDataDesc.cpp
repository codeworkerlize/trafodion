
#include "comexe/PartInputDataDesc.h"
#include "common/str.h"
#include "exp/exp_expr.h"
#include "exp/ExpSqlTupp.h"
#include "exp/ExpAtp.h"
#include "comexe/ComPackDefs.h"

#include "common/BaseTypes.h"

// -----------------------------------------------------------------------
// Methods for class ExPartInputDataDesc
// -----------------------------------------------------------------------

ExPartInputDataDesc::ExPartInputDataDesc(ExPartitioningType partType, ex_cri_desc *partInputCriDesc,
                                         int partInputDataLength, int numPartitions)
    : NAVersionedObject(partType) {
  partType_ = partType;
  partInputCriDesc_ = partInputCriDesc;
  partInputDataLength_ = partInputDataLength;
  numPartitions_ = numPartitions;
}

// -----------------------------------------------------------------------
// Methods for class ExHashPartInputData
// -----------------------------------------------------------------------
ExHashPartInputData::ExHashPartInputData(ex_cri_desc *partInputCriDesc, int numPartitions)
    : ExPartInputDataDesc(HASH_PARTITIONED, partInputCriDesc, 2 * sizeof(int), numPartitions) {}

void ExHashPartInputData::copyPartInputValue(int fromPartNum, int toPartNum, char *buffer, int bufferLength) {
  // ex_assert(bufferLength == 2 * sizeof(long),
  //	    "Hash part intput values are always two 4 byte integers");
  str_cpy_all(buffer, (char *)&fromPartNum, sizeof(int));
  str_cpy_all(&buffer[sizeof(int)], (char *)&toPartNum, sizeof(int));
}

// -----------------------------------------------------------------------
// Methods for class ExRoundRobinPartInputData
// -----------------------------------------------------------------------
ExRoundRobinPartInputData::ExRoundRobinPartInputData(ex_cri_desc *partInputCriDesc, int numPartitions,
                                                     int numOrigRRPartitions)
    : ExPartInputDataDesc(ROUNDROBIN_PARTITIONED, partInputCriDesc, 2 * sizeof(int), numPartitions),
      numOrigRRPartitions_(numOrigRRPartitions) {}

void ExRoundRobinPartInputData::copyPartInputValue(int fromPartNum, int toPartNum, char *buffer,
                                                   int bufferLength) {
  int scaleFactor = numOrigRRPartitions_ / getNumPartitions();
  int transPoint = numOrigRRPartitions_ % getNumPartitions();

  int loPart;
  int hiPart;

  if (fromPartNum < transPoint) {
    loPart = fromPartNum * (scaleFactor + 1);
  } else {
    loPart = (fromPartNum * scaleFactor) + transPoint;
  }

  if (toPartNum < transPoint) {
    hiPart = (toPartNum * (scaleFactor + 1)) + scaleFactor;
  } else {
    hiPart = (toPartNum * scaleFactor) + scaleFactor + transPoint - 1;
  }

  str_cpy_all(buffer, (char *)&loPart, sizeof(int));
  str_cpy_all(&buffer[sizeof(int)], (char *)&hiPart, sizeof(int));
}

// -----------------------------------------------------------------------
// Methods for class ExRangePartInputData
// -----------------------------------------------------------------------

ExRangePartInputData::ExRangePartInputData(ex_cri_desc *partInputCriDesc, int partInputDataLength,
                                           int partKeyLength, int exclusionIndicatorOffset, int numPartitions,
                                           Space *space, int useExpressions)
    : ExPartInputDataDesc(RANGE_PARTITIONED, partInputCriDesc, partInputDataLength, numPartitions) {
  exclusionIndicatorOffset_ = exclusionIndicatorOffset;
  exclusionIndicatorLength_ = sizeof(int);  // fixed for now
  partKeyLength_ = partKeyLength;
  alignedPartKeyLength_ = (partKeyLength + 7) / 8 * 8;
  useExpressions_ = useExpressions;
  partRangeExprAtp_ = -1;       // may be set later
  partRangeExprAtpIndex_ = -1;  // may be set later
  partRangeExprHasBeenEvaluated_ = NOT useExpressions;

  if (numPartitions > 0) {
    // allocate space for (numPartitions+1) keys, since n partitions have
    // n+1 boundaries, if one counts the ends
    int totalPartRangesLength = alignedPartKeyLength_ * (numPartitions + 1);
    partRanges_ = space->allocateAlignedSpace((size_t)totalPartRangesLength);

    if (useExpressions_) {
      // Expressions are used at run time to compute the boundaries,
      // the creator of this object will later set those expressions
      // and (hopefully) set atp and atpindex values. Allocate an
      // array of expression pointers, one pointer for each boundary.
      partRangeExpressions_ = new (space) ExExprPtr[numPartitions + 1];

      // just to be safe, initialize the array with NULLs
      for (int i = 0; i < (numPartitions + 1); i++) partRangeExpressions_[i] = (ExExprPtrPtr)NULL;
    } else {
      partRangeExpressions_ = (ExExprPtrPtr)NULL;
    }
  } else {
    // otherwise, this is a fake partitioning data descriptor
    partRanges_ = (NABasicPtr)NULL;
    partRangeExpressions_ = (ExExprPtrPtr)NULL;
  }
}

ExRangePartInputData::~ExRangePartInputData() {}

void ExRangePartInputData::setPartitionStartExpr(int partNo, ex_expr *expr) {
  // ex_assert(partNo >= 0 AND partNo <= getNumPartitions() AND useExpressions_,
  //	    "Partition expr. number out of range or desc doesn't use exprs");
  partRangeExpressions_[partNo] = expr;
}

void ExRangePartInputData::setPartitionStartValue(int partNo, char *val) {
  // ex_assert(partNo >= 0 AND partNo <= getNumPartitions(),
  //	    "Partition number out of range");
  str_cpy_all(&((char *)partRanges_)[partNo * alignedPartKeyLength_], val, partKeyLength_);
}

void ExRangePartInputData::copyPartInputValue(int fromPartNum, int toPartNum, char *buffer, int bufferLength) {
  // ex_assert(fromPartNum >= 0 AND
  //	    fromPartNum <= getNumPartitions() AND
  //	    toPartNum >= 0 AND
  //	    toPartNum <= getNumPartitions() AND
  //	    fromPartNum <= toPartNum AND
  //	    bufferLength >= getPartInputDataLength(),
  //	    "Partition number or buffer length out of range");
  // ex_assert(2*partKeyLength_ <= getPartInputDataLength(),
  //	    "Part. input data length must > 2 * key length");
  // copy begin key (entry fromPartNum)
  str_cpy_all(buffer, &((char *)partRanges_)[fromPartNum * alignedPartKeyLength_], partKeyLength_);
  // copy end key (entry toPartNum + 1)
  str_cpy_all(&buffer[partKeyLength_], &((char *)partRanges_)[(toPartNum + 1) * alignedPartKeyLength_], partKeyLength_);
  // indicate whether the end key is inclusive or exclusive
  int exclusive = (toPartNum < getNumPartitions() - 1);
  // ex_assert(exclusionIndicatorLength_ == sizeof(exclusive),
  //	    "Exclusion indicator length must be 4");
  str_cpy_all(&buffer[exclusionIndicatorOffset_], (char *)&exclusive, sizeof(exclusive));
}

int ExRangePartInputData::evalExpressions(Space *space, CollHeap *exHeap, ComDiagsArea **diags) {
  int result = 0;  // 0 == success

  // return if there is no work to do
  if (getNumPartitions() == 0 OR partRangeExprHasBeenEvaluated_) return result;

  // Actually need to evaluate all the expressions and store their
  // results in the partRanges_ byte array.

  // prepare a work atp
  atp_struct *workAtp = allocateAtp(getPartInputCriDesc(), space);
  tupp_descriptor td;

  workAtp->getTupp(partRangeExprAtpIndex_) = &td;

  if (workAtp->getDiagsArea() != *diags) workAtp->setDiagsArea(*diags);

  // loop over all expressions, fixing them up and evaluating them
  for (int i = 0; i <= getNumPartitions() AND result == 0; i++) {
    int offs = i * alignedPartKeyLength_;

    workAtp->getTupp(partRangeExprAtpIndex_).setDataPointer(&((char *)partRanges_)[offs]);

    partRangeExpressions_[i]->fixup(0, ex_expr::PCODE_NONE, 0, space, exHeap, FALSE, NULL);
    if (partRangeExpressions_[i]->eval(workAtp, NULL) == ex_expr::EXPR_ERROR) result = -1;
  }

  partRangeExprHasBeenEvaluated_ = (result == 0);
  return result;
}

// -----------------------------------------------------------------------
// Methods for class ExHashDistPartInputData
// -----------------------------------------------------------------------
ExHashDistPartInputData::ExHashDistPartInputData(ex_cri_desc *partInputCriDesc, int numPartitions,
                                                 int numOrigHashPartitions)
    : ExPartInputDataDesc(HASH1_PARTITIONED, partInputCriDesc, 2 * sizeof(int), numPartitions),
      numOrigHashPartitions_(numOrigHashPartitions) {}

void ExHashDistPartInputData::copyPartInputValue(int fromPartNum, int toPartNum, char *buffer, int bufferLength) {
  // ex_assert(bufferLength == 2 * sizeof(long),
  //	    "Hash part intput values are always two 4 byte integers");

  int scaleFactor = numOrigHashPartitions_ / getNumPartitions();
  int transPoint = numOrigHashPartitions_ % getNumPartitions();

  int loPart;
  int hiPart;

  if (fromPartNum < transPoint) {
    loPart = fromPartNum * (scaleFactor + 1);
  } else {
    loPart = (fromPartNum * scaleFactor) + transPoint;
  }

  if (toPartNum < transPoint) {
    hiPart = (toPartNum * (scaleFactor + 1)) + scaleFactor;
  } else {
    hiPart = (toPartNum * scaleFactor) + scaleFactor + transPoint - 1;
  }

  // ex_assert(loPart >= 0 &&
  //           loPart <= hiPart &&
  //           hiPart < numOrigHashPartitions_,
  //           "Hash Dist: invalid input values");

  str_cpy_all(buffer, (char *)&loPart, sizeof(int));
  str_cpy_all(&buffer[sizeof(int)], (char *)&hiPart, sizeof(int));
}

// -----------------------------------------------------------------------
// Methods for class ExHash2PartInputData
// -----------------------------------------------------------------------
ExHash2PartInputData::ExHash2PartInputData(ex_cri_desc *partInputCriDesc, int numPartitions)
    : ExPartInputDataDesc(HASH2_PARTITIONED, partInputCriDesc, 2 * sizeof(int), numPartitions) {}

void ExHash2PartInputData::copyPartInputValue(int fromPartNum, int toPartNum, char *buffer, int bufferLength) {
  long numPartitions = (long)getNumPartitions();

  // For hash2, partition numbers are not passed within the partition input
  // values.  Instead, the hash boundaries are passed.  This allows a single
  // ESP to handle tables with different numbers of partitions.
  //
  // Because the integer math of the hash2 split function causes a rounding
  // down, the integer math when determining a hash boundary must round up.
  // This explains why " + numPartitions - 1" is seen in the numerator of
  // the division below.
  ULng32 loHash = (ULng32)((((long)fromPartNum << 32) + numPartitions - 1) / numPartitions);

  // The hiHash value is one less than the hash boundary for the next
  // partition number.
  ULng32 hiHash = (ULng32)(((((long)(toPartNum + 1) << 32) + numPartitions - 1) / numPartitions) - 1);

  str_cpy_all(buffer, (char *)&loHash, sizeof(int));
  str_cpy_all(&buffer[sizeof(int)], (char *)&hiHash, sizeof(int));
}

Long ExPartInputDataDesc::pack(void *space) {
  partInputCriDesc_.pack(space);
  return NAVersionedObject::pack(space);
}

Long ExHashPartInputData::pack(void *space) { return ExPartInputDataDesc::pack(space); }

Long ExRoundRobinPartInputData::pack(void *space) { return ExPartInputDataDesc::pack(space); }

Long ExRangePartInputData::pack(void *space) {
  if (useExpressions_) {
    partRangeExpressions_.pack(space, getNumPartitions() + 1);
  }

  partRanges_.pack(space);
  return ExPartInputDataDesc::pack(space);
}

Long ExHashDistPartInputData::pack(void *space) { return ExPartInputDataDesc::pack(space); }

Long ExHash2PartInputData::pack(void *space) { return ExPartInputDataDesc::pack(space); }

int ExPartInputDataDesc::unpack(void *base, void *reallocator) {
  if (partInputCriDesc_.unpack(base, reallocator)) return -1;
  return NAVersionedObject::unpack(base, reallocator);
}

int ExHashPartInputData::unpack(void *base, void *reallocator) {
  return ExPartInputDataDesc::unpack(base, reallocator);
}

int ExRoundRobinPartInputData::unpack(void *base, void *reallocator) {
  return ExPartInputDataDesc::unpack(base, reallocator);
}

int ExRangePartInputData::unpack(void *base, void *reallocator) {
  if (useExpressions_) {
    if (partRangeExpressions_.unpack(base, getNumPartitions() + 1, reallocator)) return -1;
  }

  if (partRanges_.unpack(base)) return -1;
  return ExPartInputDataDesc::unpack(base, reallocator);
}

int ExHashDistPartInputData::unpack(void *base, void *reallocator) {
  return ExPartInputDataDesc::unpack(base, reallocator);
}

int ExHash2PartInputData::unpack(void *base, void *reallocator) {
  return ExPartInputDataDesc::unpack(base, reallocator);
}

// -----------------------------------------------------------------------
// This method returns the virtual function table pointer for an object
// with the given class ID; used by NAVersionedObject::driveUnpack().
// -----------------------------------------------------------------------
char *ExPartInputDataDesc::findVTblPtr(short classID) {
  char *vtblPtr;
  switch (classID) {
    case HASH_PARTITIONED:
      GetVTblPtr(vtblPtr, ExHashPartInputData);
      break;
    case RANGE_PARTITIONED:
      GetVTblPtr(vtblPtr, ExRangePartInputData);
      break;
    case ROUNDROBIN_PARTITIONED:
      GetVTblPtr(vtblPtr, ExRoundRobinPartInputData);
      break;
    case HASH1_PARTITIONED:
      GetVTblPtr(vtblPtr, ExHashDistPartInputData);
      break;
    case HASH2_PARTITIONED:
      GetVTblPtr(vtblPtr, ExHash2PartInputData);
      break;
    default:
      GetVTblPtr(vtblPtr, ExPartInputDataDesc);
      break;
  }
  return vtblPtr;
}

void ExPartInputDataDesc::fixupVTblPtr() {
  char *to_vtbl_ptr = (char *)this;
  char *from_vtbl_ptr;

  switch (partType_) {
    case HASH_PARTITIONED: {
      ExHashPartInputData partInputDataDesc(NULL, 1);
      from_vtbl_ptr = (char *)&partInputDataDesc;
      str_cpy_all(to_vtbl_ptr, from_vtbl_ptr, sizeof(char *));
    } break;
    case RANGE_PARTITIONED: {
      ExRangePartInputData partInputDataDesc(NULL, 0, 0, 0, 0, NULL, 0);
      from_vtbl_ptr = (char *)&partInputDataDesc;
      str_cpy_all(to_vtbl_ptr, from_vtbl_ptr, sizeof(char *));
    } break;
    case ROUNDROBIN_PARTITIONED: {
      ExRoundRobinPartInputData partInputDataDesc(NULL, 1, 1);
      from_vtbl_ptr = (char *)&partInputDataDesc;
      str_cpy_all(to_vtbl_ptr, from_vtbl_ptr, sizeof(char *));
    } break;
    case HASH1_PARTITIONED: {
      ExHashDistPartInputData partInputDataDesc(NULL, 1, 1);
      from_vtbl_ptr = (char *)&partInputDataDesc;
      str_cpy_all(to_vtbl_ptr, from_vtbl_ptr, sizeof(char *));
    } break;
    case HASH2_PARTITIONED: {
      ExHash2PartInputData partInputDataDesc(NULL, 1);
      from_vtbl_ptr = (char *)&partInputDataDesc;
      str_cpy_all(to_vtbl_ptr, from_vtbl_ptr, sizeof(char *));
    } break;
    default: {
      // ex_assert(0,"Invalid partitioning type");
    }
  }
}
