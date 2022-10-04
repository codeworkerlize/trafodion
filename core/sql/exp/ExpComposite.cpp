/*********************************************************************
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
 *****************************************************************************
 *
 * File:         <file>
 * Description:
 *
 *
 * Created:
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Platform.h"
#include "ExpComposite.h"

#include "exp/exp_bignum.h"
#include "exp/exp_clause.h"
#include "exp/exp_clause_derived.h"
#include "exp/exp_interval.h"
#include "exp/exp_datetime.h"
#include "common/DatetimeType.h"
#include "sqlci/Formatter.h"
#include "common/Int64.h"
#include "common/IntervalType.h"

Int32 compExprNum = 0;

//////////////////////////////////////////////////////
// CompositeAttributes
//////////////////////////////////////////////////////
Long CompositeAttributes::pack(void *space) {
  if (elements_) {
    for (int i = 0; i < numElements_; i++) {
      elements_[i].pack(space);
    }
  }
  elements_.packShallow(space);

  return Attributes::pack(space);
}

int CompositeAttributes::unpack(void *base, void *reallocator) {
  if (elements_) {
    if (elements_.unpackShallow(base)) return -1;
    for (int i = 0; i < numElements_; i++) {
      if (elements_[i].unpack(base, reallocator)) return -1;
    }
  }

  return Attributes::unpack(base, reallocator);
}

////////////////////////////////////////////////////////////////////
// ExpCompositeBase
////////////////////////////////////////////////////////////////////
ExpCompositeBase::ExpCompositeBase(OperatorTypeEnum oper_type, short type, ULng32 numElements, short numAttrs,
                                   Attributes **attr, ULng32 compRowLen, ex_expr *compExpr, ex_cri_desc *compCriDesc,
                                   AttributesPtr compAttrs, Space *space)
    : ex_function_clause(oper_type, numAttrs, attr, space),
      compExpr_(compExpr),
      compCriDesc_(compCriDesc),
      compAttrs_(compAttrs),
      numElements_(numElements),
      type_(type),
      compRowLen_(compRowLen),
      compAtp_(NULL),
      compTuppDesc_(NULL),
      compRow_(NULL) {}

Long ExpCompositeBase::pack(void *space) {
  compExpr_.packShallow(space);
  compCriDesc_.pack(space);
  compAttrs_.pack(space);

  return packClause(space, sizeof(ExpCompositeBase));
}

int ExpCompositeBase::unpack(void *base, void *reallocator) {
  if (compExpr_) {
    if (compExpr_.unpackShallow(base)) return -1;

    ex_expr dummyExpr;
    compExpr_ = (ex_expr *)compExpr_->driveUnpack((void *)compExpr_, &dummyExpr, NULL);
  }

  if (compCriDesc_.unpack(base, reallocator)) return -1;
  if (compAttrs_.unpack(base, reallocator)) return -1;

  return unpackClause(base, reallocator);
}

ex_expr::exp_return_type ExpCompositeBase::fixup(Space *space, CollHeap *exHeap, char *constantsArea, char *tempsArea,
                                                 char *persistentArea, short fixupFlag, NABoolean spaceCompOnly) {
  if (getCompExpr()) {
    getCompExpr()->fixup(0, ex_expr::PCODE_NONE, NULL, space, exHeap, FALSE, NULL);

    compAtp_ = allocateAtp(compCriDesc_, space);
    compTuppDesc_ = (tupp_descriptor *)(new (space) tupp_descriptor);
    compTuppDesc_->init();

    compAtp_->getTupp(2) = compTuppDesc_;
    compRow_ = new (space) char[compRowLen_];

    compAtp_->getTupp(2).setDataPointer(compRow_);
  }

  return ex_clause::fixup(space, exHeap, constantsArea, tempsArea, persistentArea, fixupFlag, spaceCompOnly);
}

void ExpCompositeBase::displayContents(Space *space, const char *displayStr, Int32 clauseNum, char *constsArea,
                                       ULng32 flag) {
  char buf[200];

  compExprNum++;
  Int32 exprNum = compExprNum;
  str_sprintf(buf, "  %s #%d: %s:L%d, type: %s, numElements: %d", "Clause", clauseNum, displayStr, exprNum,
              (type_ == 1 ? "ARRAY" : "ROW"), numElements_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  if (compCriDesc_) {
    str_sprintf(buf, "  compCriDesc->numTuples: %d", compCriDesc_->noTuples());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  if (getCompExpr()) {
    flag |= 0x00000020;
    getCompExpr()->displayContents(space, 0, "compExpr", flag);
  }

  compExprNum--;

  ex_clause::displayContents(space, NULL, clauseNum, constsArea);
}

// return: 0, if all ok. -1, if error
static short getAttrDataPtr(Attributes *op, char *rowDataPtr, char *&opdata, int &varLen, NABoolean &isNullVal) {
  UInt32 offset = op->getOffset();
  ExpTupleDesc::TupleDataFormat tdf = op->getTupleFormat();

  if ((tdf != ExpTupleDesc::SQLMX_ALIGNED_FORMAT) && (tdf != ExpTupleDesc::SQLARK_EXPLODED_FORMAT))
    return -1;  // error, only aligned or exploded format supported

  opdata = NULL;
  isNullVal = FALSE;

  if (op->getNullFlag())  // is nullable
  {
    char *nulldata = rowDataPtr + op->getNullIndOffset();
    if (ExpTupleDesc::isNullValue(nulldata, op->getNullBitIndex(), tdf)) {
      isNullVal = TRUE;
      return 0;
    }
  }

  varLen = -1;
  if (op->getVCIndicatorLength() > 0)  // varchar
  {
    if (tdf == ExpTupleDesc::SQLMX_ALIGNED_FORMAT) {
      offset = ExpTupleDesc::getVoaOffset(rowDataPtr, op->getVoaOffset(), tdf);

      char *vardata = rowDataPtr + offset;
      varLen = op->getLength(vardata);
      offset += op->getVCIndicatorLength();
    } else  // exploded format
    {
      char *vardata = rowDataPtr + op->getVCLenIndOffset();
      varLen = op->getLength(vardata);
    }
  }

  opdata = rowDataPtr + offset;

  return 0;
}

///////////////////////////////////////////////////////////
// ExpCompositeArrayLength
///////////////////////////////////////////////////////////
ExpCompositeArrayLength::ExpCompositeArrayLength(OperatorTypeEnum oper_type, short type, Attributes **attr,
                                                 Space *space)
    : ExpCompositeBase(oper_type, type, 0, 2, attr, 0, NULL, NULL, NULL, space) {}

void ExpCompositeArrayLength::displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum,
                                              char *constsArea, ULng32 flag) {
  ExpCompositeBase::displayContents(space, "ExpCompositeArrayLength", clauseNum, constsArea, flag);
}

ex_expr::exp_return_type ExpCompositeArrayLength::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  char *srcData = op_data[1];
  char *tgtData = op_data[0];

  Int32 srcNumElems = *(Int32 *)srcData;

  *(Int32 *)tgtData = srcNumElems;

  return ex_expr::EXPR_OK;
}

///////////////////////////////////////////////////////////
// ExpCompositeArrayCast
///////////////////////////////////////////////////////////
ExpCompositeArrayCast::ExpCompositeArrayCast(OperatorTypeEnum oper_type, short type, Attributes **attr,
                                             AttributesPtr compAttrs, AttributesPtr compAttrsChild1, Space *space)
    : ExpCompositeBase(oper_type, type, 0, 2, attr, 0, NULL, NULL, compAttrs, space),
      compAttrsChild1_(compAttrsChild1) {}

Long ExpCompositeArrayCast::pack(void *space) {
  compAttrsChild1_.pack(space);

  return ExpCompositeBase::pack(space);
}

int ExpCompositeArrayCast::unpack(void *base, void *reallocator) {
  if (compAttrsChild1_.unpack(base, reallocator)) return -1;

  return ExpCompositeBase::unpack(base, reallocator);
}

void ExpCompositeArrayCast::displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum,
                                            char *constsArea) {
  char buf[200];

  str_sprintf(buf, "  Clause #%d: ExpCompositeArrayCast, type: %s", clauseNum, (type_ == 1 ? "ARRAY" : "ROW"));
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  CompositeAttributes *compAttrs = (CompositeAttributes *)getCompAttrs();
  CompositeAttributes *childCompAttrs = (CompositeAttributes *)getChildCompAttrs();
  str_sprintf(buf, "    compFormat: %d, childCompFormat: %d", compAttrs->getCompFormat(),
              childCompAttrs->getCompFormat());
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  ex_clause::displayContents(space, NULL, clauseNum, constsArea);
}

ex_expr::exp_return_type ExpCompositeArrayCast::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  char *srcData = op_data[1];
  char *tgtData = op_data[0];

  CompositeAttributes *srcOp = (CompositeAttributes *)getOperand(1);
  CompositeAttributes *tgtOp = (CompositeAttributes *)getOperand(0);

  Int32 srcNumElems = *(Int32 *)srcData;
  Int32 tgtMaxElems = tgtOp->getNumElements();

  Int32 copyElems = MINOF(srcNumElems, tgtMaxElems);

  // if composite formats are the same, copy source to target.
  if (srcOp->getCompFormat() == tgtOp->getCompFormat()) {
    // copy source to tgt
    int srcLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

    *(Int32 *)tgtData = copyElems;
    srcData += sizeof(Int32);
    tgtData += sizeof(Int32);
    str_cpy_all(tgtData, srcData, (srcLen - sizeof(Int32)));

    if (tgtOp->getVCIndicatorLength() > 0) tgtOp->setVarLength(srcLen, op_data[-MAX_OPERANDS]);

    return ex_expr::EXPR_OK;
  }

  Int32 numCopyElems = MINOF(srcNumElems, tgtMaxElems);
  *(Int32 *)tgtData = numCopyElems;

  srcData += sizeof(Int32);
  tgtData += sizeof(Int32);

  ExpAlignedFormat *eaf = NULL;
  Attributes *tgtElemAttr = NULL;
  Attributes *srcElemAttr = NULL;

  Attributes *tgtAttrs = (Attributes *)compAttrs_;
  CompositeAttributes *tgtCompAttrs = (CompositeAttributes *)tgtAttrs;
  Attributes *srcAttrs = (Attributes *)compAttrsChild1_;
  CompositeAttributes *srcCompAttrs = (CompositeAttributes *)srcAttrs;
  tgtElemAttr = (Attributes *)(tgtCompAttrs->getElements())[0];
  srcElemAttr = (Attributes *)(srcCompAttrs->getElements())[0];

  if ((srcElemAttr->getNullFlag() != tgtElemAttr->getNullFlag()) ||
      (srcElemAttr->getVCIndicatorLength() != tgtElemAttr->getVCIndicatorLength())) {
    ExRaiseSqlError(heap, diagsArea, -3242);
    *(*diagsArea) << DgString0("Nullable and Varchar attributes of source and target must be the same");

    return ex_expr::EXPR_ERROR;
  }

  NABoolean isVarchar = (srcElemAttr->getVCIndicatorLength() > 0);
  NABoolean isNullable = srcElemAttr->getNullFlag();

  char *currTgtData = tgtData;
  char *currTgtNullData = NULL;
  if (tgtOp->getCompFormat() == COM_SQLMX_ALIGNED_FORMAT) {
    eaf = (ExpAlignedFormat *)tgtData;
    eaf->setupHeaderAndVOAs(numCopyElems, numCopyElems, (isVarchar ? numCopyElems : 0));
  } else {
    currTgtNullData = tgtData;
    currTgtData = tgtData + (isNullable ? tgtElemAttr->getNullIndicatorLength() : 0);
  }

  char *attrData = NULL;
  NABoolean isNullVal = FALSE;
  int varLen = -1;
  int attrLen = 0;
  Int32 tgtLen = 0;
  for (Int32 srcElem = 1; srcElem <= numCopyElems; srcElem++) {
    srcElemAttr = (Attributes *)(srcCompAttrs->getElements())[srcElem - 1];
    getAttrDataPtr(srcElemAttr, srcData, attrData, varLen, isNullVal);

    if (isNullVal) {
      // move null value to result.
      if (eaf) {
        eaf->setNullValue(srcElem);
      } else {
        currTgtNullData[0] = '\377';
        currTgtNullData[1] = '\377';

        currTgtNullData += tgtElemAttr->getNullIndicatorLength();
        currTgtData += tgtElemAttr->getNullIndicatorLength();
      }

      continue;
    }  // isNullVal

    attrLen = (((isVarchar) && (varLen >= 0)) ? varLen : srcElemAttr->getLength(attrData));

    if (eaf) {
      tgtLen = eaf->copyData(srcElem, attrData, attrLen, isVarchar);
      if (tgtLen < 0) {
        ExRaiseSqlError(heap, diagsArea, -3242);
        *(*diagsArea) << DgString0("Error returned from ExpAlignedFormat::copyData().");

        return ex_expr::EXPR_ERROR;
      }
    } else {
      currTgtNullData[0] = 0;
      currTgtNullData[1] = 0;

      currTgtData += tgtElemAttr->getNullIndicatorLength();
      memcpy(currTgtData, attrData, attrLen);

      currTgtData += attrLen;
      currTgtNullData = currTgtData;

      tgtLen = (currTgtData - tgtData);
    }
  }  // for

  if (getOperand(0)->getVCIndicatorLength() > 0) getOperand(0)->setVarLength(tgtLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type displayAttr(Space *space, Attributes *attr, Int32 operandNum, char *constsArea) {
  char buf[200];

  if (NOT((attr->getDatatype() == REC_ARRAY) || (attr->getDatatype() == REC_ROW))) {
    attr->displayContents(space, operandNum, constsArea, NULL);
    return ex_expr::EXPR_OK;
  }

  CompositeAttributes *compAttrs = (CompositeAttributes *)attr;
  if (attr->getDatatype() == REC_ARRAY) {
    str_sprintf(buf, "   ARRAY (compFormat: %d)", compAttrs->getCompFormat());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  } else if (attr->getDatatype() == REC_ROW) {
    str_sprintf(buf, "   ROW (compFormat: %d)", compAttrs->getCompFormat());
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  compAttrs->displayContents(space, operandNum, constsArea, NULL);

  str_sprintf(buf, " ");
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  Int32 numElemsToDisplay = (attr->getDatatype() == REC_ARRAY ? 1 : compAttrs->getNumElements());
  for (Int32 i = 0; i < numElemsToDisplay; i++) {
    AttributesPtrPtr app = compAttrs->getElements();
    AttributesPtr ap = app[i];
    Attributes *attr = (Attributes *)ap;

    displayAttr(space, attr, 1, constsArea);
  }  // for

  return ex_expr::EXPR_OK;
}

///////////////////////////////////////////////////////////
// ExpCompositeHiveCast
///////////////////////////////////////////////////////////
ExpCompositeHiveCast::ExpCompositeHiveCast(OperatorTypeEnum oper_type, NABoolean fromHive, short type,
                                           Attributes **attr, AttributesPtr compAttrs, AttributesPtr compAttrsChild1,
                                           Space *space)
    : ExpCompositeBase(oper_type, type, 0, 2, attr, 0, NULL, NULL, compAttrs, space),
      compAttrsChild1_(compAttrsChild1),
      flags_(0) {
  setFromHive(fromHive);
}

Long ExpCompositeHiveCast::pack(void *space) {
  compAttrsChild1_.pack(space);

  return ExpCompositeBase::pack(space);
}

int ExpCompositeHiveCast::unpack(void *base, void *reallocator) {
  if (compAttrsChild1_.unpack(base, reallocator)) return -1;

  return ExpCompositeBase::unpack(base, reallocator);
}

void ExpCompositeHiveCast::displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum,
                                           char *constsArea) {
  char buf[200];

  str_sprintf(buf, "  Clause #%d: ExpCompositeHiveCast, type: %s", clauseNum, (type_ == 1 ? "ARRAY" : "ROW"));
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  CompositeAttributes *compAttrs = (CompositeAttributes *)getCompAttrs();
  if (compAttrs) {
    str_sprintf(buf, " ");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    str_sprintf(buf, "  compAttrs begin");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    displayAttr(space, compAttrs, 1, constsArea);

    str_sprintf(buf, "  compAttrs end");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    str_sprintf(buf, " ");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  ex_clause::displayContents(space, NULL, clauseNum, constsArea);
}

// composite source data format returned from hive.
//   len:int     {length of composite data}
//   num:int     {number of elements}
//
ex_expr::exp_return_type ExpCompositeHiveCast::copyHiveSrcToTgt(Attributes *tgtAttr, char *&srcData, char *tgtData,
                                                                CollHeap *heap, ComDiagsArea **diagsArea,
                                                                NABoolean isRoot) {
  ex_expr::exp_return_type rc;
  if (NOT tgtAttr->isComposite()) {
    char *tgtElemData = tgtData + tgtAttr->getOffset();
    char *tgtElemNullData = tgtData + tgtAttr->getNullIndOffset();
    char *tgtElemVarData = tgtData + tgtAttr->getVCLenIndOffset();

    // source returned from hive has the format:
    //   4 bytes elem Len. elemLen bytes data.
    Int32 elemLen = *(Int32 *)srcData;
    srcData += sizeof(elemLen);

    if (elemLen >= 0) {
      if (DFS2REC::isSQLFixedChar(tgtAttr->getDatatype()))
        byte_str_cpy(tgtElemData, tgtAttr->getLength(), srcData, elemLen, ' ');
      else if (tgtAttr->isNumericWithPrecision()) {
        rc = convHiveDecimalToNumeric(srcData, elemLen, tgtElemData, tgtAttr, heap, diagsArea);
        if (rc == ex_expr::EXPR_ERROR) return rc;
      } else
        str_cpy_all(tgtElemData, srcData, elemLen);

      if ((NOT isRoot) && (tgtAttr->isVariableLength())) {
        tgtAttr->setVarLength(elemLen, tgtElemVarData);
      }

      srcData += elemLen;
    }

    if ((NOT isRoot) && (tgtAttr->getNullFlag())) {
      if (elemLen == -1) {
        tgtElemNullData[0] = -1;
        tgtElemNullData[1] = -1;
      } else {
        tgtElemNullData[0] = 0;
        tgtElemNullData[1] = 0;
      }
    }

    return ex_expr::EXPR_OK;
  }  // not composite

  CompositeAttributes *tgtCompAttrs = (CompositeAttributes *)tgtAttr;
  int numElems = *(Int32 *)srcData;
  srcData += sizeof(numElems);

  if (tgtCompAttrs->getNumElements() < numElems) {
    ExRaiseSqlError(heap, diagsArea, -3242);
    *(*diagsArea) << DgString0(
        NAString("Expected number of elements(") + str_itoa(tgtCompAttrs->getNumElements(), errBuf_) +
        ") cannot be less than the returned number of elements(" + str_itoa(numElems, errBuf2_) + ").");

    return ex_expr::EXPR_ERROR;
  }

  char *tgtCompData = tgtData + tgtAttr->getOffset();
  char *tgtCompNullData = tgtData + tgtAttr->getNullIndOffset();
  char *tgtCompVarData = tgtData + tgtAttr->getVCLenIndOffset();

  // move number of array entries to the beginning of returned row
  if (tgtAttr->getDatatype() == REC_ARRAY) {
    *(Int32 *)tgtCompData = numElems;
    tgtCompData += sizeof(Int32);
  }
  if ((NOT isRoot) && (tgtAttr->getNullFlag())) {
    tgtCompNullData[0] = 0;
    tgtCompNullData[1] = 0;
  }

  for (Int32 i = 0; i < numElems; i++) {
    Attributes *tgtElemAttr = (Attributes *)(tgtCompAttrs->getElements())[i];
    if (tgtElemAttr->isComposite()) srcData += sizeof(Int32);  // skip length
    rc = copyHiveSrcToTgt(tgtElemAttr, srcData, tgtCompData, heap, diagsArea, FALSE);
    if (rc != ex_expr::EXPR_OK) return rc;
  }  // for

  if ((NOT isRoot) && (tgtAttr->isVariableLength())) tgtAttr->setVarLength(tgtAttr->getLength(), tgtCompVarData);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpCompositeHiveCast::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ex_expr::exp_return_type rc;

  char *srcData = op_data[1];
  char *tgtData = op_data[0];

  Attributes *tgtAttrs = (Attributes *)compAttrs_;
  CompositeAttributes *tgtCompAttrs = (CompositeAttributes *)tgtAttrs;

  // target storage format must be exploded
  if (tgtCompAttrs->getCompFormat() != ExpTupleDesc::SQLARK_EXPLODED_FORMAT) {
    ExRaiseSqlError(heap, diagsArea, -3242);
    *(*diagsArea) << DgString0("CompositHiveCast target must be in exploded format.");

    return ex_expr::EXPR_ERROR;
  }

  if (NOT((getOperand(0)->isVariableLength()) && (getOperand(0)->getNullFlag()))) {
    ExRaiseSqlError(heap, diagsArea, -3242);
    *(*diagsArea) << DgString0("CompositHiveCast target must be nullable varchar.");

    return ex_expr::EXPR_ERROR;
  }

  Int32 tgtOffset = tgtAttrs->getOffset();  // save current offset
  tgtAttrs->setOffset(0);                   // row will be formed starting at 'tgtData'
  rc = copyHiveSrcToTgt(tgtAttrs, srcData, tgtData, heap, diagsArea, TRUE);
  tgtAttrs->setOffset(tgtOffset);  // restore saved offset
  if (rc == ex_expr::EXPR_ERROR) return rc;

  if (getOperand(0)->isVariableLength())
    getOperand(0)->setVarLength(getOperand(0)->getLength(), op_data[-MAX_OPERANDS]);

  return rc;
}

///////////////////////////////////////////////////////////
// ExpCompositeConcat
///////////////////////////////////////////////////////////
ExpCompositeConcat::ExpCompositeConcat(OperatorTypeEnum oper_type, short type, Attributes **attr,
                                       AttributesPtr compAttrs, AttributesPtr compAttrsChild1,
                                       AttributesPtr compAttrsChild2, Space *space)
    : ExpCompositeBase(oper_type, type, 0, 3, attr, 0, NULL, NULL, compAttrs, space),
      compAttrsChild1_(compAttrsChild1),
      compAttrsChild2_(compAttrsChild2) {}

Long ExpCompositeConcat::pack(void *space) {
  compAttrsChild1_.pack(space);
  compAttrsChild2_.pack(space);

  return ExpCompositeBase::pack(space);
}

int ExpCompositeConcat::unpack(void *base, void *reallocator) {
  if (compAttrsChild1_.unpack(base, reallocator)) return -1;
  if (compAttrsChild2_.unpack(base, reallocator)) return -1;

  return ExpCompositeBase::unpack(base, reallocator);
}

ex_expr::exp_return_type ExpCompositeConcat::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  char *child1Data = op_data[1];
  char *child2Data = op_data[2];
  char *resultData = op_data[0];

  Int32 child1NumElems = *(Int32 *)child1Data;
  Int32 child2NumElems = *(Int32 *)child2Data;
  Int32 numElems = child1NumElems + child2NumElems;

  child1Data += sizeof(Int32);
  child2Data += sizeof(Int32);

  *(Int32 *)resultData = numElems;
  resultData += sizeof(Int32);

  ExpAlignedFormat *eaf = NULL;
  char *currResultData = NULL;
  char *currResultNullData = NULL;
  Attributes *resultAttrs = NULL;
  Attributes *resultAttr = NULL;
  resultAttrs = (Attributes *)compAttrs_;
  resultAttr = (Attributes *)(((CompositeAttributes *)resultAttrs)->getElements())[0];
  if (getOperand(0)->isSQLMXAlignedFormat()) {
    eaf = (ExpAlignedFormat *)resultData;
    eaf->setupHeaderAndVOAs(numElems, numElems, (resultAttr->getVCIndicatorLength() > 0 ? numElems : 0));

    currResultData = resultData + eaf->getFirstFixedOffset();
    currResultNullData = resultData + eaf->getBitmapOffset();
  } else {
    currResultData = resultData;
    if (resultAttr->getNullIndicatorLength() > 0) {
      currResultNullData = currResultData;
      currResultData += resultAttr->getNullIndicatorLength();
    }
  }

  char *attrData = NULL;
  NABoolean isNullVal = FALSE;
  int varLen = -1;
  int attrLen = 0;

  Int32 resultElem = 0;
  Attributes *attr = NULL;
  Attributes *attrs = (Attributes *)compAttrsChild1_;
  Int32 totalLen = 0;
  for (Int32 elem = 1; elem <= child1NumElems; elem++) {
    resultElem++;
    resultAttr = (Attributes *)(((CompositeAttributes *)resultAttrs)->getElements())[resultElem - 1];

    attr = (Attributes *)(((CompositeAttributes *)attrs)->getElements())[elem - 1];

    getAttrDataPtr(attr, child1Data, attrData, varLen, isNullVal);
    if (NOT getOperand(0)->isSQLMXAlignedFormat()) {
      currResultData = resultData + resultAttr->getOffset();
      currResultNullData = resultData + resultAttr->getNullIndOffset();
    }

    if (resultAttr->getNullFlag()) {
      if (isNullVal) {
        // move null value to result.
        ExpTupleDesc::setNullValue(currResultNullData, resultElem, getOperand(0)->getTupleFormat());
        continue;
      }

      ExpTupleDesc::clearNullValue(currResultNullData, resultElem, getOperand(0)->getTupleFormat());

      totalLen += resultAttr->getNullIndicatorLength();
    }

    attrLen = (varLen >= 0 ? varLen : attr->getLength(attrData));
    memcpy(currResultData, attrData, attrLen);
    totalLen += attrLen;
  }  // for

  attrs = (Attributes *)compAttrsChild2_;
  for (Int32 elem = 1; elem <= child2NumElems; elem++) {
    resultElem++;
    resultAttr = (Attributes *)(((CompositeAttributes *)resultAttrs)->getElements())[resultElem - 1];

    attr = (Attributes *)(((CompositeAttributes *)attrs)->getElements())[elem - 1];

    getAttrDataPtr(attr, child2Data, attrData, varLen, isNullVal);
    if (NOT getOperand(0)->isSQLMXAlignedFormat()) {
      currResultData = resultData + resultAttr->getOffset();
      currResultNullData = resultData + resultAttr->getNullIndOffset();
    }

    if (resultAttr->getNullFlag()) {
      if (isNullVal) {
        // move null value to result.
        ExpTupleDesc::setNullValue(currResultNullData, resultElem, getOperand(0)->getTupleFormat());
        continue;
      }

      ExpTupleDesc::clearNullValue(currResultNullData, resultElem, getOperand(0)->getTupleFormat());
      totalLen += resultAttr->getNullIndicatorLength();
    }

    attrLen = (varLen >= 0 ? varLen : attr->getLength(attrData));
    memcpy(currResultData, attrData, attrLen);
    totalLen += attrLen;
  }  // for

  Int32 tgtLen = sizeof(Int32) + totalLen;
  if (getOperand(0)->getVCIndicatorLength() > 0) getOperand(0)->setVarLength(tgtLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

void ExpCompositeConcat::displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum, char *constsArea,
                                         ULng32 flag) {
  ExpCompositeBase::displayContents(space, "ExpCompositeConcat", clauseNum, constsArea, flag);
}

////////////////////////////////////////////////////////////////////
// ExpCompositeCreate
////////////////////////////////////////////////////////////////////
ExpCompositeCreate::ExpCompositeCreate(OperatorTypeEnum oper_type, short type, ULng32 numElements, short numAttrs,
                                       Attributes **attr, ULng32 compRowLen, ex_expr *compExpr,
                                       ex_cri_desc *compCriDesc, AttributesPtr compAttrs, Space *space)
    : ExpCompositeBase(oper_type, type, numElements, numAttrs, attr, compRowLen, compExpr, compCriDesc, compAttrs,
                       space) {}

Long ExpCompositeCreate::pack(void *space) { return ExpCompositeBase::pack(space); }

int ExpCompositeCreate::unpack(void *base, void *reallocator) { return ExpCompositeBase::unpack(base, reallocator); }

ex_expr::exp_return_type ExpCompositeCreate::eval(char *op_data[], atp_struct *atp1, atp_struct *atp2, atp_struct *atp3,
                                                  CollHeap *heap, ComDiagsArea **diagsArea) {
  ULng32 actualRowLen = 0;
  ex_expr::exp_return_type evalRetCode = getCompExpr()->eval(atp1, atp2, compAtp_, heap, -1, &actualRowLen, NULL, NULL);
  ComDiagsArea *tempDA = atp1->getDiagsArea();
  Int32 actualElems = -1;
  if ((evalRetCode == ex_expr::EXPR_ERROR) && (getOperand(0)->getDatatype() == REC_ARRAY) &&
      (tempDA && (tempDA->contains(-8147)))) {
    ComCondition *cc = tempDA->findCondition(-8147);
    actualElems = cc->getOptionalInteger(1);

    tempDA->clear();
    evalRetCode = ex_expr::EXPR_OK;
  }

  if (evalRetCode == ex_expr::EXPR_ERROR) {
    if (atp1->getDiagsArea()) {
      *diagsArea = atp1->getDiagsArea();
    }

    if (((!diagsArea) || (!*diagsArea)) || (diagsArea && *diagsArea && (NOT(*diagsArea)->contains(-8146)))) {
      ExRaiseSqlError(heap, diagsArea, -8146);
      *(*diagsArea) << DgString0("Error returned from ExpCompositeCreate::getCompExpr()->eval() method.");
    }

    return ex_expr::EXPR_ERROR;
  }

  int maxTgtLen = getOperand(0)->getLength();

  actualRowLen = compRowLen_;

  // this will happen if there are no varchars. In that case, compiletime
  // maxTgtLen is the actual len
  if (actualRowLen == 0) actualRowLen = maxTgtLen;

  if (actualRowLen > maxTgtLen) {
    char buf[200];
    sprintf(buf, "actualRowLen(%d) is greater than maxRowLen(%d) in method ExpCompositeCreate::eval.", actualRowLen,
            maxTgtLen);
    ExRaiseSqlError(heap, diagsArea, -8146);
    *(*diagsArea) << DgString0(buf);

    return ex_expr::EXPR_ERROR;
  }

  getOperand(0)->setVarLength(actualRowLen, op_data[-MAX_OPERANDS]);

  // move number of array entries to the beginning of returned row
  if (getOperand(0)->getDatatype() == REC_ARRAY) {
    Int32 numElems = numElements();
    if (actualElems >= 0) numElems = actualElems;

    *(Int32 *)op_data[0] = numElems;

    str_cpy_all(&op_data[0][sizeof(Int32)], compRow_, actualRowLen);
  } else
    str_cpy_all(op_data[0], compRow_, actualRowLen);

  char *resultNull = op_data[-2 * MAX_OPERANDS];
  ExpTupleDesc::clearNullValue(resultNull, getOperand(0)->getNullBitIndex(), getOperand(0)->getTupleFormat());

  return ex_expr::EXPR_OK;
}

void ExpCompositeCreate::displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum, char *constsArea,
                                         ULng32 flag) {
  ExpCompositeBase::displayContents(space, "ExpCompositeCreate", clauseNum, constsArea, flag);
}

///////////////////////////////////////////////////////////
// ExpCompositeDisplay
///////////////////////////////////////////////////////////
ExpCompositeDisplay::ExpCompositeDisplay(OperatorTypeEnum oper_type, short type, ULng32 numElements, short numAttrs,
                                         Attributes **attr, ex_cri_desc *compCriDesc, AttributesPtr compAttrs,
                                         Space *space)
    : ExpCompositeBase(oper_type, type, numElements, numAttrs, attr, 0, NULL, compCriDesc, compAttrs, space) {}

static int getDisplayLength(int datatype, int length, int precision, int scale) {
  return NAType::getDisplayLengthStatic(datatype, length, precision, scale, 0);
}

static short displayValue(Attributes *attr, char *tgtData, UInt32 &currTgtIndex, char *srcData, int varLen) {
  Int32 tgtDataType = CharInfo::getFSTypeFixedChar(CharInfo::ISO88591);

  Int32 datatype = attr->getDatatype();
  Int32 precision = attr->getPrecision();
  Int32 scale = attr->getScale();

  UInt32 length = (varLen >= 0 ? varLen : attr->getLength(srcData));
  Int32 tgtLen = getDisplayLength(datatype, length, precision, scale);

  char dispBuf[tgtLen + 1];
  dispBuf[tgtLen] = 0;

  ex_expr::exp_return_type retcode = convDoIt(srcData, length, datatype, precision, scale, dispBuf, tgtLen, tgtDataType,
                                              0,                   // targetPrecision
                                              CharInfo::ISO88591,  // TBD: handle UTF8
                                              NULL, 0);
  if (retcode == ex_expr::EXPR_ERROR) return -1;

  int len = tgtLen;
  char *trimBuf = str_strip_blanks(dispBuf, len, TRUE, TRUE);
  memcpy(tgtData, trimBuf, len);
  currTgtIndex += len;

  return 0;
}

short displayValues(Attributes *attr, char *tgtPtr, char *srcPtr, int varLen, UInt32 &currTgtIndex) {
  if (NOT((attr->getDatatype() == REC_ARRAY) || (attr->getDatatype() == REC_ROW))) {
    if (displayValue(attr, &tgtPtr[currTgtIndex], currTgtIndex, srcPtr, varLen)) return -1;

    return 0;
  }

  CompositeAttributes *compAttrs = (CompositeAttributes *)attr;
  if (attr->getDatatype() == REC_ARRAY) {
    memcpy(&tgtPtr[currTgtIndex], "[", 1);
  } else if (attr->getDatatype() == REC_ROW) {
    memcpy(&tgtPtr[currTgtIndex], "(", 1);
  }

  currTgtIndex++;

  Int32 numElements = compAttrs->getNumElements();
  if (attr->getDatatype() == REC_ARRAY) {
    numElements = *(Int32 *)srcPtr;
    srcPtr += sizeof(Int32);
  }

  for (Int32 i = 0; i < numElements; i++) {
    AttributesPtrPtr app = compAttrs->getElements();
    AttributesPtr ap = app[i];
    Attributes *attr = (Attributes *)ap;

    char *attrData = NULL;
    NABoolean isNullVal = FALSE;
    int varLen = -1;
    getAttrDataPtr(attr, srcPtr, attrData, varLen, isNullVal);

    if (isNullVal) {
      memcpy(&tgtPtr[currTgtIndex], "?", 1);
      currTgtIndex++;
    } else {
      if (displayValues(attr, tgtPtr, attrData, varLen, currTgtIndex)) return -1;
    }

    if (i < numElements - 1) {
      memcpy(&tgtPtr[currTgtIndex], ",", 1);
      currTgtIndex += 1;
    }
  }  // for

  if (attr->getDatatype() == REC_ARRAY) {
    memcpy(&tgtPtr[currTgtIndex], "]", 1);
  } else if (attr->getDatatype() == REC_ROW) {
    memcpy(&tgtPtr[currTgtIndex], ")", 1);
  }

  currTgtIndex++;

  return 0;
}

void ExpCompositeDisplay::displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum,
                                          char *constsArea) {
  char buf[200];

  str_sprintf(buf, "  Clause #%d: ExpCompositeDisplay, type: %s, numElements: %d", clauseNum,
              (type_ == 1 ? "ARRAY" : "ROW"), numElements_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, " ");
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  str_sprintf(buf, "  compAttrs_ begin");
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  displayAttr(space, compAttrs_, 1, constsArea);

  str_sprintf(buf, "  compAttrs_ end");
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  str_sprintf(buf, " ");
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  ex_clause::displayContents(space, NULL, clauseNum, constsArea);
}

ex_expr::exp_return_type ExpCompositeDisplay::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  int srcLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  UInt32 currTgtIndex = 0;
  if (displayValues(compAttrs_, op_data[0], op_data[1], srcLen, currTgtIndex)) {
    ExRaiseSqlError(heap, diagsArea, -8146);
    *(*diagsArea) << DgString0("Error returned while displaying value.");

    return ex_expr::EXPR_ERROR;
  }

  int tgtLen = currTgtIndex;
  if (getOperand(0)->getVCIndicatorLength() > 0) getOperand(0)->setVarLength(tgtLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

///////////////////////////////////////////////////////////
// ExpCompositeExtract
///////////////////////////////////////////////////////////
ExpCompositeExtract::ExpCompositeExtract(OperatorTypeEnum oper_type, short type, ULng32 numElements, int elemNum,
                                         short numAttrs, Attributes **attr, AttributesPtr compAttrs,
                                         int numSearchAttrs, char *searchAttrTypeList, char *searchAttrIndexList,
                                         Space *space)
    : ExpCompositeBase(oper_type, type, numElements, numAttrs, attr, 0, NULL, NULL, compAttrs, space),
      elemNum_(elemNum),
      numSearchAttrs_(numSearchAttrs),
      searchAttrTypeList_(searchAttrTypeList),
      searchAttrIndexList_(searchAttrIndexList),
      flags_(0) {}

Long ExpCompositeExtract::pack(void *space) {
  searchAttrTypeList_.pack(space);
  searchAttrIndexList_.pack(space);

  return ExpCompositeBase::pack(space);
}

int ExpCompositeExtract::unpack(void *base, void *reallocator) {
  if (searchAttrTypeList_.unpack(base)) return -1;
  if (searchAttrIndexList_.unpack(base)) return -1;

  return ExpCompositeBase::unpack(base, reallocator);
}

void ExpCompositeExtract::displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum,
                                          char *constsArea) {
  char buf[200];

  str_sprintf(buf, "  Clause #%d: ExpCompositeExtract, type: %s", clauseNum, (type_ == 1 ? "ARRAY" : "ROW"));
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "  numElements: %d, elemNum: %d", numElements_, elemNum_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  if (elemNum_ < 0) {
    str_sprintf(buf, " ");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    str_sprintf(buf, "  numSearchAttrs: %d", numSearchAttrs_);
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    for (Int32 i = 0; i < numSearchAttrs_; i++) {
      str_sprintf(buf, "  searchAttrType[%d]: %3d, searchAttrIndex[%d]: %2d", i, getSearchAttrType(i), i,
                  getSearchAttrIndex(i));
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
  }  // elemNum_ < 0

  if (compAttrs_) {
    str_sprintf(buf, " ");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    str_sprintf(buf, "  compAttrs_ begin");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

    displayAttr(space, compAttrs_, 1, constsArea);

    str_sprintf(buf, "  compAttrs_ end");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    str_sprintf(buf, " ");
    space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
  }

  ex_clause::displayContents(space, NULL, clauseNum, constsArea);
}

ex_expr::exp_return_type ExpCompositeExtract::extractValue(Attributes *inAttrs, int elemNum, char *tgtPtr,
                                                           char *srcPtr, Int32 maxNumElems, NABoolean &isNullVal,
                                                           ULng32 &attrLen, Int32 &numElems, CollHeap *heap,
                                                           ComDiagsArea **diagsArea) {
  CompositeAttributes *compAttrs = (CompositeAttributes *)inAttrs;
  AttributesPtrPtr app = compAttrs->getElements();
  AttributesPtr ap = app[elemNum - 1];
  Attributes *attr = (Attributes *)ap;

  char *attrData = NULL;
  isNullVal = FALSE;
  int varLen = -1;

  char errBuf[200];
  if (inAttrs->getDatatype() == REC_ARRAY) {
    if (elemNum > maxNumElems)  // greater than max num elements
    {
      ExRaiseSqlError(heap, diagsArea, -3242);
      *(*diagsArea) << DgString0(NAString("Element to be extracted must be between 1 and max elements(") +
                                 str_itoa(maxNumElems, errBuf) + ").");

      return ex_expr::EXPR_ERROR;
    }

    numElems = *(Int32 *)srcPtr;
    if (elemNum > numElems) {
      isNullVal = TRUE;
      return ex_expr::EXPR_OK;
    }
    srcPtr += sizeof(Int32);
  }
  getAttrDataPtr(attr, srcPtr, attrData, varLen, isNullVal);
  if (isNullVal) return ex_expr::EXPR_OK;

  attrLen = (varLen >= 0 ? varLen : attr->getLength(attrData));

  memcpy(tgtPtr, attrData, attrLen);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpCompositeExtract::searchAndExtractValue(Attributes *inAttrs, char *tgtPtr, char *srcPtr,
                                                                    NABoolean &isNullVal, ULng32 &attrLen,
                                                                    CollHeap *heap, ComDiagsArea **diagsArea) {
  Attributes *currCompAttrs = inAttrs;
  Attributes *currAttr = NULL;

  char *currCompAttrsData = NULL;
  char *currAttrData = NULL;

  int varLen = -1;
  isNullVal = FALSE;

  currCompAttrsData = srcPtr;
  for (Int32 i = 0; i < numSearchAttrs(); i++) {
    Int32 elemNum = getSearchAttrIndex(i);
    if (elemNum <= 0) continue;

    CompositeAttributes *compAttrs = (CompositeAttributes *)currCompAttrs;
    AttributesPtrPtr app = compAttrs->getElements();
    AttributesPtr ap = app[elemNum - 1];
    currAttr = (Attributes *)ap;

    if (currCompAttrs->getDatatype() == REC_ARRAY) {
      Int32 numElems = *(Int32 *)currCompAttrsData;
      if (elemNum > numElems) {
        isNullVal = TRUE;
        return ex_expr::EXPR_OK;
      }
      currCompAttrsData += sizeof(Int32);
    }
    getAttrDataPtr(currAttr, currCompAttrsData, currAttrData, varLen, isNullVal);
    if (isNullVal) return ex_expr::EXPR_OK;

    currCompAttrs = currAttr;
    currCompAttrsData = currAttrData;
  }  // for

  attrLen = (varLen >= 0 ? varLen : currAttr->getLength(currAttrData));
  memcpy(tgtPtr, currAttrData, attrLen);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpCompositeExtract::eval(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea) {
  ex_expr::exp_return_type rc;

  char **null_data = &op_data[-2 * ex_clause::MAX_OPERANDS];
  if (ex_clause::processNulls(null_data, heap, diagsArea) == ex_expr::EXPR_NULL) return ex_expr::EXPR_OK;

  int srcLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS + 1]);

  UInt32 tgtLen = 0;

  if ((numSearchAttrs() == 0) && (elemNum_ < 0) && (!getOperand(2))) {
    ExRaiseSqlError(heap, diagsArea, -8146);
    *(*diagsArea) << DgString0(
        "In method ExpCompositeExtract::eval, numSearchAttrs() == 0  and elemNum_ == -1 and getOperand(2) == NULL");

    return ex_expr::EXPR_ERROR;
  }

  Int32 elemNum = elemNum_;

  // get elemNum from child operand 2, if specified
  if ((elemNum < 0) && (numSearchAttrs() <= 0) && (getOperand(2)) && (op_data[2])) {
    elemNum = *(Int32 *)op_data[2];
  }

  NABoolean isNullVal = FALSE;
  if (numSearchAttrs() > 0) {
    rc = searchAndExtractValue(compAttrs_, op_data[0], op_data[1], isNullVal, tgtLen, heap, diagsArea);
  } else {
    Int32 numElems = 0;
    rc = extractValue(compAttrs_, elemNum, op_data[0], op_data[1], numElements(), isNullVal, tgtLen, numElems, heap,
                      diagsArea);
    if ((compAttrs_->getDatatype() == REC_ARRAY) && (numElems > 0) && (elemNum > numElems)) {
      // return warning with numElems number.
      ExRaiseSqlError(heap, diagsArea, (ExeErrorCode)8147);
      *(*diagsArea) << DgInt0(elemNum_) << DgInt1(numElems);
      return ex_expr::EXPR_ERROR;
    }
  }
  if (rc == ex_expr::EXPR_ERROR) return rc;

  if (isNullVal) {
    // move null value to result.
    ExpTupleDesc::setNullValue(null_data[0], getOperand(0)->getNullBitIndex(), getOperand(0)->getTupleFormat());

    return ex_expr::EXPR_OK;
  } else {
    ExpTupleDesc::clearNullValue(null_data[0], getOperand(0)->getNullBitIndex(), getOperand(0)->getTupleFormat());
  }

  if (getOperand(0)->getVCIndicatorLength() > 0) getOperand(0)->setVarLength(tgtLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}
