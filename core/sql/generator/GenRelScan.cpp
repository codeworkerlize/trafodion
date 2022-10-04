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
******************************************************************************
*
* File:         GenRelScan.cpp
* Description:  Scan operators
*
* Created:      5/17/94
* Language:     C++
*
*
******************************************************************************
*/

#define SQLPARSERGLOBALS_FLAGS

#include <algorithm>
#include <vector>

#include "optimizer/Sqlcomp.h"
#include "GroupAttr.h"
#include "optimizer/ControlDB.h"
#include "GenExpGenerator.h"
#include "comexe/ComTdbDDL.h"  // for describe
#include "comexe/ComTdbHbaseAccess.h"
#include "HashRow.h"  // for sizeof(HashRow)
#include "executor/sql_buffer.h"
#include "executor/sql_buffer_size.h"
#include "optimizer/OptimizerSimulator.h"
#include "RelUpdate.h"

#include "sqlcomp/CmpSeabaseDDL.h"
#include "sqlcat/TrafDDLdesc.h"

#include "parser/SqlParserGlobals.h"  // Parser Flags
#include "executor/HBaseClient_JNI.h"

/////////////////////////////////////////////////////////////////////
//
// Contents:
//
//   Describe::codeGen()
//   DP2Scan::codeGen()
//   FileScan::codeGen()
//
//////////////////////////////////////////////////////////////////////

short Describe::codeGen(Generator *generator) {
  Space *space = generator->getSpace();
  ExpGenerator *exp_gen = generator->getExpGenerator();

  // allocate a map table for the retrieved columns
  generator->appendAtEnd();

  ex_cri_desc *given_desc = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc *returned_desc = new (space) ex_cri_desc(given_desc->noTuples() + 1, space);

  ExpTupleDesc *tuple_desc = 0;
  ULng32 tupleLength;
  exp_gen->processValIdList(getTableDesc()->getClusteringIndex()->getIndexColumns(),
                            ExpTupleDesc::SQLARK_EXPLODED_FORMAT, tupleLength, 0,
                            // where the fetched row will be
                            returned_desc->noTuples() - 1, &tuple_desc, ExpTupleDesc::SHORT_FORMAT);

  // add this descriptor to the returned cri descriptor.
  returned_desc->setTupleDescriptor(returned_desc->noTuples() - 1, tuple_desc);

  char *query = space->allocateAndCopyToAlignedSpace(originalQuery_, strlen(originalQuery_), 0);

  ComTdbDescribe::DescribeType type = ComTdbDescribe::INVOKE_;
  ULng32 flags = 0;
  if (format_ == INVOKE_)
    type = ComTdbDescribe::INVOKE_;
  else if (format_ == SHOWSTATS_)
    type = ComTdbDescribe::SHOWSTATS_;
  else if (format_ == TRANSACTION_)
    type = ComTdbDescribe::TRANSACTION_;
  else if (format_ == SHORT_)
    type = ComTdbDescribe::SHORT_;
  else if (format_ == SHOWDDL_)
    type = ComTdbDescribe::LONG_;
  else if (format_ == PLAN_)
    type = ComTdbDescribe::PLAN_;
  else if (format_ == LABEL_)
    type = ComTdbDescribe::LABEL_;
  else if (format_ == SHAPE_)
    type = ComTdbDescribe::SHAPE_;
  else if (format_ == ENVVARS_)
    type = ComTdbDescribe::ENVVARS_;
  else if (format_ == LEAKS_) {
    type = ComTdbDescribe::LEAKS_;
#ifdef NA_DEBUG_HEAPLOG
    flags = flags_;
#else
    flags = 0;
#endif
  }

  ComTdbDescribe *desc_tdb = new (space) ComTdbDescribe(
      query, strlen(query), (Int16)SQLCHARSETCODE_UTF8, 0, 0, 0, tupleLength, 0, 0, type, flags, given_desc,
      returned_desc, (queue_index)getDefault(GEN_DESC_SIZE_DOWN), (queue_index)getDefault(GEN_DESC_SIZE_UP),
      getDefault(GEN_DESC_NUM_BUFFERS), getDefault(GEN_DESC_BUFFER_SIZE));
  generator->initTdbFields(desc_tdb);

  if (!generator->explainDisabled()) {
    generator->setExplainTuple(addExplainInfo(desc_tdb, 0, 0, generator));
  }

  generator->setCriDesc(returned_desc, Generator::UP);
  generator->setGenObj(this, desc_tdb);

  // Do not infer that any transaction started can
  // be in READ ONLY mode for showddl and invoke.
  // This causes trouble for MP table access.
  generator->setNeedsReadWriteTransaction(TRUE);

  // don't need a transaction for showtransaction statement.

  if (maybeInMD()) {
    BindWA *bindWA = generator->getBindWA();
    CorrName descCorrName = getDescribedTableName();
    Lng32 daMark = CmpCommon::diags()->mark();
    NATable *describedTable = bindWA->getSchemaDB()->getNATableDB()->get(descCorrName, bindWA, NULL, FALSE);
    CmpCommon::diags()->rewind(daMark, TRUE);

    if (describedTable && describedTable->isSeabaseTable() && describedTable->isEnabledForDDLQI())
      generator->objectUids().insert(describedTable->objectUid().get_value());
  }

  return 0;
}

/////////////////////////////////////////////////////////////
//
// FileScan::codeGen()
//
/////////////////////////////////////////////////////////////
short FileScan::codeGen(Generator *generator) {
  if (getTableDesc()->getNATable()->isHiveTable()) {
    return 0;
  }

  GenAssert(0, "FileScan::codeGen. Should never reach here.");

  return 0;
}

int HbaseAccess::createAsciiColAndCastExprForExtStorage(Generator *generator, const NAType &castType,
                                                        const NAType *underlyingHiveType, ItemExpr *&asciiValue,
                                                        ItemExpr *&castValue) {
  int result = 0;
  asciiValue = NULL;
  castValue = NULL;
  CollHeap *h = generator->wHeap();

  const NAType *asciiType = (underlyingHiveType ? underlyingHiveType : &castType);
  if ((underlyingHiveType) && (DFS2REC::isSQLVarChar(underlyingHiveType->getFSDatatype())) &&
      (castType.getTypeQualifier() == NA_CHARACTER_TYPE)) {
    SQLVarChar *vcAsciiType = (SQLVarChar *)underlyingHiveType;

    // if underlying hive type passed in is a hive 'string'
    // datatype that is being mapped to external table char/varchar type,
    // then set its size and charset to be the same as external table datatype.
    Lng32 maxByteLen = castType.getNominalSize();
    if ((vcAsciiType->getHiveTypeName() == hiveProtoTypeKindStr[HIVE_STRING_TYPE]) &&
        ((maxByteLen != vcAsciiType->getNominalSize()) || (castType.getCharSet() != vcAsciiType->getCharSet()))) {
      vcAsciiType = new (h) SQLVarChar(h, maxByteLen, vcAsciiType->supportsSQLnull(), vcAsciiType->isUpshifted(),
                                       vcAsciiType->isCaseinsensitive(), castType.getCharSet(),
                                       vcAsciiType->getCollation(), vcAsciiType->getCoercibility());
    }
    asciiType = vcAsciiType;
  }

  // if this is an upshifted datatype, remove the upshift attr.
  // We dont want to upshift data during retrievals or while building keys.
  // Data is only upshifted during insert or updates.
  const NAType *newCastType = &castType;
  if (newCastType->getTypeQualifier() == NA_CHARACTER_TYPE && ((CharType *)newCastType)->isUpshifted()) {
    newCastType = newCastType->newCopy(h);
    ((CharType *)newCastType)->setUpshifted(FALSE);
  }

  if (asciiType->isComposite()) {
    NAType *newCompType = ((CompositeType *)asciiType)->newCopy2(COM_SQLARK_EXPLODED_FORMAT, h);

    Int32 hiveSrcMaxLen = ((CompositeType *)asciiType)->getHiveSourceMaxLen();
    SQLVarChar *vcAsciiType =
        new (h) SQLVarChar(h, hiveSrcMaxLen, TRUE, FALSE, FALSE, CharInfo::DefaultCharSet, CharInfo::DefaultCollation,
                           CharInfo::COERCIBLE, CharInfo::UnknownCharSet,
                           // sizeof(Int32));
                           0);
    asciiType = vcAsciiType;
    asciiValue = new (h) NATypeToItem((NAType *)asciiType);

    castValue = new (h) CompositeHiveCast(asciiValue, newCompType,
                                          TRUE);  // from hive to traf cast
    castValue = castValue->bindNode(generator->getBindWA());
    if (generator->getBindWA()->errStatus()) {
      GenExit();
      return 0;
    }
    castValue = castValue->preCodeGen(generator);

    const NAType &castType1 = castValue->getValueId().getType();
    const CompositeType &compType1 = (CompositeType &)castType1;

    if (((CompositeType *)newCastType)->getCompFormat() != compType1.getCompFormat()) {
      castValue = new (h) CompositeCast(castValue, newCastType);
      castValue = castValue->bindNode(generator->getBindWA());
      if (generator->getBindWA()->errStatus()) {
        GenExit();
        return 0;
      }
      castValue = castValue->preCodeGen(generator);
    }
  } else {
    asciiValue = new (h) NATypeToItem((NAType *)asciiType);

    castValue = new (h) Cast(asciiValue, newCastType);

    // this is an explicit cast. Do not do implicit casting and return
    // truncation errors.
    if (underlyingHiveType) {
      ((Cast *)castValue)->setTgtCSSpecified(TRUE);
      ((Cast *)castValue)->setCheckTruncationError(TRUE);
    }

    if (newCastType->getTypeQualifier() == NA_INTERVAL_TYPE) ((Cast *)castValue)->setAllowSignInInterval(TRUE);
  }

  if (castValue && asciiValue) result = 1;

  return result;
}

int HbaseAccess::createAsciiColAndCastExpr(Generator *generator, const NAType &givenType, ItemExpr *&asciiValue,
                                           ItemExpr *&castValue, NABoolean srcIsInt32Varchar) {
  int result = 0;
  asciiValue = NULL;
  castValue = NULL;
  CollHeap *h = generator->wHeap();
  bool needTranslate = FALSE;
  UInt32 hiveScanMode = CmpCommon::getDefaultLong(HIVE_SCAN_SPECIAL_MODE);

  // if this is an upshifted datatype, remove the upshift attr.
  // We dont want to upshift data during retrievals or while building keys.
  // Data is only upshifted during insert or updates.
  const NAType *newGivenType = &givenType;
  if (newGivenType->getTypeQualifier() == NA_CHARACTER_TYPE && ((CharType *)newGivenType)->isUpshifted()) {
    newGivenType = newGivenType->newCopy(h);
    ((CharType *)newGivenType)->setUpshifted(FALSE);
  }

  if (newGivenType->getTypeQualifier() == NA_CHARACTER_TYPE &&
      (CmpCommon::getDefaultString(HIVE_FILE_CHARSET) == "GBK" ||
       CmpCommon::getDefaultString(HIVE_FILE_CHARSET) == "gbk") &&
      CmpCommon::getDefaultString(HIVE_DEFAULT_CHARSET) == "UTF8")
    needTranslate = TRUE;

  // source ascii row is a varchar where the data is a pointer to the source data
  // in the hdfs buffer.
  NAType *asciiType = NULL;

  if (DFS2REC::isDoubleCharacter(newGivenType->getFSDatatype())) {
    asciiType = new (h) SQLVarChar(h, sizeof(Int64) / 2, newGivenType->supportsSQLnull(), FALSE, FALSE,
                                   newGivenType->getCharSet(), CharInfo::DefaultCollation, CharInfo::COERCIBLE,
                                   CharInfo::UnknownCharSet, (srcIsInt32Varchar ? sizeof(Int32) : 0));
  }
  // set the source charset to GBK if HIVE_FILE_CHARSET is set
  // HIVE_FILE_CHARSET can only be empty or GBK
  else if (needTranslate == TRUE) {
    asciiType = new (h) SQLVarChar(h, sizeof(Int64), newGivenType->supportsSQLnull(), FALSE, FALSE, CharInfo::GBK,
                                   CharInfo::DefaultCollation, CharInfo::COERCIBLE, CharInfo::UnknownCharSet,
                                   (srcIsInt32Varchar ? sizeof(Int32) : 0));
  } else {
    asciiType = new (h) SQLVarChar(h, sizeof(Int64), newGivenType->supportsSQLnull(), FALSE, FALSE,
                                   CharInfo::DefaultCharSet, CharInfo::DefaultCollation, CharInfo::COERCIBLE,
                                   CharInfo::UnknownCharSet, (srcIsInt32Varchar ? sizeof(Int32) : 0));
  }

  asciiValue = new (h) NATypeToItem(asciiType->newCopy(h));

  castValue = new (h) Cast(asciiValue, newGivenType);
  ((Cast *)castValue)->setSrcIsVarcharPtr(TRUE);

  if ((hiveScanMode & 2) > 0) ((Cast *)castValue)->setConvertNullWhenError(TRUE);

  if (newGivenType->getTypeQualifier() == NA_INTERVAL_TYPE) ((Cast *)castValue)->setAllowSignInInterval(TRUE);

  if (DFS2REC::isBinaryString(givenType.getFSDatatype()))
    castValue = new (h) DecodeBase64(castValue, NULL, CharInfo::ISO88591, h);

  if (castValue && asciiValue) result = 1;

  return result;
}

int HbaseAccess::createAsciiColAndCastExpr2(Generator *generator, ItemExpr *colNode, const NAType &givenType,
                                            ItemExpr *&asciiValue, ItemExpr *&castValue, NABoolean alignedFormat) {
  asciiValue = NULL;
  castValue = NULL;
  CollHeap *h = generator->wHeap();

  // if this is an upshifted datatype, remove the upshift attr.
  // We dont want to upshift data during retrievals or while building keys.
  // Data is only upshifted during insert or updates.
  const NAType *newGivenType = &givenType;
  if (newGivenType->getTypeQualifier() == NA_CHARACTER_TYPE && ((CharType *)newGivenType)->isUpshifted()) {
    newGivenType = newGivenType->newCopy(h);
    ((CharType *)newGivenType)->setUpshifted(FALSE);
  }

  NABoolean encodingNeeded = FALSE;
  asciiValue = new (h) NATypeToItem(newGivenType->newCopy(h));
  castValue = new (h) Cast(asciiValue, newGivenType);

  if ((!alignedFormat) && HbaseAccess::isEncodingNeededForSerialization(colNode)) {
    castValue = new (generator->wHeap()) CompDecode(castValue, newGivenType->newCopy(h), FALSE, TRUE);
  }

  return 1;
}

int HbaseAccess::createAsciiColAndCastExpr3(Generator *generator, const NAType &givenType, ItemExpr *&asciiValue,
                                            ItemExpr *&castValue) {
  asciiValue = NULL;
  castValue = NULL;
  CollHeap *h = generator->wHeap();

  // if this is an upshifted datatype, remove the upshift attr.
  // We dont want to upshift data during retrievals or while building keys.
  // Data is only upshifted during insert or updates.
  const NAType *newGivenType = &givenType;
  if (newGivenType->getTypeQualifier() == NA_CHARACTER_TYPE && ((CharType *)newGivenType)->isUpshifted()) {
    newGivenType = newGivenType->newCopy(h);
    ((CharType *)newGivenType)->setUpshifted(FALSE);
  }

  NAType *asciiType = NULL;

  // source data is in string format.
  // Use cqd to determine max len of source if source type is not string.
  // If cqd is not set, use displayable length for the given datatype.
  Lng32 cvl = 0;
  if (NOT(DFS2REC::isAnyCharacter(newGivenType->getFSDatatype()))) {
    cvl = (Lng32)CmpCommon::getDefaultNumeric(HBASE_MAX_COLUMN_VAL_LENGTH);
    if (cvl <= 0) {
      // compute col val length
      cvl = newGivenType->getDisplayLength();
    }
  } else {
    cvl = newGivenType->getNominalSize();
  }

  asciiType = new (h) SQLVarChar(h, cvl, newGivenType->supportsSQLnull());

  //  asciiValue = new (h) NATypeToItem(newGivenType->newCopy(h));
  asciiValue = new (h) NATypeToItem(asciiType);
  castValue = new (h) Cast(asciiValue, newGivenType);

  return 1;
}

NABoolean HbaseAccess::columnEnabledForSerialization(ItemExpr *colNode) {
  NAColumn *nac = NULL;
  if (colNode->getOperatorType() == ITM_BASECOLUMN) {
    nac = ((BaseColumn *)colNode)->getNAColumn();
  } else if (colNode->getOperatorType() == ITM_INDEXCOLUMN) {
    nac = ((IndexColumn *)colNode)->getNAColumn();
  }

  return CmpSeabaseDDL::enabledForSerialization(nac);
}

NABoolean HbaseAccess::isEncodingNeededForSerialization(ItemExpr *colNode) {
  NAColumn *nac = NULL;
  if (colNode->getOperatorType() == ITM_BASECOLUMN) {
    nac = ((BaseColumn *)colNode)->getNAColumn();
  } else if (colNode->getOperatorType() == ITM_INDEXCOLUMN) {
    nac = ((IndexColumn *)colNode)->getNAColumn();
  }

  return CmpSeabaseDDL::isEncodingNeededForSerialization(nac);
}

Int64 bestBlockNb(Int64 offset, Int64 size, Int64 blocksize) {
  Int64 blockBoundary = ((offset + size) / blocksize) * blocksize;
  if (blockBoundary == 0) return 0;
  if ((blockBoundary - offset) > (offset + size - blockBoundary))
    return (blockBoundary / blocksize) - 1;
  else
    return (blockBoundary / blocksize);
}

char *FileScan::genExplodedHivePartKeyVals(Generator *generator, ExpTupleDesc *partCols, const ValueIdList &valList) {
  char *tgt = generator->getSpace()->allocateAlignedSpace(partCols->tupleDataLength());
  int numPartCols = partCols->numAttrs();

  GenAssert(valList.entries() == numPartCols, "Number of Hive part key cols must match col value list");

  for (int pc = 0; pc < numPartCols; pc++) {
    NABoolean dummy;
    ConstValue *cv = valList[pc].getItemExpr()->castToConstValue(dummy);
    GenAssert(cv, "Hive part col constant not found");
    GenAssert(!dummy, "Negate not allowed in part col const");
    const NAType &srcType = cv->getValueId().getType();
    int srcNullIndOffset;
    int srcVcLenOffset;
    int srcDataOffset;
    Attributes *attr = partCols->getAttr(pc);
    const char *src = static_cast<char *>(cv->getConstValue());

    // get the data layout of the binary buffer in the constant value
    cv->getOffsetsInBuffer(srcNullIndOffset, srcVcLenOffset, srcDataOffset);

    // move null indicator, VC length indicator, data separately because
    // of alignment issues, even though the types of source and target match
    if (attr->getNullFlag()) {
      memcpy(&tgt[attr->getNullIndOffset()], &src[srcNullIndOffset], attr->getNullIndicatorLength());
    }

    if (attr->isVariableLength()) {
      GenAssert(srcVcLenOffset >= 0, "Fixed char to varchar in genExplodedHivePartKeyVals");
      memcpy(&tgt[attr->getVCLenIndOffset()], &src[srcVcLenOffset], attr->getVCIndicatorLength());
    }

    memcpy(&tgt[attr->getOffset()], &src[srcDataOffset], attr->getLength());
  }

  return tgt;
}

static Int32 computeMinBytesForPrecision(Int32 precision) {
  Int32 numBytes = 1;
  while (pow(2.0, 8 * numBytes - 1) < pow(10.0, precision)) {
    numBytes += 1;
  }

  return numBytes;
}

static short genParqColSchStr(Generator *generator, const NAString &inColName, const NAType *nat, NAString &schStrNAS) {
  NAString colName(inColName);
  colName.toLower();

  schStrNAS += " optional";
  switch (nat->getHiveType()) {
    case HIVE_BOOLEAN_TYPE: {
      schStrNAS += " boolean " + colName + ";";
    } break;

    case HIVE_BYTE_TYPE: {
      schStrNAS += " int32 " + colName + ";";
    } break;

    case HIVE_SHORT_TYPE: {
      schStrNAS += " int32 " + colName + ";";
    } break;

    case HIVE_INT_TYPE: {
      schStrNAS += " int32 " + colName + ";";
    } break;

    case HIVE_LONG_TYPE: {
      schStrNAS += " int64 " + colName + ";";
    } break;

    case HIVE_FLOAT_TYPE: {
      schStrNAS += " float " + colName + ";";
    } break;

    case HIVE_DOUBLE_TYPE: {
      schStrNAS += " double " + colName + ";";
    } break;

    case HIVE_CHAR_TYPE:
    case HIVE_VARCHAR_TYPE:
    case HIVE_STRING_TYPE:
    case HIVE_BINARY_TYPE: {
      schStrNAS += " binary " + colName + " (UTF8)" + ";";
    } break;

    case HIVE_DECIMAL_TYPE: {
      char buf1[100];
      char buf2[100];
      Int32 precision = nat->getPrecision();
      Int32 scale = nat->getScale();

      int numBytes = computeMinBytesForPrecision(precision);
      schStrNAS += NAString(" fixed_len_byte_array(") + str_itoa(numBytes, buf1) + ") ";
      schStrNAS += colName + " (DECIMAL(" + str_itoa(precision, buf1) + "," + str_itoa(scale, buf2) + "))" + ";";
    } break;

    case HIVE_TIMESTAMP_TYPE: {
      if (CmpCommon::getDefault(PARQUET_LEGACY_TIMESTAMP_FORMAT) == DF_ON)
        schStrNAS += " int96 " + colName + ";";
      else
        schStrNAS += " int64 " + colName + ";";
    } break;

    case HIVE_ARRAY_TYPE: {
      SQLArray *sa = (SQLArray *)nat;
      const NAType *elemType = sa->getElementType();
      schStrNAS += " group " + colName + " (LIST) { \n";
      schStrNAS += " repeated group bag { \n";
      if (genParqColSchStr(generator, "array_element", elemType, schStrNAS)) return -1;

      schStrNAS += " }\n";
      schStrNAS += " }";
    } break;

    case HIVE_STRUCT_TYPE: {
      SQLRow *sr = (SQLRow *)nat;
      schStrNAS += " group " + colName + " { \n";
      for (Int32 i = 0; i < sr->getNumElements(); i++) {
        const NAType *elemType = sr->fieldTypes()[i];
        const NAString &elemName = sr->fieldNames()[i];
        if (genParqColSchStr(generator, elemName.data(), elemType, schStrNAS)) return -1;
      }  // for
      schStrNAS += " }";
    } break;

    default: {
    } break;

  }  // switch

  schStrNAS += "\n";

  return 0;
}

short FileScan::createParqTableSchStr(Generator *generator, NAString &tableName, const NAColumnArray &hiveColArr,
                                      char *&schStr) {
  Space *space = generator->getSpace();

  NAString schStrNAS;
  schStrNAS = NAString("message ") + tableName + " { \n";
  for (int i = 0; i < hiveColArr.entries(); i++) {
    const NAColumn *hiveNACol = hiveColArr[i];
    const NAType *hiveNAType = hiveNACol->getType();

    if ((hiveNACol->isHiveVirtualColumn()) || (hiveNACol->isHivePartColumn())) continue;

    if (genParqColSchStr(generator, hiveNACol->getColName(), hiveNAType, schStrNAS)) return -1;
  }

  schStrNAS += "}";

  schStr = space->allocateAndCopyToAlignedSpace(schStrNAS.data(), schStrNAS.length(), 0);

  return 0;
}

// create list of name and types that will be passed on to external storage
// reader/writer methods.
// Types defined in enum HiveProtoTypeKind in ComSmallDefs.h.
short FileScan::createHiveColNameAndTypeLists(Generator *generator, const NAColumnArray &hiveColArr,
                                              Queue *&colNameList, Queue *&colTypeList) {
  Space *space = generator->getSpace();

  if (colNameList == NULL) colNameList = new (space) Queue(space);
  if (colTypeList == NULL) colTypeList = new (space) Queue(space);

  CollIndex hiveColIx = 0;

  for (int i = 0; i < hiveColArr.entries(); i++) {
    const NAColumn *hiveNACol = hiveColArr[i];
    const NAType *hiveNAType = hiveNACol->getType();

    if ((hiveNACol->isHiveVirtualColumn()) || (hiveNACol->isHivePartColumn())) continue;

    NAString colName(hiveNACol->getColName());
    colName.toLower();
    char *cnameInList = space->allocateAndCopyToAlignedSpace(colName.data(), colName.length(), 0);
    colNameList->insert(cnameInList);

    Int32 hiveType = hiveNAType->getHiveType();
    Int32 hiveLength = hiveNAType->getNominalSize();
    Int32 hivePrecision = hiveNAType->getPrecision();
    Int32 hiveScale = hiveNAType->getScale();

    Lng32 typeInfo[4];
    typeInfo[0] = hiveType;
    typeInfo[1] = hiveLength;
    typeInfo[2] = hivePrecision;
    typeInfo[3] = hiveScale;

    char *ctypeInList = space->allocateAndCopyToAlignedSpace((char *)&typeInfo, sizeof(typeInfo), 0);
    colTypeList->insert(ctypeInList);
  }  // for

  return 0;
}

/////////////////////////////////////////////////////////////
//
// HbaseAccess::validateVirtualTableDesc
//
/////////////////////////////////////////////////////////////
NABoolean HbaseAccess::validateVirtualTableDesc(NATable *naTable) {
  if ((NOT naTable->isHbaseCellTable()) && (NOT naTable->isHbaseRowTable())) return FALSE;

  NABoolean isRW = naTable->isHbaseRowTable();

  const NAColumnArray &naColumnArray = naTable->getNAColumnArray();

  Lng32 v1 = (Lng32)CmpCommon::getDefaultNumeric(HBASE_MAX_COLUMN_NAME_LENGTH);
  Lng32 v2 = (Lng32)CmpCommon::getDefaultNumeric(HBASE_MAX_COLUMN_VAL_LENGTH);
  Lng32 v3 = (Lng32)CmpCommon::getDefaultNumeric(HBASE_MAX_COLUMN_INFO_LENGTH);

  NAColumn *nac = NULL;
  for (Lng32 i = 0; i < naColumnArray.entries(); i++) {
    nac = naColumnArray[i];
    Lng32 length = nac->getType()->getNominalSize();
    if (isRW) {
      if (i == HBASE_ROW_ROWID_INDEX) {
        if (length != v1) return FALSE;
      } else if (i == HBASE_COL_DETAILS_INDEX) {
        if (length != v3) return FALSE;
      }
    } else {
      if ((i == HBASE_ROW_ID_INDEX) || (i == HBASE_COL_NAME_INDEX) || (i == HBASE_COL_FAMILY_INDEX)) {
        if (length != v1) return FALSE;
      } else if (i == HBASE_COL_VALUE_INDEX) {
        if (length != v2) return FALSE;
      }
    }
  }  // for

  return TRUE;
}

void populateRangeDescForBeginKey(char *buf, Int32 len, struct TrafDesc *target, NAMemory *heap) {
  target->nodetype = DESC_HBASE_RANGE_REGION_TYPE;
  target->hbaseRegionDesc()->beginKey = buf;
  target->hbaseRegionDesc()->beginKeyLen = len;
  target->hbaseRegionDesc()->endKey = NULL;
  target->hbaseRegionDesc()->endKeyLen = 0;
}

TrafDesc *HbaseAccess::createVirtualTableDesc(const char *name, NABoolean isRW, NABoolean isCW,
                                              NAArray<HbaseStr> *beginKeys, NAMemory *heap) {
  TrafDesc *table_desc = NULL;

  if (isRW)
    table_desc = Generator::createVirtualTableDesc(
        name, heap, ComTdbHbaseAccess::getVirtTableRowwiseNumCols(), ComTdbHbaseAccess::getVirtTableRowwiseColumnInfo(),
        ComTdbHbaseAccess::getVirtTableRowwiseNumKeys(), ComTdbHbaseAccess::getVirtTableRowwiseKeyInfo(),
        0,      // numConstrs = 0
        NULL,   // constrInfo = NULL
        0,      // Int32 numIndexes = 0
        NULL,   // *indexInfo = NULL
        0,      // numViews = 0
        NULL,   // viewInfo = NULL
        NULL,   // tableInfo = NULL
        NULL,   // seqInfo = NULL
        NULL,   // * statsInfo = NULL
        NULL,   // * endKeyArray = NULL
        FALSE,  // genPackedDesc = FALSE
        NULL,   // packedDescLen = NULL
        TRUE    // isSharedSchema = FALSE
    );
  else if (isCW)
    table_desc = Generator::createVirtualTableDesc(
        name, heap, ComTdbHbaseAccess::getVirtTableNumCols(), ComTdbHbaseAccess::getVirtTableColumnInfo(),
        ComTdbHbaseAccess::getVirtTableNumKeys(), ComTdbHbaseAccess::getVirtTableKeyInfo(),
        0,      // numConstrs = 0
        NULL,   // constrInfo = NULL
        0,      // Int32 numIndexes = 0
        NULL,   // *indexInfo = NULL
        0,      // numViews = 0
        NULL,   // viewInfo = NULL
        NULL,   // tableInfo = NULL
        NULL,   // seqInfo = NULL
        NULL,   // * statsInfo = NULL
        NULL,   // * endKeyArray = NULL
        FALSE,  // genPackedDesc = FALSE
        NULL,   // packedDescLen = NULL
        TRUE    // isSharedSchema = FALSE
    );

  if (table_desc) {
    struct TrafDesc *head = Generator::assembleDescs(beginKeys, heap);

    table_desc->tableDesc()->hbase_regionkey_desc = head;

    Lng32 v1 = (Lng32)CmpCommon::getDefaultNumeric(HBASE_MAX_COLUMN_NAME_LENGTH);
    Lng32 v2 = (Lng32)CmpCommon::getDefaultNumeric(HBASE_MAX_COLUMN_VAL_LENGTH);
    Lng32 v3 = (Lng32)CmpCommon::getDefaultNumeric(HBASE_MAX_COLUMN_INFO_LENGTH);

    TrafDesc *cols_desc = table_desc->tableDesc()->columns_desc;
    for (Lng32 i = 0; i < table_desc->tableDesc()->colcount; i++) {
      if (isRW) {
        if (i == HBASE_ROW_ROWID_INDEX)
          cols_desc->columnsDesc()->length = v1;
        else if (i == HBASE_COL_DETAILS_INDEX)
          cols_desc->columnsDesc()->length = v3;
      } else {
        if ((i == HBASE_ROW_ID_INDEX) || (i == HBASE_COL_NAME_INDEX) || (i == HBASE_COL_FAMILY_INDEX))
          cols_desc->columnsDesc()->length = v1;
        else if (i == HBASE_COL_VALUE_INDEX)
          cols_desc->columnsDesc()->length = v2;
      }
      cols_desc = cols_desc->next;

    }  // for
  }

  return table_desc;
}

TrafDesc *HbaseAccess::createVirtualTableDesc(const char *name, NAList<char *> &colNameList,
                                              NAList<char *> &colValList) {
  TrafDesc *table_desc = NULL;

  Lng32 arrSize = colNameList.entries();
  ComTdbVirtTableColumnInfo *colInfoArray =
      (ComTdbVirtTableColumnInfo *)new char[arrSize * sizeof(ComTdbVirtTableColumnInfo)];

  // to be safe, allocate an array as big as the column array
  ComTdbVirtTableKeyInfo *keyInfoArray = (ComTdbVirtTableKeyInfo *)new char[arrSize * sizeof(ComTdbVirtTableKeyInfo)];

  // colList contains 3 column families:
  //  obj_type, col_details and key_details
  Lng32 numCols = 0;
  Lng32 numKeys = 0;
  Lng32 i = 0;
  char colFamily[100];
  for (i = 0; i < arrSize; i++) {
    char *colName = colNameList[i];
    char *val = colValList[i];

    // look for ":" and find the column family
    Lng32 cp = 0;
    while ((cp < strlen(colName)) && (colName[cp] != ':')) cp++;

    if (cp < strlen(colName)) {
      str_cpy_and_null(colFamily, colName, cp, '\0', ' ', TRUE);
    } else {
      // must have a column family
      return NULL;
    }

    if (strcmp(colFamily, "col_details") == 0) {
      colInfoArray[numCols].colName = &colName[cp + 1];

      // val format: TTTT:LLLL:PPPP:SS:DS:DE:KK:NN
      //                   type:length:precision:scale:datestart:dateend:primaryKey:nullable

      char buf[100];
      Lng32 curPos = 0;
      Lng32 type = (Lng32)str_atoi(&val[curPos], 4);
      curPos += 4;
      curPos++;  // skip the ":"

      Lng32 len = (Lng32)str_atoi(&val[curPos], 4);
      curPos += 4;
      curPos++;  // skip the ":"

      Lng32 precision = (Lng32)str_atoi(&val[curPos], 4);
      curPos += 4;
      curPos++;  // skip the ":"

      Lng32 scale = (Lng32)str_atoi(&val[curPos], 2);
      curPos += 2;
      curPos++;  // skip the ":"

      Lng32 dtStart = (Lng32)str_atoi(&val[curPos], 2);
      curPos += 2;
      curPos++;  // skip the ":"

      Lng32 dtEnd = (Lng32)str_atoi(&val[curPos], 2);
      curPos += 2;
      curPos++;  // skip the ":"

      Lng32 pKey = (Lng32)str_atoi(&val[curPos], 2);
      curPos += 2;
      curPos++;  // skip the ":"

      Lng32 nullHeaderSize = (Lng32)str_atoi(&val[curPos], 2);

      colInfoArray[numCols].datatype = type;
      colInfoArray[numCols].length = len;
      colInfoArray[numCols].nullable = (nullHeaderSize > 0 ? 1 : 0);
      colInfoArray[numCols].charset = SQLCHARSETCODE_UNKNOWN;

      colInfoArray[numCols].precision = precision;
      colInfoArray[numCols].scale = scale;
      colInfoArray[numCols].dtStart = dtStart;
      colInfoArray[numCols].dtEnd = dtEnd;

      numCols++;
    } else if (strcmp(colFamily, "key_details") == 0) {
      Lng32 keySeq = (Lng32)str_atoi(&val[0], 4);
      Lng32 colNum = (Lng32)str_atoi(&val[4 + 1], 4);

      keyInfoArray[numKeys].colName = NULL;
      keyInfoArray[numKeys].keySeqNum = keySeq;
      keyInfoArray[numKeys].tableColNum = colNum;
      keyInfoArray[numKeys].ordering = 0;

      numKeys++;
    } else if (strcmp(colFamily, "obj_type") == 0) {
      char *val = colValList[i];
      if (strcmp(val, "BT") != 0) {
        // must be a base table
        return NULL;
      }
    }
  }

  table_desc = Generator::createVirtualTableDesc(name,
                                                 NULL,           // let it decide what heap to use
                                                 numCols,        // ComTdbHbaseAccess::getVirtTableNumCols(),
                                                 colInfoArray,   // ComTdbHbaseAccess::getVirtTableColumnInfo(),
                                                 numKeys,        // ComTdbHbaseAccess::getVirtTableNumKeys(),
                                                 keyInfoArray);  // ComTdbHbaseAccess::getVirtTableKeyInfo());

  return table_desc;
}

short HbaseAccess::genRowIdExpr(Generator *generator, const NAColumnArray &keyColumns,
                                NAList<HbaseSearchKey *> &searchKeys, ex_cri_desc *work_cri_desc, const Int32 work_atp,
                                const Int32 rowIdAsciiTuppIndex, const Int32 rowIdTuppIndex, ULng32 &rowIdAsciiRowLen,
                                ExpTupleDesc *&rowIdAsciiTupleDesc, UInt32 &rowIdLength, ex_expr *&rowIdExpr,
                                NABoolean encodeKeys) {
  Space *space = generator->getSpace();
  ExpGenerator *expGen = generator->getExpGenerator();

  rowIdAsciiRowLen = 0;
  rowIdAsciiTupleDesc = 0;
  rowIdLength = 0;
  rowIdExpr = NULL;

  ValueIdList rowIdAsciiVids;
  if (searchKeys.entries() > 0) {
    ValueIdList keyConvVIDList;

    HbaseSearchKey *searchKey = searchKeys[0];

    for (CollIndex i = 0; i < searchKey->getKeyColumns().entries(); i++) {
      ValueId vid = searchKey->getKeyColumns()[i];
      const NAType &givenType = vid.getType();

      int res;
      ItemExpr *castVal = NULL;
      ItemExpr *asciiVal = NULL;
      res = createAsciiColAndCastExpr(generator, givenType, asciiVal, castVal);
      GenAssert(res == 1 && asciiVal != NULL && castVal != NULL,
                "Error building expression tree for cast output value");

      asciiVal->bindNode(generator->getBindWA());
      rowIdAsciiVids.insert(asciiVal->getValueId());

      ItemExpr *ie = castVal;
      if ((givenType.getVarLenHdrSize() > 0) && (encodeKeys)) {
        // Explode varchars by moving them to a fixed field
        // whose length is equal to the max length of varchar.
        // handle different character set cases.
        const CharType &char_t = (CharType &)givenType;

        ie = new (generator->wHeap())
            Cast(ie, (new (generator->wHeap()) SQLChar(
                         generator->wHeap(), CharLenInfo(char_t.getStrCharLimit(), char_t.getDataStorageSize()),
                         givenType.supportsSQLnull(), FALSE, FALSE, FALSE, char_t.getCharSet(), char_t.getCollation(),
                         char_t.getCoercibility())));

        if (char_t.isVarchar2()) ((Cast *)ie)->setPadUseZero(TRUE);
      }

      NABoolean descFlag = TRUE;
      if (keyColumns.isAscending(i)) descFlag = FALSE;
      if (encodeKeys) {
        ie = new (generator->wHeap()) CompEncode(ie, descFlag);
      }

      ie->bindNode(generator->getBindWA());
      keyConvVIDList.insert(ie->getValueId());
    }  // for

    expGen->processValIdList(rowIdAsciiVids,  // [IN] ValueIdList
                             ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                             rowIdAsciiRowLen,            // [OUT] tuple length
                             work_atp,                    // [IN] atp number
                             rowIdAsciiTuppIndex,         // [IN] index into atp
                             &rowIdAsciiTupleDesc,        // [optional OUT] tuple desc
                             ExpTupleDesc::LONG_FORMAT);  // [optional IN] desc format

    work_cri_desc->setTupleDescriptor(rowIdAsciiTuppIndex, rowIdAsciiTupleDesc);

    expGen->generateContiguousMoveExpr(keyConvVIDList, 0 /*no conv nodes*/, work_atp, rowIdTuppIndex,
                                       ExpTupleDesc::SQLARK_EXPLODED_FORMAT, rowIdLength, &rowIdExpr);
  }  // if

  return 0;
}

short HbaseAccess::genRowIdExprForNonSQ(Generator *generator, const NAColumnArray &keyColumns,
                                        NAList<HbaseSearchKey *> &searchKeys, ex_cri_desc *work_cri_desc,
                                        const Int32 work_atp, const Int32 rowIdAsciiTuppIndex,
                                        const Int32 rowIdTuppIndex, ULng32 &rowIdAsciiRowLen,
                                        ExpTupleDesc *&rowIdAsciiTupleDesc, UInt32 &rowIdLength, ex_expr *&rowIdExpr) {
  Space *space = generator->getSpace();
  ExpGenerator *expGen = generator->getExpGenerator();

  rowIdAsciiRowLen = 0;
  rowIdAsciiTupleDesc = 0;
  rowIdLength = 0;
  rowIdExpr = NULL;

  ValueIdList rowIdAsciiVids;
  if (searchKeys.entries() > 0) {
    ValueIdList keyConvVIDList;

    HbaseSearchKey *searchKey = searchKeys[0];

    for (CollIndex i = 0; i < searchKey->getKeyColumns().entries(); i++) {
      ValueId vid = searchKey->getKeyColumns()[i];
      const NAType &givenType = vid.getType();

      int res;
      ItemExpr *castVal = NULL;
      ItemExpr *asciiVal = NULL;
      res = createAsciiColAndCastExpr(generator, givenType, asciiVal, castVal);
      GenAssert(res == 1 && asciiVal != NULL && castVal != NULL,
                "Error building expression tree for cast output value");

      asciiVal->bindNode(generator->getBindWA());
      rowIdAsciiVids.insert(asciiVal->getValueId());

      ItemExpr *ie = castVal;
      const CharType &char_t = (CharType &)givenType;
      ie = new (generator->wHeap()) Cast(
          ie, (new (generator->wHeap()) ANSIChar(
                  generator->wHeap(), char_t.getDataStorageSize(), givenType.supportsSQLnull(), FALSE, FALSE,
                  char_t.getCharSet(), char_t.getCollation(), char_t.getCoercibility(), CharInfo::UnknownCharSet)));

      ie->bindNode(generator->getBindWA());
      keyConvVIDList.insert(ie->getValueId());
    }  // for

    expGen->processValIdList(rowIdAsciiVids,  // [IN] ValueIdList
                             ExpTupleDesc::SQLARK_EXPLODED_FORMAT,
                             rowIdAsciiRowLen,            // [OUT] tuple length
                             work_atp,                    // [IN] atp number
                             rowIdAsciiTuppIndex,         // [IN] index into atp
                             &rowIdAsciiTupleDesc,        // [optional OUT] tuple desc
                             ExpTupleDesc::LONG_FORMAT);  // [optional IN] desc format

    work_cri_desc->setTupleDescriptor(rowIdAsciiTuppIndex, rowIdAsciiTupleDesc);

    expGen->generateContiguousMoveExpr(keyConvVIDList, 0 /*no conv nodes*/, work_atp, rowIdTuppIndex,
                                       ExpTupleDesc::SQLARK_EXPLODED_FORMAT, rowIdLength, &rowIdExpr);
  }  // if

  return 0;
}

short HbaseAccess::genListsOfRows(Generator *generator, NAList<HbaseRangeRows> &listOfRangeRows,
                                  NAList<HbaseUniqueRows> &listOfUniqueRows, Queue *&tdbListOfRangeRows,
                                  Queue *&tdbListOfUniqueRows) {
  Space *space = generator->getSpace();
  ExpGenerator *expGen = generator->getExpGenerator();

  if (listOfRangeRows.entries() > 0) {
    tdbListOfRangeRows = new (space) Queue(space);

    for (Lng32 i = 0; i < listOfRangeRows.entries(); i++) {
      HbaseRangeRows &hs = listOfRangeRows[i];

      Queue *colNamesList = new (space) Queue(space);

      char *beginRowIdInList = NULL;
      if (hs.beginRowId_.length() == 0) {
        beginRowIdInList = space->allocateAlignedSpace(sizeof(short));
        *(short *)beginRowIdInList = 0;
      } else
        beginRowIdInList = space->AllocateAndCopyToAlignedSpace(hs.beginRowId_, 0);

      char *endRowIdInList = NULL;
      if (hs.endRowId_.length() == 0) {
        endRowIdInList = space->allocateAlignedSpace(sizeof(short));
        *(short *)endRowIdInList = 0;
      } else
        endRowIdInList = space->AllocateAndCopyToAlignedSpace(hs.endRowId_, 0);

      for (Lng32 j = 0; j < hs.colNames_.entries(); j++) {
        NAString colName = "cf1:";
        colName += hs.colNames_[j];

        char *colNameInList = space->AllocateAndCopyToAlignedSpace(colName, 0);

        colNamesList->insert(colNameInList);
      }

      ComTdbHbaseAccess::HbaseScanRows *hsr = new (space) ComTdbHbaseAccess::HbaseScanRows();
      hsr->beginRowId_ = beginRowIdInList;
      hsr->endRowId_ = endRowIdInList;
      hsr->beginKeyExclusive_ = (hs.beginKeyExclusive_ ? 1 : 0);
      hsr->endKeyExclusive_ = (hs.endKeyExclusive_ ? 1 : 0);
      hsr->colNames_ = colNamesList;
      hsr->colTS_ = -1;

      tdbListOfRangeRows->insert(hsr);
    }  // for
  }

  if (listOfUniqueRows.entries() > 0) {
    tdbListOfUniqueRows = new (space) Queue(space);

    for (Lng32 i = 0; i < listOfUniqueRows.entries(); i++) {
      HbaseUniqueRows &hg = listOfUniqueRows[i];

      Queue *rowIdsList = new (space) Queue(space);
      Queue *colNamesList = new (space) Queue(space);

      for (Lng32 j = 0; j < hg.rowIds_.entries(); j++) {
        NAString &rowId = hg.rowIds_[j];

        char *rowIdInList = space->AllocateAndCopyToAlignedSpace(rowId, 0);

        rowIdsList->insert(rowIdInList);
      }

      for (Lng32 j = 0; j < hg.colNames_.entries(); j++) {
        NAString colName = "cf1:";
        colName += hg.colNames_[j];

        char *colNameInList = space->AllocateAndCopyToAlignedSpace(colName, 0);

        colNamesList->insert(colNameInList);
      }

      ComTdbHbaseAccess::HbaseGetRows *hgr = new (space) ComTdbHbaseAccess::HbaseGetRows();
      hgr->rowIds_ = rowIdsList;
      hgr->colNames_ = colNamesList;
      hgr->colTS_ = -1;

      tdbListOfUniqueRows->insert(hgr);
    }  // for
  }

  return 0;
}

short HbaseAccess::genColName(Generator *generator, ItemExpr *col_node, char *&colNameInList) {
  Space *space = generator->getSpace();

  NAString cnInList;
  colNameInList = NULL;
  if (!col_node) {
    createHbaseColId(NULL, cnInList);

    colNameInList = space->AllocateAndCopyToAlignedSpace(cnInList, 0);
  } else if (col_node->getOperatorType() == ITM_BASECOLUMN) {
    const NAColumn *nac = ((BaseColumn *)col_node)->getNAColumn();

    createHbaseColId(nac, cnInList);

    colNameInList = space->AllocateAndCopyToAlignedSpace(cnInList, 0);
  } else if (col_node->getOperatorType() == ITM_INDEXCOLUMN) {
    NAColumn *nac = ((IndexColumn *)col_node)->getNAColumn();

    createHbaseColId(nac, cnInList);

    colNameInList = space->AllocateAndCopyToAlignedSpace(cnInList, 0);
  } else if (col_node->getOperatorType() == ITM_CONSTANT) {
    ConstValue *cv = (ConstValue *)col_node;

    char *colVal = (char *)cv->getConstValue();
    short len = cv->getStorageSize();

    cnInList.append((char *)&len, sizeof(short));
    cnInList.append(colVal, len);

    colNameInList = space->AllocateAndCopyToAlignedSpace(cnInList, 0);
  } else if (col_node->getOperatorType() == ITM_REFERENCE) {
    GenAssert(0, "Should not have ColReference");
  }

  return 0;
}

short HbaseAccess::genListOfColNames(Generator *generator, const IndexDesc *indexDesc, ValueIdList &columnList,
                                     Queue *&listOfColNames) {
  Space *space = generator->getSpace();
  ExpGenerator *expGen = generator->getExpGenerator();

  const CollIndex numColumns = columnList.entries();

  listOfColNames = new (space) Queue(space);

  for (CollIndex c = 0; c < numColumns; c++) {
    ItemExpr *col_node = ((columnList[c]).getValueDesc())->getItemExpr();

    char *colNameInList = NULL;

    genColName(generator, col_node, colNameInList);

    listOfColNames->insert(colNameInList);
  }

  return 0;
}

short HbaseAccess::convNumToId(const char *colQualPtr, Lng32 colQualLen, NAString &cid) {
  Int64 colQval = str_atoi(colQualPtr, colQualLen);

  if (colQval <= UCHAR_MAX) {
    unsigned char c = (unsigned char)colQval;
    cid.append((char *)&c, 1);
  } else if (colQval <= USHRT_MAX) {
    unsigned short s = (unsigned short)colQval;
    cid.append((char *)&s, 2);
  } else if (colQval <= ULONG_MAX) {
    Lng32 l = (Lng32)colQval;
    cid.append((char *)&l, 4);
  } else
    cid.append((char *)&colQval, 8);

  return 0;
}

// colIdformat:
//     2 bytes len, len bytes colname.
//
// colname format:
//       <colfam>:<colQual>  (for base table access)
//       <colfam>:@<colQual>   (for index access)
//
//                colQual is 1,2,4 bytes
//
short HbaseAccess::createHbaseColId(const NAColumn *nac, NAString &cid, NABoolean isSecondaryIndex,
                                    NABoolean noLenPrefix) {
  if (nac)
    cid = nac->getHbaseColFam();
  else
    cid = SEABASE_DEFAULT_COL_FAMILY;
  cid += ":";

  if (nac && nac->getNATable() && nac->getNATable()->isHbaseMapTable()) {
    char *colQualPtr = (char *)nac->getColName().data();
    Lng32 colQualLen = nac->getHbaseColQual().length();

    cid.append(colQualPtr, colQualLen);
  } else if (nac) {
    char *colQualPtr = (char *)nac->getHbaseColQual().data();
    Lng32 colQualLen = nac->getHbaseColQual().length();
    if (colQualPtr[0] == '@') {
      cid += "@";
      colQualLen--;
      colQualPtr++;
    } else if (isSecondaryIndex)
      cid += "@";

    convNumToId(colQualPtr, colQualLen, cid);
  }

  if (NOT noLenPrefix) {
    short len = cid.length();
    cid.prepend((char *)&len, sizeof(short));
  }

  return 0;
}

NABoolean HbaseAccess::isSnapshotScanFeasible(LatestSnpSupportEnum snpSupport, char *tabName) {
  if (snpType_ == SNP_NONE) return FALSE;

  if (snpType_ != SNP_NONE &&
      (!getTableDesc()->getNATable()->isSeabaseTable() || getTableDesc()->getNATable()->isSeabaseMDTable() ||
       (getTableDesc()->getNATable()->getTableName().getObjectName() == HBASE_HIST_NAME) ||
       (getTableDesc()->getNATable()->getTableName().getObjectName() == HBASE_HISTINT_NAME))) {
    snpType_ = SNP_NONE;
  } else if (snpType_ == SNP_LATEST) {
    if ((snpSupport == latest_snp_index_table) || (snpSupport == latest_snp_no_snapshot_available) ||
        (snpSupport == latest_snp_small_table) || (snpSupport == latest_snp_not_trafodion_table)) {
      // revert to regular scan
      snpType_ = SNP_NONE;
      char msg[200];
      if (snpSupport == latest_snp_index_table) {
        sprintf(msg, "%s", "snapshot scan is not supported with index tables yet");
      } else if (snpSupport == latest_snp_no_snapshot_available) {
        sprintf(msg, "%s", "there are no snapshots associated with it");
      } else if (snpSupport == latest_snp_small_table) {
        sprintf(msg, "%s %d MBs", "its estimated size is less than the threshold of",
                getDefault(TRAF_TABLE_SNAPSHOT_SCAN_TABLE_SIZE_THRESHOLD));
      } else if (snpSupport == latest_snp_not_trafodion_table) {
        sprintf(msg, "%s", "it is not a Trafodion table");
      }
      // issue warning
      *CmpCommon::diags() << DgSqlCode(4372) << DgString0(tabName) << DgString1(msg);
    }
  }
  return (snpType_ != SNP_NONE);
}
short HbaseAccess::createSortValue(ItemExpr *col_node, std::vector<SortValue> &myvector, NABoolean isSecondaryIndex) {
  if (col_node->getOperatorType() == ITM_BASECOLUMN) {
    NAColumn *nac = ((BaseColumn *)col_node)->getNAColumn();
    NAString cn;
    createHbaseColId(nac, cn);

    SortValue sv;
    sv.str_ = cn;
    sv.vid_ = col_node->getValueId();
    myvector.push_back(sv);
  } else if (col_node->getOperatorType() == ITM_INDEXCOLUMN) {
    IndexColumn *idxCol = (IndexColumn *)col_node;

    NAColumn *nac = idxCol->getNAColumn();

    NAString cn;

    createHbaseColId(nac, cn, isSecondaryIndex);

    SortValue sv;
    sv.str_ = cn;
    sv.vid_ = col_node->getValueId();
    myvector.push_back(sv);
  } else if (col_node->getOperatorType() == ITM_REFERENCE) {
    GenAssert(0, "Should not have ColReference");
  }

  return 0;
}

short HbaseAccess::returnDups(std::vector<HbaseAccess::SortValue> &myvector, ValueIdList &srcVIDlist,
                              ValueIdList &dupVIDlist) {
  size_t startPos = 0;
  size_t currPos = 0;
  HbaseAccess::SortValue startVal = myvector[startPos];

  while (currPos <= myvector.size()) {
    NABoolean greater = FALSE;
    if (currPos == myvector.size())
      greater = TRUE;
    else {
      HbaseAccess::SortValue currVal = myvector[currPos];
      if (currVal < startVal) return -1;  // error, must be sorted

      if (NOT(currVal == startVal))  // currVal > startVal
        greater = TRUE;
    }

    if (greater) {
      if ((currPos - startPos) > 1) {
        for (size_t j = startPos + 1; j < currPos; j++) {
          srcVIDlist.insert(myvector[startPos].vid_);
          dupVIDlist.insert(myvector[j].vid_);
        }
      }

      if (currPos < myvector.size()) {
        startPos = currPos;
        startVal = myvector[startPos];
      }
    }  // currVal > startVal

    currPos++;
  }

  return 0;
}

short HbaseAccess::sortValues(const ValueIdList &inList, ValueIdList &sortedList, NABoolean isSecondaryIndex) {
  std::vector<SortValue> myvector;

  for (CollIndex i = 0; i < inList.entries(); i++) {
    ItemExpr *col_node = inList[i].getValueDesc()->getItemExpr();

    createSortValue(col_node, myvector, isSecondaryIndex);
  }

  // using object as comp
  std::sort(myvector.begin(), myvector.end());

  myvector.erase(unique(myvector.begin(), myvector.end()), myvector.end());

  for (size_t ii = 0; ii < myvector.size(); ii++) {
    sortedList.insert(myvector[ii].vid_);
  }

  return 0;
}

short HbaseAccess::sortValues(const ValueIdSet &inSet, ValueIdList &uniqueSortedList, ValueIdList &srcVIDlist,
                              ValueIdList &dupVIDlist, NABoolean isSecondaryIndex) {
  std::vector<SortValue> myvector;

  for (ValueId vid = inSet.init(); inSet.next(vid); inSet.advance(vid)) {
    ItemExpr *col_node = vid.getValueDesc()->getItemExpr();

    createSortValue(col_node, myvector, isSecondaryIndex);
  }

  // using object as comp
  std::sort(myvector.begin(), myvector.end());

  if (isSecondaryIndex) {
    returnDups(myvector, srcVIDlist, dupVIDlist);
  }

  myvector.erase(unique(myvector.begin(), myvector.end()), myvector.end());

  for (size_t ii = 0; ii < myvector.size(); ii++) {
    uniqueSortedList.insert(myvector[ii].vid_);
  }

  return 0;
}

short HbaseAccess::sortValues(NASet<NAString> &inSet, NAList<NAString> &sortedList) {
  std::vector<NAString> myvector;

  for (Lng32 i = 0; i < inSet.entries(); i++) {
    NAString &colName = inSet[i];

    myvector.push_back(colName);
  }

  // using object as comp
  std::sort(myvector.begin(), myvector.end());

  myvector.erase(unique(myvector.begin(), myvector.end()), myvector.end());

  for (size_t ii = 0; ii < myvector.size(); ii++) {
    sortedList.insert(myvector[ii]);
  }

  return 0;
}

// infoType: 0, timestamp. 1, version. 2, security label.
static short HbaseAccess_updateHbaseInfoNode(ValueIdList &hbiVIDlist, const NAString &colName, Lng32 colIndex) {
  for (Lng32 i = 0; i < hbiVIDlist.entries(); i++) {
    ValueId &vid = hbiVIDlist[i];
    ItemExpr *ie = vid.getItemExpr();

    const ItemExpr *col_node = NULL;
    HbaseVisibility *hbtg = NULL;
    HbaseTimestamp *hbt = NULL;
    HbaseVersion *hbv = NULL;
    HbaseRowid *hbr = NULL;
    if (ie->getOperatorType() == ITM_HBASE_VISIBILITY) {
      hbtg = (HbaseVisibility *)ie;

      col_node = hbtg->col();
    } else if (ie->getOperatorType() == ITM_HBASE_TIMESTAMP) {
      hbt = (HbaseTimestamp *)ie;

      col_node = hbt->col();
    } else if (ie->getOperatorType() == ITM_HBASE_VERSION) {
      hbv = (HbaseVersion *)ie;

      col_node = hbv->col();
    } else if (ie->getOperatorType() == ITM_HBASE_ROWID) {
      hbr = (HbaseRowid *)ie;

      col_node = hbr->col();
    }

    NAColumn *nac = NULL;
    if (col_node->getOperatorType() == ITM_BASECOLUMN) {
      nac = ((BaseColumn *)col_node)->getNAColumn();
    }

    if (nac && (nac->getColName() == colName)) {
      if (ie->getOperatorType() == ITM_HBASE_VISIBILITY)
        hbtg->setColIndex(colIndex);
      else if (ie->getOperatorType() == ITM_HBASE_TIMESTAMP)
        hbt->setColIndex(colIndex);
      else if (ie->getOperatorType() == ITM_HBASE_VERSION)
        hbv->setColIndex(colIndex);
      else
        hbr->setColIndex(colIndex);
    }
  }

  return 0;
}

// asAnsiString: return name in ansi delimited format
short FileScan::genTableName(Generator *generator, ComSpace *space, const CorrName &tableName, const NATable *naTable,
                             const NAFileSet *naFileSet, const NABoolean asAnsiString, char *&gendTablename,
                             const NABoolean isForGetUIDName) {
  gendTablename = NULL;

  NAString tabNS;
  if (naTable && (NOT((NATable *)naTable)->getNamespace().isNull())) {
    tabNS = ((NATable *)naTable)->getNamespace() + NAString(":");
  }

  if (naTable && ((naTable->isHbaseRowTable()) || (naTable->isHbaseCellTable()))) {
    if (NOT tabNS.isNull()) {
      NAString tabName = tabNS + tableName.getQualifiedNameObj().getObjectName();
      gendTablename = space->AllocateAndCopyToAlignedSpace(tabName, 0);
    } else {
      gendTablename = space->AllocateAndCopyToAlignedSpace(
          GenGetQualifiedName(tableName.getQualifiedNameObj().getObjectName(), FALSE, asAnsiString), 0);
    }
  } else if (naTable && (naTable->isHbaseMapTable())) {
    const char *tblColonPos = strchr(tableName.getQualifiedNameObj().getObjectName(), ':');
    if ((NOT tabNS.isNull()) && (!tblColonPos)) {
      NAString tabName = tabNS + tableName.getQualifiedNameObj().getObjectName();
      gendTablename = space->AllocateAndCopyToAlignedSpace(tabName, 0);
    } else {
      gendTablename = space->AllocateAndCopyToAlignedSpace(
          GenGetQualifiedName(tableName.getQualifiedNameObj().getObjectName(), FALSE, asAnsiString), 0);
    }
  } else if (naFileSet) {
    if (tabNS.isNull()) {
      gendTablename = space->AllocateAndCopyToAlignedSpace(
          GenGetQualifiedName(naFileSet->getFileSetName(), FALSE, asAnsiString), 0);
    } else {
      NAString tabName = tabNS + GenGetQualifiedName(naFileSet->getFileSetName(), FALSE, asAnsiString);

      gendTablename = space->AllocateAndCopyToAlignedSpace(tabName, 0);
    }
  }

  if (naTable && isForGetUIDName) {
    if ((naTable->getObjectType() == COM_BASE_TABLE_OBJECT || naTable->getObjectType() == COM_INDEX_OBJECT) &&
        ComIsUserTable(tableName)) {
      Int64 objUID;
      if (naFileSet && naFileSet->getKeytag() != 0)
        objUID = naFileSet->getIndexUID();
      else
        objUID = naTable->objDataUID().castToInt64();
      if (tabNS.isNull()) {
        NAString tabNameforUID = tableName.getQualifiedNameObj().getCatalogName() + "__" + Int64ToNAString(objUID);
        gendTablename = space->AllocateAndCopyToAlignedSpace(tabNameforUID, 0);
      } else {
        NAString tabNameforUID =
            tabNS + tableName.getQualifiedNameObj().getCatalogName() + "__" + Int64ToNAString(objUID);
        gendTablename = space->AllocateAndCopyToAlignedSpace(tabNameforUID, 0);
      }
    }
    return 0;
  }

  if (gendTablename == NULL) {
    if (tabNS.isNull())
      gendTablename = space->AllocateAndCopyToAlignedSpace(GenGetQualifiedName(tableName, FALSE, asAnsiString), 0);
    else {
      NAString tabName = tabNS + GenGetQualifiedName(tableName, FALSE, asAnsiString);

      gendTablename = space->AllocateAndCopyToAlignedSpace(tabName, 0);
    }
  }

  return 0;
}

short HbaseAccess::codeGen(Generator *generator) {
  Space *space = generator->getSpace();
  ExpGenerator *expGen = generator->getExpGenerator();

  // allocate a map table for the retrieved columns
  //  generator->appendAtEnd();
  MapTable *last_map_table = generator->getLastMapTable();

  ex_expr *scanExpr = 0;
  ex_expr *proj_expr = 0;
  ex_expr *convert_expr = NULL;
  ex_expr *hbaseFilterValExpr = NULL;
  ex_expr *scanPrecondExpr = 0;

  ex_cri_desc *givenDesc = generator->getCriDesc(Generator::DOWN);

  ex_cri_desc *returnedDesc = NULL;

  const Int32 work_atp = 1;
  const Int32 convertTuppIndex = 2;
  const Int32 rowIdTuppIndex = 3;
  const Int32 asciiTuppIndex = 4;
  const Int32 rowIdAsciiTuppIndex = 5;
  const Int32 keyTuppIndex = 6;
  const Int32 hbaseFilterValTuppIndex = 7;
  const Int32 hbaseTimestampTuppIndex = 8;
  const Int32 hbaseVersionTuppIndex = 9;
  const Int32 hbaseTagTuppIndex = 10;
  const Int32 hbaseRowidIndex = 11;

  ULng32 asciiRowLen;
  ExpTupleDesc *asciiTupleDesc = 0;
  ExpTupleDesc *convertTupleDesc = NULL;
  ExpTupleDesc *hbaseFilterValTupleDesc = NULL;

  ULng32 hbaseFilterValRowLen = 0;

  ex_cri_desc *work_cri_desc = NULL;
  work_cri_desc = new (space) ex_cri_desc(12, space);

  returnedDesc = new (space) ex_cri_desc(givenDesc->noTuples() + 1, space);

  ExpTupleDesc::TupleDataFormat hbaseRowFormat;
  ValueIdList asciiVids;
  ValueIdList executorPredCastVids;
  ValueIdList convertExprCastVids;

  NABoolean addDefaultValues = TRUE;
  NABoolean hasAddedColumns = FALSE;
  if (getTableDesc()->getNATable()->hasAddedColumn()) hasAddedColumns = TRUE;

  NABoolean isSecondaryIndex = FALSE;
  if (getIndexDesc()->getNAFileSet()->getKeytag() != 0) {
    isSecondaryIndex = TRUE;
    hasAddedColumns = FALSE;  // secondary index doesn't have added cols
  }

  NABoolean isAlignedFormat = getTableDesc()->getNATable()->isAlignedFormat(getIndexDesc());
  NABoolean isHbaseMapFormat = getTableDesc()->getNATable()->isHbaseMapTable();

  // If CIF is not OFF use aligned format, except when table is
  // not aligned and it has added columns. Support for added columns
  // in not aligned tables is doable, but is turned off now due to
  // this case causing a regression failure.
  hbaseRowFormat =
      ((hasAddedColumns && !isAlignedFormat) || (CmpCommon::getDefault(COMPRESSED_INTERNAL_FORMAT) == DF_OFF))
          ? ExpTupleDesc::SQLARK_EXPLODED_FORMAT
          : ExpTupleDesc::SQLMX_ALIGNED_FORMAT;

  // build key information
  keyRangeGen *keyInfo = 0;
  NABoolean reverseScan = getReverseScan();

  expGen->buildKeyInfo(&keyInfo,  // out
                       generator, getIndexDesc()->getNAFileSet()->getIndexKeyColumns(), getIndexDesc()->getIndexKey(),
                       getBeginKeyPred(), (searchKey() && searchKey()->isUnique() ? NULL : getEndKeyPred()),
                       searchKey(), getMdamKeyPtr(), reverseScan, ExpTupleDesc::SQLMX_KEY_FORMAT);

  const ValueIdList &retColumnList = retColRefSet_;
  // Always get the index name -- it will be the base tablename for
  // primary access if it is trafodion table. We need the base table
  // name also for DDL validation.
  char *tablename = NULL;
  char *tablenameForUID = NULL;
  char *baseTableName = NULL;
  char *snapshotName = NULL;
  UInt32 maxRowSize = 0;
  double avgRowSize = 0;
  avgRowSize = getGroupAttr()->getAverageVarcharSize(retColumnList, maxRowSize);

  // this means we have not caculate avgRowSize by update stats
  if (maxRowSize == avgRowSize) avgRowSize = 0;

  if (genTableName(generator, space, getTableName(), getTableDesc()->getNATable(), getIndexDesc()->getNAFileSet(),
                   FALSE, tablename)) {
    GenAssert(0, "genTableName failed");
  }

  if (genTableName(generator, space, getTableDesc()->getCorrNameObj(), getTableDesc()->getNATable(),
                   NULL,  // so we get the base table name
                   FALSE, baseTableName)) {
    GenAssert(0, "genTableName failed");
  }

  LatestSnpSupportEnum latestSnpSupport = latest_snp_supported;
  Int32 computedHBaseRowSizeFromMetaData = getTableDesc()->getNATable()->computeHBaseRowSizeFromMetaData();

  if (CmpCommon::getDefault(TRAF_TABLE_SNAPSHOT_SCAN) != DF_NONE) {
    if ((getTableDesc()->getNATable()->isHbaseRowTable()) || (getTableDesc()->getNATable()->isHbaseCellTable()) ||
        (getTableName().getQualifiedNameObj().isHbaseMappedName()))
      latestSnpSupport = latest_snp_not_trafodion_table;
    else if (CmpCommon::getDefault(TRAF_TABLE_SNAPSHOT_SCAN) == DF_FORCE)
      latestSnpSupport = latest_snp_supported;
    else if (computedHBaseRowSizeFromMetaData * getEstRowsAccessed().getValue() <
             getDefault(TRAF_TABLE_SNAPSHOT_SCAN_TABLE_SIZE_THRESHOLD) * 1024 * 1024)
      latestSnpSupport = latest_snp_small_table;
    else {
      if (getIndexDesc() && getIndexDesc()->getNAFileSet()) {
        if (getIndexDesc()->isClusteringIndex()) {
          if (CmpCommon::getDefault(TRAF_TABLE_SNAPSHOT_SCAN) == DF_LATEST) {
            // Base table
            Lng32 retcode = HBaseClient_JNI::getLatestSnapshot(baseTableName, snapshotName, generator->wHeap());
            if (retcode != HBC_OK) GenAssert(0, "HBaseClient_JNI::getLatestSnapshot failed");
            if (snapshotName == NULL) latestSnpSupport = latest_snp_no_snapshot_available;
          }
        } else
          latestSnpSupport = latest_snp_index_table;
      }
    }
  }
  ValueIdList columnList;
  if ((getTableDesc()->getNATable()->isSeabaseTable()) && (NOT isAlignedFormat) && (NOT isHbaseMapFormat))
    sortValues(retColumnList, columnList, (getIndexDesc()->getNAFileSet()->getKeytag() != 0));
  else {
    for (Lng32 i = 0; i < getIndexDesc()->getIndexColumns().entries(); i++) {
      columnList.insert(getIndexDesc()->getIndexColumns()[i]);
    }
  }

  ValueIdList hbTagVIDlist;
  ValueIdList hbTsVIDlist;
  ValueIdList hbVersVIDlist;
  ValueIdList hbRidVIDlist;
  for (CollIndex hi = 0; hi < retColumnList.entries(); hi++) {
    ValueId vid = retColumnList[hi];

    if ((vid.getItemExpr()->getOperatorType() != ITM_HBASE_VISIBILITY) &&
        (vid.getItemExpr()->getOperatorType() != ITM_HBASE_TIMESTAMP) &&
        (vid.getItemExpr()->getOperatorType() != ITM_HBASE_VERSION) &&
        (vid.getItemExpr()->getOperatorType() != ITM_HBASE_ROWID))
      continue;

    ValueId tsValsVID;
    if (vid.getItemExpr()->getOperatorType() == ITM_HBASE_VISIBILITY) {
      hbTagVIDlist.insert(vid);
      tsValsVID = ((HbaseVisibility *)vid.getItemExpr())->tsVals()->getValueId();
    } else if (vid.getItemExpr()->getOperatorType() == ITM_HBASE_TIMESTAMP) {
      hbTsVIDlist.insert(vid);
      tsValsVID = ((HbaseTimestamp *)vid.getItemExpr())->tsVals()->getValueId();
    } else if (vid.getItemExpr()->getOperatorType() == ITM_HBASE_VERSION) {
      hbVersVIDlist.insert(vid);
      tsValsVID = ((HbaseVersion *)vid.getItemExpr())->tsVals()->getValueId();
    } else {
      hbRidVIDlist.insert(vid);
      tsValsVID = ((HbaseRowid *)vid.getItemExpr())->tsVals()->getValueId();
    }

    Attributes *attr = generator->addMapInfo(tsValsVID, 0)->getAttr();
    attr->setAtp(work_atp);
    if (vid.getItemExpr()->getOperatorType() == ITM_HBASE_VISIBILITY)
      attr->setAtpIndex(hbaseTagTuppIndex);
    else if (vid.getItemExpr()->getOperatorType() == ITM_HBASE_TIMESTAMP)
      attr->setAtpIndex(hbaseTimestampTuppIndex);
    else if (vid.getItemExpr()->getOperatorType() == ITM_HBASE_VERSION)
      attr->setAtpIndex(hbaseVersionTuppIndex);
    else  // ITM_HBASE_ROWID
      attr->setAtpIndex(hbaseRowidIndex);

    attr->setTupleFormat(ExpTupleDesc::SQLARK_EXPLODED_FORMAT);
    attr->setOffset(0);
    attr->setNullIndOffset(-1);
  }

  // contains source values corresponding to the key columns that will be used
  // to create the encoded key.
  ValueIdArray encodedKeyExprVidArr(getIndexDesc()->getIndexKey().entries());
  const CollIndex numColumns = columnList.entries();

  NABoolean longVC = FALSE;
  for (CollIndex ii = 0; ii < numColumns; ii++) {
    ItemExpr *col_node = ((columnList[ii]).getValueDesc())->getItemExpr();

    const NAType &givenType = col_node->getValueId().getType();
    int res;
    ItemExpr *asciiValue = NULL;
    ItemExpr *castValue = NULL;

    if ((isHbaseMapFormat) && (getTableDesc()->getNATable()->isHbaseDataFormatString())) {
      res = createAsciiColAndCastExpr3(generator,   // for heap
                                       givenType,   // [IN] Actual type of column
                                       asciiValue,  // [OUT] Returned expression for ascii rep.
                                       castValue    // [OUT] Returned expression for binary rep.
      );

    } else {
      res = createAsciiColAndCastExpr2(generator,  // for heap
                                       col_node,
                                       givenType,   // [IN] Actual type of HDFS column
                                       asciiValue,  // [OUT] Returned expression for ascii rep.
                                       castValue,   // [OUT] Returned expression for binary rep.
                                       isAlignedFormat);
    }

    GenAssert(res == 1 && castValue != NULL, "Error building expression tree for cast output value");
    if (asciiValue) {
      asciiValue->synthTypeAndValueId();
      asciiValue->bindNode(generator->getBindWA());
      asciiVids.insert(asciiValue->getValueId());
    }

    castValue = castValue->bindNode(generator->getBindWA());
    if ((!castValue) || (generator->getBindWA()->errStatus())) {
      GenExit();
      return -1;
    }

    convertExprCastVids.insert(castValue->getValueId());

    NAColumn *nac = NULL;
    if (col_node->getOperatorType() == ITM_BASECOLUMN) {
      nac = ((BaseColumn *)col_node)->getNAColumn();
    } else if (col_node->getOperatorType() == ITM_INDEXCOLUMN) {
      nac = ((IndexColumn *)col_node)->getNAColumn();
    }

    if (getMdamKeyPtr() && nac) {
      // find column position of the key col and add that value id to the vidlist
      Lng32 colPos = getIndexDesc()->getNAFileSet()->getIndexKeyColumns().getColumnPosition(*nac);
      if (colPos != -1) encodedKeyExprVidArr.insertAt(colPos, castValue->getValueId());
    }

    // if any hbase info functions are specified(hbase_timestamp, hbase_version),
    // then update them with index of the col.
    // At runtime, ts and version values are populated in a ts/vers array that has
    // one entry for each column. The index values updated here is used to access
    // that info at runtime.
    HbaseAccess_updateHbaseInfoNode(hbTagVIDlist, nac->getColName(), ii);
    HbaseAccess_updateHbaseInfoNode(hbTsVIDlist, nac->getColName(), ii);
    HbaseAccess_updateHbaseInfoNode(hbVersVIDlist, nac->getColName(), ii);
    HbaseAccess_updateHbaseInfoNode(hbRidVIDlist, nac->getColName(), ii);

    if ((DFS2REC::isAnyVarChar(givenType.getFSDatatype())) && (givenType.getTotalSize() > 1024)) longVC = TRUE;

  }  // for (ii = 0; ii < numCols; ii++)

  // use CIF if there are long varchars (> 1K length) and CIF has not
  // been explicitly turned off.
  if (longVC && (CmpCommon::getDefault(COMPRESSED_INTERNAL_FORMAT) != DF_OFF)) generator->setCompressedInternalFormat();

  ValueIdList encodedKeyExprVids(encodedKeyExprVidArr);

  ExpTupleDesc::TupleDataFormat asciiRowFormat =
      (isAlignedFormat ? ExpTupleDesc::SQLMX_ALIGNED_FORMAT : ExpTupleDesc::SQLARK_EXPLODED_FORMAT);

  // Add ascii columns to the MapTable. After this call the MapTable
  // has ascii values in the work ATP at index asciiTuppIndex.
  const NAColumnArray *colArray = NULL;
  unsigned short pcm = expGen->getPCodeMode();
  if ((asciiRowFormat == ExpTupleDesc::SQLMX_ALIGNED_FORMAT) && (hasAddedColumns)) {
    colArray = &getIndexDesc()->getAllColumns();

    // current pcode does not handle added columns in aligned format rows.
    // Generated pcode assumes that the row does not have missing columns.
    // Missing columns can only be evaluated using regular clause expressions.
    // Set this flag so both pcode and clauses are saved in generated expr.
    // At runtime, if a row has missing/added columns, the clause expression
    // is evaluated. Otherwise it is evaluated using pcode.
    // See ExHbaseAccess.cpp::missingValuesInAlignedFormatRow for details.
    expGen->setSaveClausesInExpr(TRUE);
  }

  expGen->processValIdList(asciiVids,                  // [IN] ValueIdList
                           asciiRowFormat,             // [IN] tuple data format
                           asciiRowLen,                // [OUT] tuple length
                           work_atp,                   // [IN] atp number
                           asciiTuppIndex,             // [IN] index into atp
                           &asciiTupleDesc,            // [optional OUT] tuple desc
                           ExpTupleDesc::LONG_FORMAT,  // [optional IN] desc format
                           0, NULL, (NAColumnArray *)colArray, isSecondaryIndex);

  work_cri_desc->setTupleDescriptor(asciiTuppIndex, asciiTupleDesc);

  // add hbase info nodes to convert list.
  convertExprCastVids.insert(hbTagVIDlist);
  convertExprCastVids.insert(hbTsVIDlist);
  convertExprCastVids.insert(hbVersVIDlist);
  convertExprCastVids.insert(hbRidVIDlist);
  for (CollIndex i = 0; i < columnList.entries(); i++) {
    ValueId colValId = columnList[i];
    generator->addMapInfo(colValId, 0)->getAttr();
  }  // for

  ExpTupleDesc *tuple_desc = 0;
  ExpTupleDesc *hdfs_desc = 0;
  ULng32 executorPredColsRecLength;
  ULng32 convertRowLen = 0;
  ExpHdrInfo hdrInfo;

  expGen->generateContiguousMoveExpr(convertExprCastVids,        // [IN] source ValueIds
                                     FALSE,                      // [IN] add convert nodes?
                                     work_atp,                   // [IN] target atp number
                                     convertTuppIndex,           // [IN] target tupp index
                                     hbaseRowFormat,             // [IN] target tuple format
                                     convertRowLen,              // [OUT] target tuple length
                                     &convert_expr,              // [OUT] move expression
                                     &convertTupleDesc,          // [optional OUT] target tuple desc
                                     ExpTupleDesc::LONG_FORMAT,  // [optional IN] target desc format
                                     NULL, NULL, 0, NULL, FALSE, NULL, FALSE /* doBulkMove */);

  if ((asciiRowFormat == ExpTupleDesc::SQLMX_ALIGNED_FORMAT) && (hasAddedColumns)) {
    expGen->setPCodeMode(pcm);
    expGen->setSaveClausesInExpr(FALSE);
  }

  work_cri_desc->setTupleDescriptor(convertTuppIndex, convertTupleDesc);

  for (CollIndex i = 0; i < columnList.entries(); i++) {
    ValueId colValId = columnList[i];
    ValueId castValId = convertExprCastVids[i];

    Attributes *colAttr = (generator->getMapInfo(colValId))->getAttr();
    Attributes *castAttr = (generator->getMapInfo(castValId))->getAttr();

    colAttr->copyLocationAttrs(castAttr);

  }  // for

  for (CollIndex i = 0; i < hbTsVIDlist.entries(); i++) {
    ValueId vid = hbTsVIDlist[i];

    generator->getMapInfo(vid)->codeGenerated();
  }  // for

  for (CollIndex i = 0; i < hbTagVIDlist.entries(); i++) {
    ValueId vid = hbTagVIDlist[i];

    generator->getMapInfo(vid)->codeGenerated();
  }  // for

  for (CollIndex i = 0; i < hbVersVIDlist.entries(); i++) {
    ValueId vid = hbVersVIDlist[i];

    generator->getMapInfo(vid)->codeGenerated();
  }  // for

  for (CollIndex i = 0; i < hbRidVIDlist.entries(); i++) {
    ValueId vid = hbRidVIDlist[i];

    generator->getMapInfo(vid)->codeGenerated();
  }  // for

  if (addDefaultValues) {
    expGen->addDefaultValues(columnList, getIndexDesc()->getAllColumns(), convertTupleDesc, TRUE);

    if (asciiRowFormat == ExpTupleDesc::SQLMX_ALIGNED_FORMAT) {
      expGen->addDefaultValues(columnList, getIndexDesc()->getAllColumns(), asciiTupleDesc, TRUE);
    } else {
      // copy default values from convertTupleDesc to asciiTupleDesc
      expGen->copyDefaultValues(asciiTupleDesc, convertTupleDesc);
    }
  }

  ex_expr *encodedKeyExpr = NULL;
  ULng32 encodedKeyLen = 0;
  if (getMdamKeyPtr()) {
    ULng32 firstKeyColumnOffset = 0;
    expGen->generateKeyEncodeExpr(getIndexDesc(),
                                  work_atp,  // (IN) Destination Atp
                                  keyTuppIndex, ExpTupleDesc::SQLMX_KEY_FORMAT, encodedKeyLen, &encodedKeyExpr, FALSE,
                                  firstKeyColumnOffset, &encodedKeyExprVids);
  }

  Queue *listOfFetchedColNames = NULL;
  char *pkeyColName = NULL;
  if ((getTableDesc()->getNATable()->isSeabaseTable()) && (isAlignedFormat)) {
    listOfFetchedColNames = new (space) Queue(space);

    NAString cnInList;
    if (isSecondaryIndex) {
      cnInList += SEABASE_DEFAULT_COL_FAMILY;
    } else {
      cnInList += getTableDesc()->getNATable()->defaultTrafColFam();
    }
    cnInList += ":";
    unsigned char c = 1;
    cnInList.append((char *)&c, 1);
    short len = cnInList.length();
    cnInList.prepend((char *)&len, sizeof(short));

    char *colNameInList = space->AllocateAndCopyToAlignedSpace(cnInList, 0);

    listOfFetchedColNames->insert(colNameInList);
  } else if ((getTableDesc()->getNATable()->isSeabaseTable()) && (NOT isHbaseMapFormat)) {
    listOfFetchedColNames = new (space) Queue(space);

    for (CollIndex c = 0; c < numColumns; c++) {
      ItemExpr *col_node = ((columnList[c]).getValueDesc())->getItemExpr();

      NAString cnInList;
      char *colNameInList = NULL;
      if (col_node->getOperatorType() == ITM_BASECOLUMN) {
        const NAColumn *nac = ((BaseColumn *)col_node)->getNAColumn();

        HbaseAccess::createHbaseColId(nac, cnInList);

        colNameInList = space->AllocateAndCopyToAlignedSpace(cnInList, 0);
      } else if (col_node->getOperatorType() == ITM_INDEXCOLUMN) {
        const NAColumn *nac = ((IndexColumn *)col_node)->getNAColumn();

        HbaseAccess::createHbaseColId(nac, cnInList, (getIndexDesc()->getNAFileSet()->getKeytag() != 0));

        colNameInList = space->AllocateAndCopyToAlignedSpace(cnInList, 0);
      } else if (col_node->getOperatorType() == ITM_REFERENCE) {
        GenAssert(0, "HbaseAccess::codeGen. Should not reach here.");
      }

      listOfFetchedColNames->insert(colNameInList);
    }
  } else if ((getTableDesc()->getNATable()->isSeabaseTable()) && (isHbaseMapFormat)) {
    listOfFetchedColNames = new (space) Queue(space);

    for (CollIndex c = 0; c < numColumns; c++) {
      ItemExpr *col_node = ((columnList[c]).getValueDesc())->getItemExpr();

      NAString cnInList;
      char *colNameInList = NULL;
      if ((col_node->getOperatorType() == ITM_BASECOLUMN) || (col_node->getOperatorType() == ITM_INDEXCOLUMN)) {
        const NAColumn *nac = NULL;
        if (col_node->getOperatorType() == ITM_BASECOLUMN)
          nac = ((BaseColumn *)col_node)->getNAColumn();
        else
          nac = ((IndexColumn *)col_node)->getNAColumn();

        cnInList = nac->getHbaseColFam();
        cnInList += ":";
        cnInList += nac->getColName();

        short len = cnInList.length();
        cnInList.prepend((char *)&len, sizeof(short));
        colNameInList = space->AllocateAndCopyToAlignedSpace(cnInList, 0);

        if ((nac->isPrimaryKey()) && (getTableDesc()->getNATable()->isHbaseDataFormatString()))
          pkeyColName = colNameInList;
      } else if (col_node->getOperatorType() == ITM_REFERENCE) {
        GenAssert(0, "HbaseAccess::codeGen. Should not reach here.");
      }

      listOfFetchedColNames->insert(colNameInList);
    }
  } else if ((getTableDesc()->getNATable()->isHbaseRowTable()) && (retHbaseColRefSet_.entries() > 0)) {
    listOfFetchedColNames = new (space) Queue(space);

    NAList<NAString> sortedColList(generator->wHeap());
    sortValues(retHbaseColRefSet_, sortedColList);

    for (Lng32 ij = 0; ij < sortedColList.entries(); ij++) {
      NAString colName = sortedColList[ij];
      char *colNameInList = NULL;

      short len = colName.length();
      colName.prepend((char *)&len, sizeof(short));

      colNameInList = space->AllocateAndCopyToAlignedSpace(colName, 0);

      listOfFetchedColNames->insert(colNameInList);
    }
  }

  if (getTableDesc()->getNATable()->isPartitionEntityTable() && getTableDesc()->getPreconditions().entries() > 0) {
    ValueIdList &precond = getTableDesc()->getPreconditions();
    ItemExpr *scanPreCondTree = precond.rebuildExprTree(ITM_AND, TRUE, TRUE);
    expGen->generateExpr(scanPreCondTree->getValueId(), ex_expr::exp_SCAN_PRED, &scanPrecondExpr);
  }
  // generate explain selection expression, if present
  if (!executorPred().isEmpty()) {
    // fix mantis 13737
    // for constant executor predicator, eg 'a' = ?
    // evaluate it before physical scan/get
    if (accessType() == SELECT_) {
      for (ValueId vid = executorPred().init(); executorPred().next(vid); executorPred().advance(vid)) {
        NABoolean addToScanPrecond = false;
        ItemExpr *exePre = vid.getItemExpr();
        if (exePre && exePre->isBiRelat()) {
          if (exePre->child(0)->getOperatorType() == ITM_DYN_PARAM &&
              exePre->child(1)->getOperatorType() == ITM_DYN_PARAM) {
            addToScanPrecond = true;
          } else if (exePre->child(0)->getOperatorType() == ITM_DYN_PARAM) {
            if (exePre->child(1)->constFold())  // is const
              addToScanPrecond = true;
          } else if (exePre->child(1)->getOperatorType() == ITM_DYN_PARAM) {
            if (exePre->child(0)->constFold())  // is const
              addToScanPrecond = true;
          }
        }
        if (addToScanPrecond) {
          scanPreCond().insert(vid);
          executorPred().remove(vid);
        }
      }
    }
    if (!scanPreCond().isEmpty()) {
      ItemExpr *scanPreCondTree = scanPreCond().rebuildExprTree(ITM_AND, TRUE, TRUE);
      expGen->generateExpr(scanPreCondTree->getValueId(), ex_expr::exp_SCAN_PRED, &scanPrecondExpr);
    }
    if (!executorPred().isEmpty()) {
      ItemExpr *newPredTree = executorPred().rebuildExprTree(ITM_AND, TRUE, TRUE);
      expGen->generateExpr(newPredTree->getValueId(), ex_expr::exp_SCAN_PRED, &scanExpr);
    }
  }

  if (getOptStoi() && getOptStoi()->getStoi()) generator->addSqlTableOpenInfo(getOptStoi()->getStoi());

  LateNameInfo *lateNameInfo = new (generator->wHeap()) LateNameInfo();
  char *compileTimeAnsiName = (char *)getOptStoi()->getStoi()->ansiName();

  lateNameInfo->setCompileTimeName(compileTimeAnsiName, space);
  lateNameInfo->setLastUsedName(compileTimeAnsiName, space);
  lateNameInfo->setNameSpace(COM_TABLE_NAME);
  if (getIndexDesc()->getNAFileSet()->getKeytag() != 0)
  // is an index.
  {
    lateNameInfo->setIndex(TRUE);
    lateNameInfo->setNameSpace(COM_INDEX_NAME);
  }
  generator->addLateNameInfo(lateNameInfo);

  // generate filter value expression, if present
  Queue *hbaseFilterColNames = NULL;
  Queue *hbaseCompareOpList = NULL;
  if (!hbaseFilterValueVIDlist_.isEmpty()) {
    expGen->generateContiguousMoveExpr(hbaseFilterValueVIDlist_,    // [IN] source ValueIds
                                       FALSE,                       // [IN] add convert nodes?
                                       work_atp,                    // [IN] target atp number
                                       hbaseFilterValTuppIndex,     // [IN] target tupp index
                                       asciiRowFormat,              // [IN] target tuple format
                                       hbaseFilterValRowLen,        // [OUT] target tuple length
                                       &hbaseFilterValExpr,         // [OUT] move expression
                                       &hbaseFilterValTupleDesc,    // [optional OUT] target tuple desc
                                       ExpTupleDesc::LONG_FORMAT);  // [optional IN] target desc format

    work_cri_desc->setTupleDescriptor(hbaseFilterValTuppIndex, hbaseFilterValTupleDesc);
  }
  if (!hbaseFilterColVIDlist_.isEmpty()) {  // with unary operator we can have column without value
    genListOfColNames(generator, getIndexDesc(), hbaseFilterColVIDlist_, hbaseFilterColNames);

    hbaseCompareOpList = new (generator->getSpace()) Queue(generator->getSpace());
    for (Lng32 i = 0; i < opList_.entries(); i++) {
      char *op = space->AllocateAndCopyToAlignedSpace(opList_[i], 0);
      hbaseCompareOpList->insert(op);
    }
  }

  ULng32 rowIdAsciiRowLen = 0;
  ExpTupleDesc *rowIdAsciiTupleDesc = 0;
  ex_expr *rowIdExpr = NULL;
  ULng32 rowIdLength = 0;
  Queue *tdbListOfRangeRows = NULL;
  Queue *tdbListOfUniqueRows = NULL;

  if (getTableDesc()->getNATable()->isSeabaseTable()) {
    // dont encode keys for hbase mapped tables since these tables
    // could be populated from outside of traf.
    NABoolean encodeKeys = TRUE;
    if (getTableDesc()->getNATable()->isHbaseMapTable()) encodeKeys = FALSE;

    genRowIdExpr(generator, getIndexDesc()->getNAFileSet()->getIndexKeyColumns(), getHbaseSearchKeys(), work_cri_desc,
                 work_atp, rowIdAsciiTuppIndex, rowIdTuppIndex, rowIdAsciiRowLen, rowIdAsciiTupleDesc, rowIdLength,
                 rowIdExpr, encodeKeys);
  } else {
    genRowIdExprForNonSQ(generator, getIndexDesc()->getNAFileSet()->getIndexKeyColumns(), getHbaseSearchKeys(),
                         work_cri_desc, work_atp, rowIdAsciiTuppIndex, rowIdTuppIndex, rowIdAsciiRowLen,
                         rowIdAsciiTupleDesc, rowIdLength, rowIdExpr);
  }

  genListsOfRows(generator, listOfRangeRows_, listOfUniqueRows_, tdbListOfRangeRows, tdbListOfUniqueRows);

  // The hbase row will be returned as the last entry of the returned atp.
  // Change the atp and atpindex of the returned values to indicate that.
  expGen->assignAtpAndAtpIndex(columnList, 0, returnedDesc->noTuples() - 1);

  if (NOT hbTsVIDlist.isEmpty()) expGen->assignAtpAndAtpIndex(hbTsVIDlist, 0, returnedDesc->noTuples() - 1);

  if (NOT hbTagVIDlist.isEmpty()) expGen->assignAtpAndAtpIndex(hbTagVIDlist, 0, returnedDesc->noTuples() - 1);

  if (NOT hbVersVIDlist.isEmpty()) expGen->assignAtpAndAtpIndex(hbVersVIDlist, 0, returnedDesc->noTuples() - 1);

  if (NOT hbRidVIDlist.isEmpty()) expGen->assignAtpAndAtpIndex(hbRidVIDlist, 0, returnedDesc->noTuples() - 1);

  Cardinality expectedRows = (Cardinality)getEstRowsUsed().getValue();
  ULng32 buffersize = 3 * getDefault(GEN_DPSO_BUFFER_SIZE);
  queue_index upqueuelength = (queue_index)getDefault(GEN_DPSO_SIZE_UP);
  queue_index downqueuelength = (queue_index)getDefault(GEN_DPSO_SIZE_DOWN);
  Int32 numBuffers = getDefault(GEN_DPUO_NUM_BUFFERS);

  // Compute the buffer size based on upqueue size and row size.
  // Try to get enough buffer space to hold twice as many records
  // as the up queue.
  //
  // This should be more sophisticate than this, and should maybe be done
  // within the buffer class, but for now this will do.
  //
  UInt32 FiveM = 5 * 1024 * 1024;

  // If returnedrowlen > 5M, then set upqueue entries to 2.
  UInt32 bufRowlen = MAXOF(convertRowLen, 1000);
  if (bufRowlen > FiveM) upqueuelength = 2;

  ULng32 cbuffersize = SqlBufferNeededSize((upqueuelength * 2 / numBuffers),
                                           bufRowlen);  // returnedRowlen);
  // But use at least the default buffer size.
  //
  buffersize = buffersize > cbuffersize ? buffersize : cbuffersize;

  // Cap the buffer size at 5M and adjust upqueue entries.
  // Do this only if returnrowlen is not > 5M
  if ((bufRowlen <= FiveM) && (buffersize > FiveM)) {
    buffersize = FiveM;

    upqueuelength = ((buffersize / bufRowlen) * numBuffers) / 2;
  }

  char *connectParam1;
  char *connectParam2;
  NATable::getConnectParams(getTableDesc()->getNATable()->storageType(), &connectParam1, &connectParam2);

  char *server = space->allocateAlignedSpace(strlen(connectParam1) + 1);
  strcpy(server, connectParam1);
  char *zkPort = space->allocateAlignedSpace(strlen(connectParam2) + 1);
  strcpy(zkPort, connectParam2);

  NAString snapNameNAS;
  char *snapName = NULL;
  NAString tmpLocNAS;
  char *tmpLoc = NULL;
  NAString snapScanRunIdNAS;
  char *snapScanRunId = NULL;
  NAString snapScanHelperTabNAS;
  char *snapScanHelperTab = NULL;

  ComTdbHbaseAccess::HbaseSnapshotScanAttributes *snapAttrs =
      new (space) ComTdbHbaseAccess::HbaseSnapshotScanAttributes();

  if (isSnapshotScanFeasible(latestSnpSupport, tablename)) {
    snapAttrs->setUseSnapshotScan(TRUE);
    snapAttrs->setSnapshotType(snpType_);
    if (snpType_ == SNP_LATEST) {
      CMPASSERT(snapshotName != NULL);
      snapNameNAS = snapshotName;
    } else if (snpType_ == SNP_SUFFIX) {
      // this case is mainly used with bulk unload
      snapNameNAS = tablename;
      // snapshot names cannot have : in them, typically : comes with namespace
      snapNameNAS = replaceAll(snapNameNAS, ":", "_");
      snapNameNAS.append("_");
      snapNameNAS.append(ActiveSchemaDB()->getDefaults().getValue(TRAF_TABLE_SNAPSHOT_SCAN_SNAP_SUFFIX));
    } else if (snpType_ == SNP_FORCE) {
      snapNameNAS = tablename;
      snapNameNAS = replaceAll(snapNameNAS, ":", "_");
      snapNameNAS.append("_");
      snapNameNAS.append(ActiveSchemaDB()->getDefaults().getValue(TRAF_TABLE_SNAPSHOT_SCAN_SNAP_SUFFIX));
      snapNameNAS.append("_FORCE_");

      char sqAddrStr[100];
      str_itoa((Int64)this, sqAddrStr);
      snapNameNAS.append(sqAddrStr);
    }
    snapName = space->allocateAlignedSpace(snapNameNAS.length() + 1);
    strcpy(snapName, snapNameNAS.data());
    snapAttrs->setSnapshotName(snapName);

    tmpLocNAS = generator->getSnapshotScanTmpLocation();
    CMPASSERT(tmpLocNAS[tmpLocNAS.length() - 1] == '/');
    tmpLoc = space->allocateAlignedSpace(tmpLocNAS.length() + 1);
    strcpy(tmpLoc, tmpLocNAS.data());

    snapAttrs->setSnapScanTmpLocation(tmpLoc);

    snapAttrs->setSnapshotScanTimeout(getDefault(TRAF_TABLE_SNAPSHOT_SCAN_TIMEOUT));
    // create the strings from generator heap
    NAString *tbl = new NAString(tablename, generator->wHeap());
    generator->objectNames().insert(*tbl);  // verified that it detects duplicate names in the NASet
  }
  ComTdbHbaseAccess::HbasePerfAttributes *hbpa = new (space) ComTdbHbaseAccess::HbasePerfAttributes();
  if (CmpCommon::getDefault(COMP_BOOL_184) == DF_ON) hbpa->setUseMinMdamProbeSize(TRUE);

  Float32 samplePerc = samplePercent();

  // TEMP_MONARCH Sample not supported with monarch tables.
  if (getTableDesc()->getNATable()->isSeabaseTable() && getTableDesc()->getNATable()->isMonarch()) samplePerc = 0;

  Lng32 hbaseRowSize;
  Lng32 hbaseBlockSize;
  if (getIndexDesc() && getIndexDesc()->getNAFileSet()) {
    const NAFileSet *fileset = (NAFileSet *)getIndexDesc()->getNAFileSet();
    hbaseRowSize = fileset->getRecordLength();
    hbaseRowSize += ((NAFileSet *)fileset)->getEncodedKeyLength();
    hbaseBlockSize = fileset->getBlockSize();
  } else {
    hbaseRowSize = computedHBaseRowSizeFromMetaData;
    hbaseBlockSize = CmpCommon::getDefaultLong(HBASE_BLOCK_SIZE);
  }

  generator->setHBaseNumCacheRows(MAXOF(getEstRowsAccessed().getValue(), getMaxCardEst().getValue()), hbpa,
                                  hbaseRowSize, samplePerc, isNeedPushDownLimit());
  generator->setHBaseCacheBlocks(hbaseRowSize, getEstRowsAccessed().getValue(), hbpa);

  generator->setHBaseSmallScanner(hbaseRowSize, getEstRowsAccessed().getValue(), hbaseBlockSize, hbpa);
  generator->setHBaseParallelScanner(hbpa);

  ComTdbHbaseAccess::ComHbaseAccessOptions *hbo = NULL;
  if (getOptHbaseAccessOptions()) {
    hbo = new (space) ComTdbHbaseAccess::ComHbaseAccessOptions();

    if (getOptHbaseAccessOptions()->getNumVersions() != 0)
      hbo->hbaseAccessOptions().setNumVersions(getOptHbaseAccessOptions()->getNumVersions());

    if (getOptHbaseAccessOptions()->hbaseMinTS() >= 0)
      hbo->hbaseAccessOptions().setHbaseMinTS(getOptHbaseAccessOptions()->hbaseMinTS());

    if (getOptHbaseAccessOptions()->hbaseMaxTS() >= 0)
      hbo->hbaseAccessOptions().setHbaseMaxTS(getOptHbaseAccessOptions()->hbaseMaxTS());

    char *haStr = NULL;
    if (NOT getOptHbaseAccessOptions()->hbaseAuths().isNull()) {
      haStr = space->allocateAlignedSpace(getOptHbaseAccessOptions()->hbaseAuths().length() + 1);
      strcpy(haStr, getOptHbaseAccessOptions()->hbaseAuths().data());

      hbo->setHbaseAuths(haStr);
    }
  }

  // determine object epoch info for tdb
  bool validateDDL = getTableDesc()->getNATable()->DDLValidationRequired();
  UInt32 expectedEpoch = 0;
  UInt32 expectedFlags = 0;
  if (validateDDL)  // if it is not a metadata table
  {
    expectedEpoch = getTableDesc()->getNATable()->expectedEpoch();
    expectedFlags = getTableDesc()->getNATable()->expectedFlags();
  }

  // create hbasescan_tdb
  ComTdbHbaseAccess *hbasescan_tdb = new (space) ComTdbHbaseAccess(
      ComTdbHbaseAccess::SELECT_, tablename, baseTableName,

      convert_expr, scanExpr, rowIdExpr,
      NULL,  // updateExpr
      NULL,  // mergeInsertExpr
      NULL,  // mergeInsertRowIdExpr
      NULL,  // mergeUpdScanExpr
      NULL,  // projExpr
      NULL,  // returnedUpdatedExpr
      NULL,  // returnMergeUpdateExpr
      encodedKeyExpr,
      NULL,  // keyColValExpr
      hbaseFilterValExpr,
      NULL,  // hbTagExpr

      asciiRowLen, convertRowLen,
      0,  // updateRowLen
      0,  // mergeInsertRowLen
      0,  // fetchedRowLen
      0,  // returnedRowLen

      rowIdLength, convertRowLen, rowIdAsciiRowLen, (keyInfo ? keyInfo->getKeyLength() : 0),
      0,  // keyColValLen
      hbaseFilterValRowLen,
      0,  // hbTagRowLen

      asciiTuppIndex, convertTuppIndex,
      0,  // updateTuppIndex
      0,  // mergeInsertTuppIndex
      0,  // mergeInsertRowIdTuppIndex
      0,  // mergeIUDIndicatorTuppIndex
      0,  // returnedFetchedTuppIndex
      0,  // returnedUpdatedTuppIndex

      rowIdTuppIndex, returnedDesc->noTuples() - 1, rowIdAsciiTuppIndex, keyTuppIndex,
      0,  // keyColValTuppIndex
      hbaseFilterValTuppIndex,

      (hbTsVIDlist.entries() > 0 ? hbaseTimestampTuppIndex : 0),
      (hbVersVIDlist.entries() > 0 ? hbaseVersionTuppIndex : 0),
      0,  // hbTagTuppIndex
      (hbRidVIDlist.entries() > 0 ? hbaseRowidIndex : 0),

      tdbListOfRangeRows, tdbListOfUniqueRows, listOfFetchedColNames, hbaseFilterColNames, hbaseCompareOpList,

      keyInfo, NULL,

      work_cri_desc, givenDesc, returnedDesc, downqueuelength, upqueuelength, expectedRows, numBuffers, buffersize,

      server, zkPort, hbpa, samplePerc, snapAttrs,

      hbo,

      pkeyColName,

      validateDDL, expectedEpoch, expectedFlags);

  hbasescan_tdb->setOptLargVar(CmpCommon::getDefault(OPTIMIZE_LARGE_VARCHAR) == DF_ON);

  hbasescan_tdb->setRecordLength(getGroupAttr()->getRecordLength());

  if (scanPrecondExpr) hbasescan_tdb->setScanPreCondExpr(scanPrecondExpr);
  generator->initTdbFields(hbasescan_tdb);

  if (getTableDesc()->getNATable()->isHbaseRowTable())  // rowwiseHbaseFormat())
    hbasescan_tdb->setRowwiseFormat(TRUE);

  // set we are replace hbase name by uid
  if (USE_UUID_AS_HBASE_TABLENAME && ComIsUserTable(getTableName())) {
    if (genTableName(generator, space, getTableName(), getTableDesc()->getNATable(), getIndexDesc()->getNAFileSet(),
                     FALSE, tablenameForUID, TRUE)) {
      GenAssert(0, "genTableName failed");
    }

    if (tablenameForUID) {
      hbasescan_tdb->setReplaceNameByUID(TRUE);
      hbasescan_tdb->setDataUIDName(tablenameForUID);
    }
  }

  hbasescan_tdb->setUseCif(hbaseRowFormat == ExpTupleDesc::SQLMX_ALIGNED_FORMAT);

  if (getTableDesc()->getNATable()->isSeabaseTable()) {
    hbasescan_tdb->setStorageType(getTableDesc()->getNATable()->storageType());

    hbasescan_tdb->setSQHbaseTable(TRUE);

    if (isAlignedFormat) hbasescan_tdb->setAlignedFormat(TRUE);

    if (isHbaseMapFormat) {
      hbasescan_tdb->setHbaseMapTable(TRUE);

      if (getTableDesc()->getNATable()->getClusteringIndex()->hasSingleColVarcharKey())
        hbasescan_tdb->setKeyInVCformat(TRUE);

      hbasescan_tdb->setTreatEmptyAsNull((CmpCommon::getDefault(TRAF_HBMT_TREAT_EMPTY_STRING_AS_NULL) == DF_ON));
    }

    if (getTableDesc()->getNATable()->isEnabledForDDLQI())
      generator->objectUids().insert(getTableDesc()->getNATable()->objectUid().get_value());

    if (getFirstNRows() > 0) {
      GenAssert(getFirstNRows() > 0, "first N rows must not be set.");
    }

    if (getTableDesc()->getNATable()->useEncryption()) {
      hbasescan_tdb->setUseEncryption(TRUE);

      char *encryptionInfo = space->allocateAndCopyToAlignedSpace(
          (char *)&((NATable *)getTableDesc()->getNATable())->encryptionInfo(), sizeof(ComEncryption::EncryptionInfo));

      hbasescan_tdb->setEncryptionInfo(encryptionInfo, sizeof(ComEncryption::EncryptionInfo));
    }

    if (CmpCommon::getDefault(USE_TRIGGER) == DF_ON && getTableDesc()->getNATable()->hasTrigger())
      hbasescan_tdb->setUseTrigger(TRUE);

    hbasescan_tdb->setTableId(getTableDesc()->getNATable()->objectUid().castToInt64());
    hbasescan_tdb->setDataUId(getTableDesc()->getNATable()->objDataUID().castToInt64());

    hbasescan_tdb->setNumReplications(((NATable *)getTableDesc()->getNATable())->getNumReplications());
  }

  if (keyInfo && searchKey() && searchKey()->isUnique()) hbasescan_tdb->setUniqueKeyInfo(TRUE);

  if (uniqueRowsetHbaseOper()) {
    hbasescan_tdb->setRowsetOper(TRUE);
    // for mantis 21167
    hbasescan_tdb->setHbaseRowsetVsbbSize(MINOF(hbpa->numCacheRows(), getDefault(HBASE_ROWSET_VSBB_SIZE)));
  }

  if ((Get_SqlParser_Flags(INTERNAL_QUERY_FROM_EXEUTIL)) &&
      ((getTableName().isSeabaseMD()) || (getTableName().isSeabasePrivMgrMD()))) {
    accessOptions().accessType() = TransMode::SKIP_CONFLICT_ACCESS_;
  }

  hbasescan_tdb->setSkipReadConflict(FALSE);
  TransMode::AccessType accessType = accessOptions().accessType();
  LockMode lockMode = accessOptions().lockMode();
  if (accessType == TransMode::ACCESS_TYPE_NOT_SPECIFIED_)
    accessType = TransMode::ILtoAT(generator->currentCmpContext()->getTransMode().getIsolationLevel());
  if (accessType == TransMode::SKIP_CONFLICT_ACCESS_) hbasescan_tdb->setSkipReadConflict(TRUE);
  // When SKIP_CONFLICT_FOR_NOUPDATE_SCAN, is set, all read conflict with write is not checked
  NABoolean skipReadConflict = (CmpCommon::getDefault(SKIP_CONFLICT_FOR_NOUPDATE_SCAN) == DF_ON);
  if (skipReadConflict) {
    if (!generator->updatableSelect()) {
      if (accessType == TransMode::READ_UNCOMMITTED_ACCESS_ || accessType == TransMode::READ_COMMITTED_ACCESS_)
        hbasescan_tdb->setSkipReadConflict(TRUE);
    }
  } else {
    // when this scan is part of Update/Delete/Merge, no need to check conflict
    // Looks like noSecurityCheck method is overloaded to mean that it is part of IUD
    if (noSecurityCheck() == TRUE) hbasescan_tdb->setSkipReadConflict(TRUE);
  }

  /* if the plan is not ESP parallel and it is not DDL , and it is not fork a new arkcmp*/
  /* set the firstReadBypassTm flag to true                                             */
  /* this will allow the RMInterface layer to decide if a read operation can bypass TM  */
  /* RMInterface check the TransactionState, to see if the read targe is not dirty      */
  /* if the target (region) is not dirty, it should be safe to bypass the TM path       */
  if (getFirstReadBypassTM() == TRUE && generator->getNumESPs() == 1) {
    hbasescan_tdb->setFirstReadBypassTm(TRUE);
  } else {
    hbasescan_tdb->setFirstReadBypassTm(FALSE);
  }

  if (CmpCommon::context()->isEmbeddedArkcmp() == false)  // this is a forked compiler process
  {
    hbasescan_tdb->setFirstReadBypassTm(FALSE);
  }

  if (gEnableRowLevelLock) {
    if (lockMode == LockMode::PROTECTED_) {
      hbasescan_tdb->setLockMode(HBaseLockMode::LOCK_U);
    }
    if (accessType == TransMode::SERIALIZABLE_ACCESS_) {
      hbasescan_tdb->setIsolationLevel(TransMode::SERIALIZABLE_ACCESS_);
    } else if (accessType == TransMode::REPEATABLE_READ_ACCESS_) {
      hbasescan_tdb->setIsolationLevel(TransMode::REPEATABLE_READ_);
    } else if (accessType == TransMode::READ_COMMITTED_ACCESS_) {
      hbasescan_tdb->setIsolationLevel(TransMode::READ_COMMITTED_);
    } else if (accessType == TransMode::READ_UNCOMMITTED_ACCESS_) {
      hbasescan_tdb->setIsolationLevel(TransMode::READ_UNCOMMITTED_);
    }
  }

  if (searchKey() && searchKey()->isUnique()) {
    if (accessOptions().accessType() == TransMode::SERIALIZABLE_ACCESS_ &&
        CmpCommon::getDefault(WAIT_ON_SELECT_FOR_UPDATE) == DF_ON && gEnableRowLevelLock == FALSE) {
      hbasescan_tdb->setWaitOnSelectForUpdate(TRUE);
    }
    // it is a requirement to start transaction
    // This is redundant, but this is a double check
    // When select for update is the very first statement
    // It still not start transaction
    // When that issue gone, will remove this line
    if (ActiveSchemaDB()->getDefaults().getAsLong(BEGIN_TRANSACTION_FOR_SELECT) != 0 &&
        (accessOptions().accessType() == TransMode::SERIALIZABLE_ACCESS_ ||
         (gEnableRowLevelLock && lockMode == LockMode::PROTECTED_)))
      generator->setTransactionFlag(TRUE);
  }
  hbasescan_tdb->setSkipTransaction(skipTransactionForBatchGet() || skipTransactionForCursorUpdDelScan());
  hbasescan_tdb->setSkipTransactionForced(skipTransactionForBatchGetForced() ||
                                          skipTransactionForCursorUpdDelScanForced() ||
                                          CmpCommon::getDefault(TRAF_NO_DTM_XN) == DF_ON);

  if (loadDataIntoMemoryTableOnly()) {
    hbasescan_tdb->setScanMemoryTable(true);
    hbasescan_tdb->setLoadDataIntoMemoryTable(true);
  }
  if ((getTableDesc()->getNATable()->readOnlyEnabled()) && (CmpCommon::getDefault(MEMORY_TABLE_SCAN) == DF_ON))
    hbasescan_tdb->setScanMemoryTable(true);

  if (!generator->explainDisabled()) {
    generator->setExplainTuple(addExplainInfo(hbasescan_tdb, 0, 0, generator));
  }

  if ((generator->computeStats()) && (generator->collectStatsType() == ComTdb::PERTABLE_STATS ||
                                      generator->collectStatsType() == ComTdb::OPERATOR_STATS)) {
    hbasescan_tdb->setPertableStatsTdbId((UInt16)generator->getPertableStatsTdbId());
  }

  generator->setCriDesc(givenDesc, Generator::DOWN);
  generator->setCriDesc(returnedDesc, Generator::UP);
  generator->setGenObj(this, hbasescan_tdb);

  return 0;
}
