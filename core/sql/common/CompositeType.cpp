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
**************************************************************************
*
* File:         CompositeType.C
* Description:  Composite types(ARRAY, ROW, MAP)
* Created:
* Language:     C++
*
*
*
*
**************************************************************************
*/

// -----------------------------------------------------------------------

#include "common/CompositeType.h"
#include "common/str.h"
#include "common/ComDefs.h"
#include "common/CmpCommon.h"

// ***********************************************************************
//
//  CompositeType : The composite data type
//    Base class for ARRAY and ROW
//
// ***********************************************************************
CompositeType::CompositeType(NAMemory *heap, const NAString &adtName, NABuiltInTypeEnum ev, Int32 compFormat)
    : NAType(heap, adtName, ev, 0, TRUE, SQL_NULL_HDR_SIZE, TRUE /*varchar*/, SQL_VARCHAR_HDR_SIZE_4 /*4 bytes len*/
      ) {
  // composite datatypes are stored in either aligned format or exploded format.
  // default is aligned format.
  if (compFormat == COM_UNINITIALIZED_FORMAT)
    compFormat_ = COM_SQLMX_ALIGNED_FORMAT;
  else
    compFormat_ = compFormat;
}

const NAString &CompositeType::getCompDefnStr() const { return ((CompositeType *)this)->getCompDefnStr(); }

NAString &CompositeType::getCompDefnStr() {
  if (compDefnStr_.isNull()) {
    compDefnStr_ = genCompDefnStr();
  }

  return compDefnStr_;
}

// ***********************************************************************
//
//  SQLArray : The array data type
//
// ***********************************************************************
SQLArray::SQLArray(NAMemory *heap, const NAType *elementType, const Lng32 arraySize, Int32 compFormat)
    : CompositeType(heap, LiteralArray, NA_COMPOSITE_TYPE, compFormat),
      elementType_(elementType),
      arraySize_(arraySize) {
  if (arraySize_ == 0)  // array size not specified
  {
    // use default value.
    arraySize_ = CmpCommon::getDefaultNumeric(TRAF_DEFAULT_COMPOSITE_ARRAY_SIZE);
  }

  Lng32 nominalSize = 0;

  // Int32 containing number of entries in the created row.
  // This field precedes the actual contents.
  // See exp/ExpAlignedFormat.h for details on the format.
  nominalSize = sizeof(Int32);
  if (getCompFormat() == COM_SQLMX_ALIGNED_FORMAT) nominalSize += 4 * sizeof(Int32);  // aligned format header size

  if (elementType_) {
    nominalSize +=
        ROUND4(elementType_->getTotalAlignedSize() + (elementType_->isVaryingLen() ? sizeof(Int32) : 0)) * arraySize_;
  }
  setNominalSize(nominalSize);
}

NAType *SQLArray::newCopy2(Int32 compFormat, CollHeap *h) const {
  const NAType *pType = getElementType();
  NAType *newPType = NULL;
  if (pType->isComposite() && (compFormat != COM_UNINITIALIZED_FORMAT))
    newPType = ((CompositeType *)pType)->newCopy2(compFormat, h);
  else
    newPType = pType->newCopy(h);

  SQLArray *newType = new (h) SQLArray(h, newPType, getArraySize(), compFormat);

  return newType;
}

NAType *SQLArray::newCopy(CollHeap *h) const { return newCopy2(COM_UNINITIALIZED_FORMAT, h); }

short SQLArray::getMyTypeAsText(NAString *outputStr,  // output
                                NABoolean addNullability, NABoolean addCollation) const {
  NAString nas;
  if (elementType_->getMyTypeAsText(outputStr, addNullability, addCollation)) return -1;

  if ((addNullability) && (NOT supportsSQLnull())) {
    outputStr->append(" NOT NULL NOT DROPPABLE");
  }

  char buf[20];
  *outputStr += " ARRAY";
  if (getArraySize() > 0) {
    *outputStr += "[";
    *outputStr += str_itoa(getArraySize(), buf);
    *outputStr += "]";
  }

  return 0;
}

// returns the type string for hive array datatype.
// format:  array<...>
short SQLArray::getHiveTypeStr(Lng32 hiveType, Lng32 precision, Lng32 scale, NAString *outputStr /*out*/) const {
  (*outputStr).append("array<");

  if (getElementType()->getHiveTypeStr(getElementType()->getHiveType(), getElementType()->getPrecisionOrMaxNumChars(),
                                       getElementType()->getScale(), outputStr))
    return -1;

  (*outputStr).append(">");

  return 0;
}

short SQLArray::getFSDatatype() const { return REC_ARRAY; }

NAString SQLArray::getSimpleTypeName() const { return "ARRAY"; }

NAString SQLArray::getTypeSQLname(NABoolean terse) const {
  char buf[100];
  NAString s = elementType_->getTypeSQLname(terse);
  s += " ARRAY[";
  s += str_itoa(arraySize_, buf);
  s += "]";

  return s;
}

NAString SQLArray::genCompDefnStr() const {
  NAString fieldsInfo;

  getMyTypeAsText(&fieldsInfo, FALSE, FALSE);

  return fieldsInfo;
}

// TRUE, validation passed.
// FALSE, validation failed. Error set in diags.
NABoolean SQLArray::validate(ComDiagsArea *diags) {
  if (DFS2REC::isLOB(getElementType()->getFSDatatype())) {
    *diags << DgSqlCode(-3242) << DgString0("BLOB/CLOB types are not allowed as ARRAY elements.");
    return FALSE;  // BLOB/CLOB not allowed
  }

  if (arraySize_ > CmpCommon::getDefaultNumeric(TRAF_MAX_COMPOSITE_ARRAY_SIZE)) {
    char buf[100];
    NAString reason;
    reason = "Specified array size(";
    reason += str_itoa(arraySize_, buf);
    reason += ") cannot exceed the max size(";
    reason += str_itoa(CmpCommon::getDefaultNumeric(TRAF_MAX_COMPOSITE_ARRAY_SIZE), buf);
    reason += ").";
    *diags << DgSqlCode(-3242) << DgString0(reason);
    return FALSE;
  }

  if (getNumLevels() > CmpCommon::getDefaultNumeric(TRAF_MAX_COMPOSITE_LEVELS)) {
    char buf[100];
    NAString reason;
    reason = "Number of composite nesting levels(";
    reason += str_itoa(getNumLevels(), buf);
    reason += ") cannot exceed the max value(";
    reason += str_itoa(CmpCommon::getDefaultNumeric(TRAF_MAX_COMPOSITE_LEVELS), buf);
    reason += ").";
    *diags << DgSqlCode(-3242) << DgString0(reason);

    return FALSE;
  }

  return TRUE;
}

const NAType *SQLArray::synthesizeType(enum NATypeSynthRuleEnum synthRule, const NAType &operand1,
                                       const NAType &operand2, CollHeap *h, UInt32 *flags) const {
  //
  // If the second operand's type synthesis rules have higher precedence than
  // this operand's rules, use the second operand's rules.
  //
  if (operand2.getSynthesisPrecedence() > getSynthesisPrecedence())
    return operand2.synthesizeType(synthRule, operand1, operand2, h, flags);
  //
  // If either operand is not array, the expression is invalid.
  //
  if ((operand1.getFSDatatype() != REC_ARRAY) || (operand2.getFSDatatype() != REC_ARRAY)) return NULL;

  SQLArray &op1Array = (SQLArray &)operand1;
  SQLArray &op2Array = (SQLArray &)operand2;
  if (op1Array.getArraySize() != op2Array.getArraySize()) return NULL;

  if (synthRule == SYNTH_RULE_UNION) {
    const NAType *resultType = op1Array.getElementType()->synthesizeType(SYNTH_RULE_UNION, *op1Array.getElementType(),
                                                                         *op2Array.getElementType(), h);

    if (!resultType) return NULL;

    if (op1Array.getCompFormat() == op2Array.getCompFormat())
      return new (h) SQLArray(h, resultType, op1Array.getArraySize(), op1Array.getCompFormat());
    else
      return new (h) SQLArray(h, resultType, op1Array.getArraySize());
  }

  return NULL;
}  // synthesizeType()

// ---------------------------------------------------------------------
// Are the two types compatible for comparison or assignment?
// ---------------------------------------------------------------------
NABoolean SQLArray::isCompatible(const NAType &other, UInt32 *flags) const {
  if (getFSDatatype() != other.getFSDatatype()) return FALSE;

  const NAType *otherElemType = ((SQLArray &)other).getElementType();
  if ((!getElementType()) || (!otherElemType)) {
    // this is the case when only array index is to be changed
    return TRUE;
  }

  if (NOT getElementType()->isCompatible(*otherElemType, flags)) return FALSE;

  return TRUE;
}

NABoolean SQLArray::operator==(const NAType &other) const {
  if (NOT isCompatible(other)) return FALSE;

  if (NOT(getElementType()->equalIgnoreCoercibility(*((SQLArray &)other).getElementType()))) return FALSE;

  if (getCompFormat() != ((CompositeType &)other).getCompFormat()) return FALSE;

  return TRUE;
}

// display format:  ' [ ' <elemDisplay> ' , ' ... ' ] '
Lng32 SQLArray::getDisplayLength() const {
  Lng32 elemDL = getElementType()->getDisplayLength();

  Lng32 totalDL = 6;  // leading and trailing ' [ '  ' ] '
  totalDL +=
      totalDL + elemDL * getArraySize() + (getArraySize() - 1) * 3;  // extra 3 for " , " separator between elements.

  return totalDL;
}

// need to be in sync with fillPrimitiveType method in TrafParquetFileReader.jave
static Int32 getHivePrimitiveTypeLen(const NAType *elemType) {
  Int32 len = 0;
  switch (elemType->getHiveType()) {
    case HIVE_BOOLEAN_TYPE:
    case HIVE_BYTE_TYPE:
    case HIVE_SHORT_TYPE:
    case HIVE_INT_TYPE:
      len = 4;
      break;

    case HIVE_LONG_TYPE:
    case HIVE_FLOAT_TYPE:
    case HIVE_DOUBLE_TYPE:
      len = 8;
      break;

    case HIVE_CHAR_TYPE:
    case HIVE_VARCHAR_TYPE:
    case HIVE_STRING_TYPE:
    case HIVE_BINARY_TYPE:
      len = elemType->getNominalSize();
      break;

    case HIVE_DECIMAL_TYPE: {
      if (elemType->getPrecision() <= 18)
        len = 2 /*precision*/ + 2 /*scale*/ + 8 /*Int64*/;
      else
        len = NAType::getDisplayLengthStatic(elemType->getFSDatatype(), elemType->getNominalSize(),
                                             elemType->getPrecision(), elemType->getScale(), 0);
    } break;

    case HIVE_TIMESTAMP_TYPE:
      len = 11;
      break;

    default:
      assert(0);
      len = -1;
      break;
  }

  return len;
}

Int32 SQLArray::getHiveSourceMaxLen() {
  Int32 maxLen = 0;

  maxLen += sizeof(Int32);  // total length
  maxLen += sizeof(Int32);  // numElems

  const NAType *elemType = getElementType();

  Int32 elemLen = 0;
  if (elemType->isComposite())
    elemLen += ((CompositeType *)elemType)->getHiveSourceMaxLen();
  else {
    elemLen += sizeof(Int32);  // elemLen
    elemLen += getHivePrimitiveTypeLen(elemType);
  }

  maxLen += elemLen * getArraySize();

  return maxLen;
}

// ***********************************************************************
//
//  SQLRow : The ROW data type (aka struct for hive tables)
//
// ***********************************************************************
SQLRow::SQLRow(NAMemory *heap) : CompositeType(heap, LiteralRow, NA_COMPOSITE_TYPE) {
  fieldNames_ = new (heap) NAArray<NAString>(heap);
  fieldTypes_ = new (heap) NAArray<NAType *>(heap);
}

SQLRow::SQLRow(NAMemory *heap, NAArray<NAString> *fieldNames, NAArray<NAType *> *fTypes, Int32 compFormat)
    : CompositeType(heap, LiteralRow, NA_COMPOSITE_TYPE, compFormat), fieldNames_(fieldNames), fieldTypes_(fTypes) {
  Lng32 childMaxLevels = 0;
  Lng32 nominalSize = 0;

  NABoolean fieldNamesAdded = FALSE;
  if (fieldNames_ == NULL) {
    fieldNames_ = new (heap) NAArray<NAString>(heap);
    fieldNamesAdded = TRUE;
  }

  if (getCompFormat() == COM_SQLMX_ALIGNED_FORMAT) nominalSize += 4 * sizeof(Int32);  // header size
  for (Int32 i = 0; i < fieldTypes().entries(); i++) {
    nominalSize +=
        ROUND4(fieldTypes()[i]->getTotalAlignedSize() + (fieldTypes()[i]->isVaryingLen() ? sizeof(Int32) : 0));

    childMaxLevels = MAXOF(childMaxLevels, fieldTypes()[i]->getNumLevels());

    if (fieldNamesAdded) {
      char buf[100];
      NAString fieldName("ROW_");
      fieldName += str_itoa(i + 1, buf);
      fieldNames_->insertAt(i, fieldName);
    }
  }

  setNominalSize(nominalSize);

  numLevels_ = childMaxLevels + 1;
}

NAType *SQLRow::newCopy2(Int32 compFormat, CollHeap *h) const {
  SQLRow *p = (SQLRow *)this;

  NAArray<NAString> *fieldNames = NULL;
  NAArray<NAType *> *fieldTypes = new (h) NAArray<NAType *>(h);

  if (p->fieldNamesPtr()) {
    fieldNames = new (h) NAArray<NAString>(h);
  }

  fieldTypes->resize(p->fieldTypes().entries());
  for (CollIndex i = 0; i < p->fieldTypes().entries(); i++) {
    if (fieldNames && (p->fieldNames().entries() > 0)) fieldNames->insertAt(i, p->fieldNames()[i]);

    NAType *pType = p->fieldTypes()[i];
    NAType *newPType = NULL;
    if (pType->isComposite() && (compFormat != COM_UNINITIALIZED_FORMAT))
      newPType = ((CompositeType *)pType)->newCopy2(compFormat, h);
    else
      newPType = pType->newCopy(h);
    fieldTypes->insertAt(i, newPType);
  }

  SQLRow *newP = new (h) SQLRow(h, fieldNames, fieldTypes, compFormat);
  return newP;
}

NAType *SQLRow::newCopy(CollHeap *h) const { return newCopy2(COM_UNINITIALIZED_FORMAT, h); }

short SQLRow::getMyTypeAsText(NAString *outputStr,  // output
                              NABoolean addNullability, NABoolean addCollation) const {
  const NAString &nas = getCompDefnStr();
  outputStr->append(nas);

  if ((addNullability) && (NOT supportsSQLnull())) {
    outputStr->append(" NOT NULL NOT DROPPABLE");
  }

  return 0;
}

// hive datatype string.
// format:  struct<...>
short SQLRow::getHiveTypeStr(Lng32 hiveType, Lng32 precision, Lng32 scale, NAString *outputStr /*out*/) const {
  (*outputStr).append("struct<");

  SQLRow *sr = (SQLRow *)this;
  for (Int32 i = 0; i < sr->getNumElements(); i++) {
    NAType *elemType = sr->fieldTypes()[i];
    NAString elemName = sr->fieldNames()[i];
    elemName.toLower();
    (*outputStr).append(elemName + ":");
    if (elemType->getHiveTypeStr(elemType->getHiveType(), elemType->getPrecisionOrMaxNumChars(), elemType->getScale(),
                                 outputStr))
      return -1;

    if (i < (sr->getNumElements() - 1)) (*outputStr).append(",");
  }  // for

  (*outputStr).append(">");

  return 0;
}

// TRUE, validation passed.
// FALSE, validation failed. Error set in diags.
NABoolean SQLRow::validate(ComDiagsArea *diags) {
  NAString dupName;

  CollIndex curr = 0;
  CollIndex next = 0;
  while (curr < fieldTypes().entries()) {
    NAType *type = fieldTypes()[curr];
    if (DFS2REC::isLOB(type->getFSDatatype())) {
      *diags << DgSqlCode(-3242) << DgString0("BLOB/CLOB types are not allowed within a ROW definition.");
      return FALSE;  // BLOB/CLOB not allowed
    }

    if (fieldNamesPtr()) {
      next = curr + 1;
      while (next < fieldNames().entries()) {
        if (fieldNames()[curr] == fieldNames()[next]) {
          dupName = fieldNames()[curr];

          *diags << DgSqlCode(-3242) << DgString0("Duplicate column names are not allowed within a ROW definition.");
          return FALSE;  // dup name
        }

        next++;
      }  // while
    }

    curr++;
  }

  if (getNumLevels() > CmpCommon::getDefaultNumeric(TRAF_MAX_COMPOSITE_LEVELS)) {
    char buf[100];
    NAString reason;
    reason = "Number of composite nesting levels (";
    reason += str_itoa(getNumLevels(), buf);
    reason += ") exceed the max value(";
    reason += str_itoa(CmpCommon::getDefaultNumeric(TRAF_MAX_COMPOSITE_LEVELS), buf);
    reason += ").";
    *diags << DgSqlCode(-3242) << DgString0(reason);

    return FALSE;  // dup name
  }

  return TRUE;
}

short SQLRow::getFSDatatype() const { return REC_ROW; }

const NAString &SQLRow::getFieldName(CollIndex idx) const {
  assert((idx >= 1) && (idx <= fieldTypes_->entries()));
  return (*fieldNames_)[idx - 1];
}

const NAType *SQLRow::getFieldType(CollIndex idx) const {
  assert((idx >= 1) && (idx <= fieldTypes_->entries()));
  return (*fieldTypes_)[idx - 1];
}

NABoolean SQLRow::getElementInfo(const NAString &fieldName, const NAType *&elemType, Int32 &elemNum) {
  elemNum = 0;
  elemType = NULL;

  if (!fieldNames_) return FALSE;

  for (Int32 idx = 1; idx <= fieldNames().entries(); idx++) {
    if (getFieldName(idx) == fieldName) {
      elemNum = idx;
      elemType = getElementType(idx);
      return TRUE;  // found
    }
  }

  return FALSE;  // not found
}

NAString SQLRow::getSimpleTypeName() const { return "ROW"; }

// -- The external name for the type (text representation)
NAString SQLRow::getTypeSQLname(NABoolean terse) const {
  SQLRow *p = (SQLRow *)this;
  NAString name = "ROW(";
  for (CollIndex i = 0; i < p->fieldTypes().entries(); i++) {
    name += p->fieldTypes()[i]->getTypeSQLname(terse);
    if (i < p->fieldTypes().entries() - 1) name += ", ";
  }
  name += ")";
  return name;
}

NAString SQLRow::genCompDefnStr() const {
  SQLRow *p = (SQLRow *)this;
  NAString fieldInfo;

  fieldInfo += "ROW(";
  for (CollIndex i = 0; i < p->fieldTypes().entries(); i++) {
    if (p->fieldNamesPtr() && (p->fieldNames().entries() > 0)) {
      fieldInfo += ANSI_ID(p->fieldNames()[i]);
      fieldInfo += " ";
    }

    NAString nas;
    p->fieldTypes()[i]->getMyTypeAsText(&nas, FALSE, FALSE);
    fieldInfo += nas;

    if (i < p->fieldTypes().entries() - 1) fieldInfo += ", ";
  }

  fieldInfo += ")";

  return fieldInfo;
}

const NAType *SQLRow::synthesizeType(enum NATypeSynthRuleEnum synthRule, const NAType &operand1, const NAType &operand2,
                                     CollHeap *h, UInt32 *flags) const {
  //
  // If the second operand's type synthesis rules have higher precedence than
  // this operand's rules, use the second operand's rules.
  //
  if (operand2.getSynthesisPrecedence() > getSynthesisPrecedence())
    return operand2.synthesizeType(synthRule, operand1, operand2, h, flags);

  //
  // If either operand is not ROW, the expression is invalid.
  //
  if ((operand1.getFSDatatype() != REC_ROW) || (operand2.getFSDatatype() != REC_ROW)) return NULL;

  SQLRow &op1Row = (SQLRow &)operand1;
  SQLRow &op2Row = (SQLRow &)operand2;
  if (op1Row.getNumElements() != op2Row.getNumElements()) return NULL;

  if (synthRule == SYNTH_RULE_UNION) {
    NAArray<NAString> *fieldNames = new (h) NAArray<NAString>(h);
    NAArray<NAType *> *fieldTypes = new (h) NAArray<NAType *>(h);

    // synthesize op1 fields with the corresponding op2 fields
    for (UInt32 i = 0; i < op1Row.getNumElements(); i++) {
      const NAType *resultType = op1Row.getElementType(i + 1)->synthesizeType(
          SYNTH_RULE_UNION, *op1Row.getElementType(i + 1), *op2Row.getElementType(i + 1), h);
      if (!resultType) return NULL;
      fieldTypes->insertAt(i, (NAType *)resultType);
      if (op1Row.fieldNamesPtr()) fieldNames->insertAt(i, op1Row.fieldNames()[i]);
    }

    if (op1Row.getCompFormat() == op2Row.getCompFormat())
      return new (h) SQLRow(h, fieldNames, fieldTypes, op1Row.getCompFormat());
    else
      return new (h) SQLRow(h, fieldNames, fieldTypes);
  }

  return NULL;
}  // synthesizeType()

// ---------------------------------------------------------------------
// Are the two types compatible for comparison or assignment?
// ---------------------------------------------------------------------
NABoolean SQLRow::isCompatible(const NAType &other, UInt32 *flags) const {
  if (getFSDatatype() != other.getFSDatatype()) return FALSE;

  SQLRow *p = (SQLRow *)this;
  SQLRow &newP = (SQLRow &)other;

  if (p->fieldTypes().entries() != newP.fieldTypes().entries()) return FALSE;

  for (CollIndex i = 0; i < p->fieldTypes().entries(); i++) {
    NAType *type = p->fieldTypes()[i];
    NAType *otherType = newP.fieldTypes()[i];

    if (NOT type->isCompatible(*otherType, flags)) return FALSE;
  }

  return TRUE;
}

NABoolean SQLRow::operator==(const NAType &other) const {
  if (NOT isCompatible(other)) return FALSE;

  SQLRow *p = (SQLRow *)this;
  SQLRow &newP = (SQLRow &)other;

  for (CollIndex i = 0; i < p->fieldTypes().entries(); i++) {
    NAType *type = p->fieldTypes()[i];
    NAType *otherType = newP.fieldTypes()[i];

    // if (NOT (*type == *otherType))
    if (NOT(type->equalIgnoreCoercibility(*otherType))) return FALSE;
  }

  if (getCompFormat() != ((CompositeType &)other).getCompFormat()) return FALSE;

  return TRUE;
}

// display format:  ' ( ' <elemDisplay> ' , ' ... ' ) '
Lng32 SQLRow::getDisplayLength() const {
  Lng32 totalDL = 6;  // leading and trailing ' ( ' and ' ) '

  SQLRow *p = (SQLRow *)this;
  for (CollIndex i = 0; i < p->fieldTypes().entries(); i++) {
    NAType *type = p->fieldTypes()[i];
    totalDL += type->getDisplayLength();

    if (i < (p->fieldTypes().entries() - 1)) {
      totalDL += 3;  // extra 3 for " , " separator between fields
    }
  }

  return totalDL;
}

Int32 SQLRow::getHiveSourceMaxLen() {
  Int32 maxLen = 0;

  maxLen += sizeof(Int32);  // total length
  maxLen += sizeof(Int32);  // numElems

  for (Int32 i = 0; i < getNumElements(); i++) {
    NAType *elemType = getElementType(i + 1);
    if (elemType->isComposite())
      maxLen += ((CompositeType *)elemType)->getHiveSourceMaxLen();
    else {
      maxLen += sizeof(Int32);  // elemLen
      maxLen += getHivePrimitiveTypeLen(elemType);
    }
  }

  return maxLen;
}
