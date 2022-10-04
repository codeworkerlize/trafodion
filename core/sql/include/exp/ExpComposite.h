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
#ifndef EXP_COMPOSITE_H
#define EXP_COMPOSITE_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:  Expression clauses for composite expressions (ARRAY, ROW)
 *
 * Created:
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "exp/exp_clause.h"
#include "exp/exp_clause_derived.h"
#include "common/dfs2rec.h"
#include "exp_function.h"

////////////////////////////////////////////////////////////////////////
// class CompositeAttributes
//  For ARRAY and ROW types.
// created in method createCompositeAttributes in GenItemComposite.cpp
////////////////////////////////////////////////////////////////////////
class CompositeAttributes : public Attributes {
 public:
  CompositeAttributes(Lng32 length) : length_(length), numElements_(0), elements_(NULL) {
    setClassID(CompositeAttributesID);
    memset(fillers_, 0, sizeof(fillers_));
  }

  CompositeAttributes(Int16 datatype, Int32 length, ExpTupleDesc::TupleDataFormat tdf, Int32 alignment, Int16 nullFlag,
                      Int16 nullIndicatorLen, Int16 vcIndicatorLen, DefaultClass defClass)
      : elements_(NULL) {
    setClassID(CompositeAttributesID);
    setLength(length);
    setDatatype(datatype);
    setTupleFormat(tdf);
    setNullFlag(nullFlag);
    setNullIndicatorLength(nullIndicatorLen);
    setVCIndicatorLength(vcIndicatorLen);
    setDefaultClass(defClass);
    setDataAlignmentSize(alignment);
    numElements_ = 0;
    memset(fillers_, 0, sizeof(fillers_));
  }

  CompositeAttributes() : elements_(NULL) {
    length_ = 0;
    numElements_ = 0;
    memset(fillers_, 0, sizeof(fillers_));
  }

  ~CompositeAttributes() {}

  void setLength(Int32 length) { length_ = length; }
  Int32 getLength() { return length_; }

  void copyAttrs(Attributes *source_) { *this = *(CompositeAttributes *)source_; }

  Int32 getStorageLength() {
    Int32 ret_length = length_;

    if (getNullFlag()) ret_length += getNullIndicatorLength();

    ret_length += getVCIndicatorLength();

    return ret_length;
  }

  virtual Int32 getDefaultValueStorageLength() {
    Int32 retLen = length_;

    if (getNullFlag()) retLen += ExpTupleDesc::NULL_INDICATOR_LENGTH;

    retLen += getVCIndicatorLength();

    return retLen;
  }

  Attributes *newCopy(CollHeap *heap) {
    CompositeAttributes *new_copy = new (heap) CompositeAttributes();
    *new_copy = *this;
    return new_copy;
  }

  virtual Long pack(void *);
  virtual Int32 unpack(void *, void *reallocator);

  void setElements(AttributesPtrPtr attrs) { elements_ = attrs; }
  AttributesPtrPtr getElements() { return elements_; }
  void setNumElements(UInt32 v) { numElements_ = v; }
  UInt32 getNumElements() { return numElements_; }

  Int32 getCompFormat() { return compFormat_; }
  void setCompFormat(Int32 v) { compFormat_ = v; }

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    Attributes::populateImageVersionIDArray();
  }

  virtual Int16 getClassSize() { return (Int16)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  Int32 length_;  // 00-03

  UInt32 numElements_;         // 04-07
  AttributesPtrPtr elements_;  // 08-15

  Int32 compFormat_;  // 16-19

  // ---------------------------------------------------------------------
  // Fillers for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[36];
};

///////////////////////////////////////////////////////////////
// class ExpCompositeBase
///////////////////////////////////////////////////////////////
class ExpCompositeBase : public ex_function_clause {
 public:
  ExpCompositeBase(OperatorTypeEnum oper_type, short type, ULng32 numElements, short numAttrs, Attributes **attr,
                   ULng32 tupleLen, ex_expr *compExpr, ex_cri_desc *compCriDesc, AttributesPtr compAttrs, Space *space);
  ExpCompositeBase(){};

  virtual ex_expr::exp_return_type fixup(Space *space, CollHeap *exHeap, char *constantsArea, char *tempsArea,
                                         char *persistentArea, short fixupFlag, NABoolean spaceCompOnly);

  virtual Long pack(void *);
  virtual Lng32 unpack(void *base, void *reallocator);

  virtual Int32 isNullRelevant() const { return 1; };

  virtual ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 flags);

  virtual void displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum, char *constsArea,
                               ULng32 flag);

  ULng32 numElements() { return numElements_; }

  ex_expr *getCompExpr() { return compExpr_; }

  Attributes *getCompAttrs() { return compAttrs_; }

 protected:
  ExExprPtr compExpr_;
  ExCriDescPtr compCriDesc_;
  AttributesPtr compAttrs_;
  short type_;
  UInt16 flags_;
  ULng32 numElements_;

  ULng32 compRowLen_;
  char filler1_[4];

  char errBuf_[64];
  char errBuf2_[64];

  // next 3 fields are allocated and used at runtime
  AtpStructPtr compAtp_;
  tupp_descriptor *compTuppDesc_;
  char *compRow_;
};

///////////////////////////////////////////////////////
// class ExpCompositeArrayLength
///////////////////////////////////////////////////////
class ExpCompositeArrayLength : public ExpCompositeBase {
 public:
  ExpCompositeArrayLength(OperatorTypeEnum oper_type, short type, Attributes **attr, Space *space);
  ExpCompositeArrayLength(){};

  virtual ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  virtual void displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum, char *constsArea,
                               ULng32 flag);

  virtual short getClassSize() { return (short)sizeof(*this); }

 protected:
};

///////////////////////////////////////////////////////
// class ExpCompositeArrayCast
///////////////////////////////////////////////////////
class ExpCompositeArrayCast : public ExpCompositeBase {
 public:
  ExpCompositeArrayCast(OperatorTypeEnum oper_type, short type, Attributes **attr, AttributesPtr compAttrs,
                        AttributesPtr compAttrsChild1, Space *space);
  ExpCompositeArrayCast(){};

  virtual ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  virtual Long pack(void *);
  virtual Lng32 unpack(void *base, void *reallocator);

  virtual void displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum, char *constsArea);

  virtual short getClassSize() { return (short)sizeof(*this); }

  Attributes *getChildCompAttrs() { return compAttrsChild1_; }

 protected:
  AttributesPtr compAttrsChild1_;
};

///////////////////////////////////////////////////////
// class ExpCompositeHiveCast
///////////////////////////////////////////////////////
class ExpCompositeHiveCast : public ExpCompositeBase {
 public:
  ExpCompositeHiveCast(OperatorTypeEnum oper_type, NABoolean fromHive, short type, Attributes **attr,
                       AttributesPtr compAttrs, AttributesPtr compAttrsChild1, Space *space);
  ExpCompositeHiveCast(){};

  ex_expr::exp_return_type copyHiveSrcToTgt(Attributes *tgtAttr, char *&srcData, char *tgtData, CollHeap *heap,
                                            ComDiagsArea **diagsArea, NABoolean isRoot);

  virtual ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  virtual Long pack(void *);
  virtual Lng32 unpack(void *base, void *reallocator);

  virtual void displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum, char *constsArea);

  virtual short getClassSize() { return (short)sizeof(*this); }

  NABoolean fromHive(void) { return ((flags_ & FROM_HIVE) != 0); };

  inline void setFromHive(NABoolean v) { (v) ? flags_ |= FROM_HIVE : flags_ &= ~FROM_HIVE; }

 protected:
  enum { FROM_HIVE = 0x00000001 };

  AttributesPtr compAttrsChild1_;
  UInt32 flags_;
  char filler1_[4];
};

///////////////////////////////////////////////////////
// class ExpCompositeConcat
///////////////////////////////////////////////////////
class ExpCompositeConcat : public ExpCompositeBase {
 public:
  ExpCompositeConcat(OperatorTypeEnum oper_type, short type, Attributes **attr, AttributesPtr compAttrs,
                     AttributesPtr compAttrsChild1, AttributesPtr compAttrsChild21, Space *space);
  ExpCompositeConcat(){};

  virtual ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  virtual Long pack(void *);
  virtual Lng32 unpack(void *base, void *reallocator);

  virtual void displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum, char *constsArea,
                               ULng32 flag);

  virtual short getClassSize() { return (short)sizeof(*this); }

 protected:
  AttributesPtr compAttrsChild1_;
  AttributesPtr compAttrsChild2_;
};

///////////////////////////////////////////////////////
// class ExpCompositeCreate
///////////////////////////////////////////////////////
class ExpCompositeCreate : public ExpCompositeBase {
 public:
  ExpCompositeCreate(OperatorTypeEnum oper_type, short type, ULng32 numElements, short numAttrs, Attributes **attr,
                     ULng32 tupleLen, ex_expr *compExpr, ex_cri_desc *compCriDesc, AttributesPtr compAttrs,
                     Space *space);
  ExpCompositeCreate(){};

  virtual Long pack(void *);
  virtual Lng32 unpack(void *base, void *reallocator);

  virtual ex_expr::exp_return_type eval(char *op_data[], atp_struct *atp1, atp_struct *atp2, atp_struct *atp3,
                                        CollHeap *, ComDiagsArea ** = 0);

  // This clause handles all NULL processing in the eval() method.
  virtual Int32 isNullRelevant() const { return 0; };

  virtual void displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum, char *constsArea,
                               ULng32 flag);

  virtual short getClassSize() { return (short)sizeof(*this); }

  virtual NABoolean evalNeedAtps() { return TRUE; }

 protected:
};

///////////////////////////////////////////////////////
// class ExpCompositeDisplay
///////////////////////////////////////////////////////
class ExpCompositeDisplay : public ExpCompositeBase {
 public:
  ExpCompositeDisplay(OperatorTypeEnum oper_type, short type, ULng32 numElements, short numAttrs, Attributes **attr,
                      ex_cri_desc *compCriDesc, AttributesPtr compAttrs, Space *space);
  ExpCompositeDisplay(){};

  virtual ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  virtual void displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum, char *constsArea);

  virtual short getClassSize() { return (short)sizeof(*this); }

 private:
};

/////////////////////////////////////////////////////
// class ExpCompositeExtract
/////////////////////////////////////////////////////
class ExpCompositeExtract : public ExpCompositeBase {
 public:
  ExpCompositeExtract(OperatorTypeEnum oper_type, short type, ULng32 numElements, Lng32 elemNum, short numAttrs,
                      Attributes **attr, AttributesPtr compAttrs, Lng32 numSearchAttrs, char *searchAttrTypeList,
                      char *searchAttrIndexList, Space *space);
  ExpCompositeExtract(){};

  virtual Long pack(void *);
  virtual Lng32 unpack(void *base, void *reallocator);

  virtual Int32 isNullRelevant() const { return 0; };

  virtual ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  virtual void displayContents(Space *space, const char * /*displayStr*/, Int32 clauseNum, char *constsArea);

  virtual short getClassSize() { return (short)sizeof(*this); }

  static ex_expr::exp_return_type extractValue(Attributes *compAttrs, Lng32 elemNum, char *tgtPtr, char *srcPtr,
                                               Int32 maxNumElems, NABoolean &isNullVal, ULng32 &attrLen,
                                               Int32 &numElems, CollHeap *heap, ComDiagsArea **diagsArea);

 private:
  ex_expr::exp_return_type searchAndExtractValue(Attributes *inAttrs, char *tgtPtr, char *srcPtr, NABoolean &isNullVal,
                                                 ULng32 &attrLen, CollHeap *heap, ComDiagsArea **diagsArea);

  Lng32 numSearchAttrs() { return numSearchAttrs_; }
  Int32 getSearchAttrType(Lng32 i) {
    char *searchType = searchAttrTypeList_;
    Int32 *searchTypeInt32 = (Int32 *)searchType;
    return searchTypeInt32[i];
  }
  Int32 getSearchAttrIndex(Lng32 i) {
    char *searchIndex = searchAttrIndexList_;
    Int32 *searchIndexInt32 = (Int32 *)searchIndex;
    return searchIndexInt32[i];
  }

  Lng32 elemNum_;
  UInt32 flags_;

  Int32 numSearchAttrs_;
  char filler1_[4];
  NABasicPtr searchAttrTypeList_;
  NABasicPtr searchAttrIndexList_;
};

#endif
