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
* File:         ComTdbQI.h
* Description:
*
* Created:      3/29/2021
* Language:     C++
*
*
*
*
****************************************************************************
*/

#ifndef COMTDBQI_H
#define COMTDBQI_H

#include "comexe/ComTdb.h"
#include "exp/ExpCriDesc.h"
#include "exp/exp_attrs.h"

enum QIStatType {
  STAT_NID = 0,
  STAT_OPERATION = 1,
  STAT_OBJECT_UID = 2,
  STAT_SUBJECT_HASH = 3,
  STAT_OBJECT_HASH = 4,
  STAT_REVOKE_TIME = 5
};

/****************************************************************************
  - NID : node id, start from 0
  - OPERATION : query invalidation type
  - OBJECT_UID : object uid, validate for COM_QI_OBJECT_REDEF and COM_QI_STATS_UPDATED
  - SUBJECT_HASH : generateHash(subjectUserID), validate for priviledge operation
  - OBJECT_HASH :  generateHash(objectUserID), validate for priviledge operation
  - REVOKE_TIME : update timestamp in Int64
****************************************************************************/

static const ComTdbVirtTableColumnInfo queryInvalidateVirtTableColumnInfo[] = {  // offset
    {"NID",
     0,
     COM_USER_COLUMN,
     REC_BIN32_SIGNED,
     4,
     FALSE,
     SQLCHARSETCODE_UNKNOWN,
     0,
     0,
     0,
     0,
     0,
     0,
     0,
     COM_NO_DEFAULT,
     "",
     NULL,
     NULL,
     COM_UNKNOWN_DIRECTION_LIT,
     0},
    {"OPERATION",
     1,
     COM_USER_COLUMN,
     REC_BYTE_F_ASCII,
     2,
     FALSE,
     SQLCHARSETCODE_UNKNOWN,
     0,
     0,
     0,
     0,
     0,
     0,
     0,
     COM_NO_DEFAULT,
     "",
     NULL,
     NULL,
     COM_UNKNOWN_DIRECTION_LIT,
     0},
    {"OBJECT_UID",
     2,
     COM_USER_COLUMN,
     REC_BIN64_SIGNED,
     8,
     FALSE,
     SQLCHARSETCODE_UNKNOWN,
     0,
     0,
     0,
     0,
     0,
     0,
     0,
     COM_NO_DEFAULT,
     "",
     NULL,
     NULL,
     COM_UNKNOWN_DIRECTION_LIT,
     0},
    {"SUBJECT_HASH",
     3,
     COM_USER_COLUMN,
     REC_BIN32_UNSIGNED,
     4,
     FALSE,
     SQLCHARSETCODE_UNKNOWN,
     0,
     0,
     0,
     0,
     0,
     0,
     0,
     COM_NO_DEFAULT,
     "",
     NULL,
     NULL,
     COM_UNKNOWN_DIRECTION_LIT,
     0},
    {"OBJECT_HASH",
     4,
     COM_USER_COLUMN,
     REC_BIN32_UNSIGNED,
     4,
     FALSE,
     SQLCHARSETCODE_UNKNOWN,
     0,
     0,
     0,
     0,
     0,
     0,
     0,
     COM_NO_DEFAULT,
     "",
     NULL,
     NULL,
     COM_UNKNOWN_DIRECTION_LIT,
     0},
    {"REVOKE_TIME",
     5,
     COM_USER_COLUMN,
     REC_BIN64_SIGNED,
     8,
     FALSE,
     SQLCHARSETCODE_UNKNOWN,
     0,
     0,
     0,
     0,
     0,
     0,
     0,
     COM_NO_DEFAULT,
     "",
     NULL,
     NULL,
     COM_UNKNOWN_DIRECTION_LIT,
     0}};

static const ComTdbVirtTableKeyInfo queryInvalidateVirtTableKeyInfo[] = {
    // indexname keyseqnumber tablecolnumber ordering
    {NULL, 1, 0, 0, 0, NULL, NULL}};

class ComTdbQryInvalid : public ComTdb {
  friend class ExQryInvalidStatsTcb;

 public:
  ComTdbQryInvalid();

  // Constructor used by the generator.
  ComTdbQryInvalid(ULng32 tupleLen, ULng32 returnedTuplelen, ULng32 inputTuplelen, ex_cri_desc *criDescParentDown,
                   ex_cri_desc *criDescParentUp, queue_index queueSizeDown, queue_index queueSizeUp, Lng32 numBuffers,
                   ULng32 bufferSize, ex_expr *scanExpr, ex_expr *inputExpr, ex_expr *projExpr,
                   ex_cri_desc *workCriDesc, UInt16 qi_row_atp_index, UInt16 input_row_atp_index);

  Int32 orderedQueueProtocol() const { return -1; };

  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbQryInvalid); }

  // Pack and Unpack routines
  Long pack(void *);
  Lng32 unpack(void *, void *reallocator);

  // For the GUI, Does nothing right now
  void display() const {};

  UInt32 getTupleLength() const { return tupleLen_; };
  UInt32 getReturnedTupleLength() const { return returnedTupleLen_; };
  UInt32 getInputTupleLength() const { return inputTupleLen_; };

  UInt16 getQiTupleAtpIndex() const { return qiTupleAtpIndex_; };
  UInt16 getInputTupleAtpIndex() const { return inputTupleAtpIndex_; };

  inline ex_expr *getScanExpr() const { return scanExpr_; };

  inline ex_expr *getInputExpr() const { return inputExpr_; };

  // Virtual routines to provide a consistent interface to TDB's

  virtual const ComTdb *getChild(Int32 /*child*/) const { return NULL; };

  // numChildren always returns 0 for ComTdbQryInvalid
  virtual Int32 numChildren() const { return 0; };

  virtual const char *getNodeName() const { return "EX_QRY_INVALID"; };

  // numExpressions always returns 2 for ComTdbQryInvalid
  virtual Int32 numExpressions() const { return 2; };

  // The names of the expressions
  virtual const char *getExpressionName(Int32) const;

  // The expressions themselves
  virtual ex_expr *getExpressionNode(Int32);

  static Int32 getVirtTableNumCols() {
    return sizeof(queryInvalidateVirtTableColumnInfo) / sizeof(ComTdbVirtTableColumnInfo);
  }

  static ComTdbVirtTableColumnInfo *getVirtTableColumnInfo() {
    return (ComTdbVirtTableColumnInfo *)queryInvalidateVirtTableColumnInfo;
  }

  static Int32 getVirtTableNumKeys() {
    return sizeof(queryInvalidateVirtTableKeyInfo) / sizeof(ComTdbVirtTableKeyInfo);
  }

  static ComTdbVirtTableKeyInfo *getVirtTableKeyInfo() {
    return (ComTdbVirtTableKeyInfo *)queryInvalidateVirtTableKeyInfo;
  }

 protected:
  ExExprPtr scanExpr_;  // 00-07
  ExExprPtr projExpr_;  // 08-15

  ExExprPtr inputExpr_;  // 16-23

  // Length of qi tuple to be allocated
  Int32 tupleLen_;  // 24-27

  Int32 returnedTupleLen_;  // 28-31

  Int32 inputTupleLen_;  // 32-35

  Int32 filler0ComTdbQryInvalid_;  // 36-39 unused

  ExCriDescPtr workCriDesc_;  // 40-47

  UInt16 qiTupleAtpIndex_;  // 48-49

  // position in workAtp where input row will be created.
  UInt16 inputTupleAtpIndex_;  // 50-51

  char fillersComTdbQryInvalid_[44];  // 52-95 unused

 private:
  inline Attributes *getAttrModName();
  inline Attributes *getAttrStmtName();
};

inline Attributes *ComTdbQryInvalid::getAttrModName() {
  // The moduleName is the first attribute in the tuple.
  return workCriDesc_->getTupleDescriptor(getInputTupleAtpIndex())->getAttr(0);
};

inline Attributes *ComTdbQryInvalid::getAttrStmtName() {
  // The statement Pattern is the second attribute in the tuple.
  return workCriDesc_->getTupleDescriptor(getInputTupleAtpIndex())->getAttr(1);
};

#endif
