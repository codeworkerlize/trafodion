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
#ifndef EXT_PUSHDOWNPRED_INFO_H
#define EXT_PUSHDOWNPRED_INFO_H

/* -*-C++-*-
******************************************************************************
*
* File:         orcPushdownPredInfo.h
* Description:  Definition of class ExtPushdownPredInfo and
*               ExtPushdownPredInfoList
*
* Created:      1/6/2016
* Language:     C++
* Status:       $State: Exp $
*
*
******************************************************************************
*/

#include "common/CollHeap.h"
#include "common/Collections.h"
#include "common/ComSmallDefs.h"
#include "common/NAString.h"
#include "common/CmpCommon.h"
#include "optimizer/ValueDesc.h"

// this class is used to  handle predicates that are pushdown to
// external storage (orc, parquet, avro)
class ExtPushdownPredInfo {
 public:
  ExtPushdownPredInfo(enum ExtPushdownOperatorType type, const ValueId &colValId, const ValueId &operValId)
      : type_(type), colValId_(colValId), operValId_(operValId), filterId_(extractFilterId(operValId)) {}

  ExtPushdownPredInfo(enum ExtPushdownOperatorType type, const ValueId &colValId)
      : type_(type), colValId_(colValId), filterId_(-1) {}

  ExtPushdownPredInfo(enum ExtPushdownOperatorType type, const ValueId &colValId, const ValueIdList &operValIdList)
      : type_(type), colValId_(colValId), operValIdList_(operValIdList), filterId_(-1) {}

  ExtPushdownPredInfo(enum ExtPushdownOperatorType type = UNKNOWN_OPER) : type_(type), filterId_(-1) {}

  enum ExtPushdownOperatorType getType() { return type_; }
  ValueId &colValId() { return colValId_; }
  ValueId &operValId() { return operValId_; }
  ValueIdList &operValIdList() { return operValIdList_; }
  Int16 getFilterId() { return filterId_; }

  NAString getText();

  void display();

 protected:
  Int16 extractFilterId(const ValueId &operValId);

 private:
  enum ExtPushdownOperatorType type_;
  ValueId colValId_;
  ValueId operValId_;
  Int16 filterId_;
  ValueIdList operValIdList_;
};

class ExtPushdownPredInfoList : public NAList<ExtPushdownPredInfo> {
 public:
  ExtPushdownPredInfoList(Lng32 ct = 0, CollHeap *heap = CmpCommon::statementHeap())
      : NAList<ExtPushdownPredInfo>(heap, ct){};

  ~ExtPushdownPredInfoList(){};

  void insertStartAND();
  void insertStartOR();
  void insertStartNOT();
  void insertEND();
  void insertIN(const ValueId &col, const ValueId &val);
  void insertIN(const ValueId &col, const ValueIdList &val);
  void insertEQ(const ValueId &col, const ValueId &val);
  void insertLESS(const ValueId &col, const ValueId &val);
  void insertLESS_EQ(const ValueId &col, const ValueId &val);
  void insertIS_NULL(const ValueId &col);

  NAString getText();

  void display();

  // returns: TRUE, if cannot be pushed down.
  NABoolean validatePushdownForParquet();

  // returns: TRUE, if cannot be pushed down.
  NABoolean validatePushdownForOrc();

  // returns: TRUE, if all operators can be handled.
  NABoolean validateOperatorsForParquetCppReader();
};

#endif
