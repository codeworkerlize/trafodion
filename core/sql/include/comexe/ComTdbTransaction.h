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
* File:         ComTdbTransaction.h
* Description:
*
* Created:      5/6/98
* Language:     C++
*
*
*
*
****************************************************************************
*/

#ifndef COM_TRANSACTION_H
#define COM_TRANSACTION_H

#include "comexe/ComTdb.h"
#include "common/ComTransInfo.h"

class ComDiagsArea;
class ComCondition;

///////////////////////////////////////////////////////
// class ComTdbTransaction
///////////////////////////////////////////////////////
class ComTdbTransaction : public ComTdb {
  friend class ExTransTcb;
  friend class ExTransPrivateState;

 public:
  ComTdbTransaction() : ComTdb(ComTdb::ex_TRANSACTION, eye_TRANSACTION), flags_(0){};

  ComTdbTransaction(TransStmtType trans_type, TransMode *trans_mode, ex_expr *diag_area_size_expr,
                    ex_cri_desc *work_cri_desc, ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
                    queue_index down, queue_index up, int num_buffers, ULng32 buffer_size);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbTransaction); }

  Long pack(void *);
  int unpack(void *, void *reallocator);

  int orderedQueueProtocol() const { return -1; };

  virtual const ComTdb *getChild(int /*child*/) const { return NULL; };
  virtual int numChildren() const { return 0; };
  virtual const char *getNodeName() const { return "EX_TRANSACTION"; };
  virtual int numExpressions() const { return 1; };
  virtual const char *getExpressionName(int) const { return "diagAreaSizeExpr_"; };
  virtual ex_expr *getExpressionNode(int) { return diagAreaSizeExpr_; };

  NABoolean setAllowedInXn() { return (flags_ & SET_ALLOWED_IN_XN) != 0; }
  void setSetAllowedInXn(NABoolean v) { (v ? flags_ |= SET_ALLOWED_IN_XN : flags_ &= ~SET_ALLOWED_IN_XN); }

  NABoolean coverageSavePoint() { return (flags_ & SET_SAVEPOINT_COVERED) != 0; }
  void setCoverageSavePoint(NABoolean v) { (v ? flags_ |= SET_SAVEPOINT_COVERED : flags_ &= ~SET_SAVEPOINT_COVERED); }

  TransMode *getTransMode() { return transMode_; }

  void setSavepointName(const char *svptName) {
    if (svptName == NULL) {
      savepointName_[0] = '\0';
      return;
    }

    int len = strlen(svptName);
    if (len > sizeof(savepointName_) - 1) len = sizeof(savepointName_) - 1;
    strncpy(savepointName_, svptName, len);
    hasSavepointName_ = 1;
  }

 protected:
  // transaction mode specified for the SQL SET TRANSACTION statement.
  // Valid when transType_ is SET_TRANSACTION_.
  TransModePtr transMode_;  // 00-07

  // expression used to compute the size of diagnostic area
  ExExprPtr diagAreaSizeExpr_;  // 08-15

  ExCriDescPtr workCriDesc_;  // 16-23

  // See ComTransInfo.h in common directory. (TransStmtType)
  Int16 transType_;  // 24-25

  UInt16 flags_;  // 26-27

  Int16 hasSavepointName_;  // 28-29

  char savepointName_[33];  // 30-63

  char fillersComTdbTransaction_[8];  // 64-71

 private:
  enum Flags { SET_ALLOWED_IN_XN = 0x0001, SET_SAVEPOINT_COVERED = 0x0020 };
};

#endif
