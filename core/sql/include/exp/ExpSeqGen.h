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
#ifndef EXP_SEQ_GEN_EXPR_H
#define EXP_SEQ_GEN_EXPR_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExpSeqGen.h
 * Description:
 *
 *
 * Created:      7/20/2014
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/SequenceGeneratorAttributes.h"
#include "comexe/ComQueue.h"
#include "exp/ExpHbaseInterface.h"

#define ORDER_SEQ_MD_TABLE     "ESG_TRAFODION._ORDER_SG_.ORDER_SEQ_GEN"
#define ORDER_SEQ_DEFAULT_QUAL "NextVal"

class SeqGenEntry : public NABasicObject {
 public:
  SeqGenEntry(long sgUID, CollHeap *heap);
  ~SeqGenEntry();

  short getNextSeqVal(SequenceGeneratorAttributes &sga, long &seqVal);
  short getCurrSeqVal(SequenceGeneratorAttributes &sga, long &seqVal);

  short getNextSeqValOrder(SequenceGeneratorAttributes &sga, long &seqVal);
  short getCurrSeqValOrder(SequenceGeneratorAttributes &sga, long &seqVal);

  short validateSeqValOrder(SequenceGeneratorAttributes &sga, long &nextVal);

  long getSGObjectUID() { return sgUID_; }

  void setRetryNum(UInt32 n) { retryNum_ = n; }
  UInt32 getRetryNum() { return retryNum_; }

  void setUseDlockImpl(NABoolean v) { (v) ? flags_ |= USE_DLOCK_IMPL : flags_ &= ~USE_DLOCK_IMPL; }
  NABoolean useDlockImpl() { return ((flags_ & USE_DLOCK_IMPL) != 0); }

  void setUseDtmImpl(NABoolean v) { (v) ? flags_ |= USE_DTM_IMPL : flags_ &= ~USE_DTM_IMPL; }
  NABoolean useDtmImpl() { return ((flags_ & USE_DTM_IMPL) != 0); }

  void setFetchNewRange(NABoolean v) { (v) ? flags_ |= FETCH_NEW_RANGE : flags_ &= ~FETCH_NEW_RANGE; }
  NABoolean getFetchNewRange() { return ((flags_ & FETCH_NEW_RANGE) != 0); }

  void setSkipWalForIncrColVal(NABoolean v) { (v) ? flags_ |= SKIP_WAL_INCRCOLVAL : flags_ &= ~SKIP_WAL_INCRCOLVAL; }
  NABoolean isSkipWalForIncrColVal() { return ((flags_ & SKIP_WAL_INCRCOLVAL) != 0); }
  /*
  void setUseDlockImpl(NABoolean v) { useDlockImpl_ = v; }
  NABoolean useDlockImpl() { return useDlockImpl_ ; }
  */
 private:
  enum {
    USE_DLOCK_IMPL = 0x0001,
    USE_DTM_IMPL = 0x0002,
    FETCH_NEW_RANGE = 0x0004,
    SKIP_WAL_INCRCOLVAL = 0x0008,
  };

  short fetchNewRange(SequenceGeneratorAttributes &inSGA);

  CollHeap *heap_;

  long sgUID_;

  //  NABoolean fetchNewRange_;
  //  NABoolean useDlockImpl_;
  long cachedStartValue_;
  long cachedEndValue_;
  long cachedCurrValue_;
  long redefTime_;

  void *cliInterfaceArr_;

  ExpHbaseInterface *ehi_;

  UInt32 retryNum_;

  UInt32 flags_;
};

class SequenceValueGenerator : public NABasicObject {
 public:
  SequenceValueGenerator(CollHeap *heap);
  SeqGenEntry *getEntry(SequenceGeneratorAttributes &sga);
  short getNextSeqVal(SequenceGeneratorAttributes &sga, long &seqVal);
  short getCurrSeqVal(SequenceGeneratorAttributes &sga, long &seqVal);
  short getIdtmSeqVal(SequenceGeneratorAttributes &sga, long &seqVal, ComDiagsArea **diags);
  HashQueue *sgQueue() { return sgQueue_; }
  CollHeap *getHeap() { return heap_; }
  void setRetryNum(UInt32 n) { retryNum_ = n; }
  UInt32 getRetryNum() { return retryNum_; }

  void setUseDlockImpl(NABoolean v) { (v) ? flags_ |= USE_DLOCK_IMPL : flags_ &= ~USE_DLOCK_IMPL; }
  NABoolean useDlockImpl() { return ((flags_ & USE_DLOCK_IMPL) != 0); }

  void setUseDtmImpl(NABoolean v) { (v) ? flags_ |= USE_DTM_IMPL : flags_ &= ~USE_DTM_IMPL; }
  NABoolean useDtmImpl() { return ((flags_ & USE_DTM_IMPL) != 0); }

  void setSkipWalForIncrColVal(NABoolean v) { (v) ? flags_ |= SKIP_WAL_INCRCOLVAL : flags_ &= ~SKIP_WAL_INCRCOLVAL; }
  NABoolean isSkipWalForIncrColVal() { return ((flags_ & SKIP_WAL_INCRCOLVAL) != 0); }

  /*
  void setUseDlockImpl(NABoolean v) { useDlockImpl_ = v; }
  NABoolean useDlockImpl() { return useDlockImpl_; }
  */

 private:
  enum {
    USE_DLOCK_IMPL = 0x0001,
    USE_DTM_IMPL = 0x0002,
    SKIP_WAL_INCRCOLVAL = 0x0004,
  };

  CollHeap *heap_;

  HashQueue *sgQueue_;

  UInt32 retryNum_;

  UInt32 flags_;
  //  NABoolean useDlockImpl_;
};

#endif
