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
 *****************************************************************************
 *
 * File:         ExpSeqGen.cpp
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

#include "common/Platform.h"
#include "cli/SQLCLIdev.h"
#include "exp/ExpSeqGen.h"
#include "export/ComDiags.h"
#include "executor/ex_error.h"
#include "seabed/ms.h"

//**************************************************************************
// class SeqGenEntry
//**************************************************************************
SeqGenEntry::SeqGenEntry(long sgUID, CollHeap *heap) : heap_(heap), sgUID_(sgUID), flags_(0) {
  cliInterfaceArr_ = NULL;
  retryNum_ = 100;  // default retry times
  ehi_ = NULL;
}

SeqGenEntry::~SeqGenEntry() {
  if (ehi_) NADELETE(ehi_, ExpHbaseInterface, heap_);
  ehi_ = NULL;
}

short SeqGenEntry::fetchNewRange(SequenceGeneratorAttributes &inSGA) {
  int cliRC = 0;

  // fetch new range from Seq Generator database
  SequenceGeneratorAttributes sga;
  sga = inSGA;
  if (sga.getSGCache() == 0) sga.setSGCache(1);

  sga.setSGRetryNum(getRetryNum());
  sga.setSGUseDlockImpl(useDlockImpl());
  sga.setSGUseDtmImpl(useDtmImpl());

  cliRC = SQL_EXEC_SeqGenCliInterface(&cliInterfaceArr_, &sga);
  if (cliRC < 0) return (short)cliRC;

  cachedStartValue_ = sga.getSGNextValue();
  cachedCurrValue_ = cachedStartValue_;
  cachedEndValue_ = sga.getSGEndValue();

  if (cachedStartValue_ > sga.getSGMaxValue()) {
    return -1579;  // max reached
  }

  setFetchNewRange(FALSE);

  return 0;
}

short SeqGenEntry::getNextSeqVal(SequenceGeneratorAttributes &sga, long &seqVal) {
  short rc = 0;

  if (redefTime_ != sga.getSGRedefTime()) {
    redefTime_ = sga.getSGRedefTime();
    setFetchNewRange(TRUE);
  }

  if (NOT getFetchNewRange()) {
    cachedCurrValue_ += sga.getSGIncrement();
    if (cachedCurrValue_ > cachedEndValue_) setFetchNewRange(TRUE);
  }

  if (getFetchNewRange()) {
    rc = fetchNewRange(sga);
    if (rc) return rc;
  }

  seqVal = cachedCurrValue_;

  return 0;
}

short SeqGenEntry::getNextSeqValOrder(SequenceGeneratorAttributes &sga, long &seqVal) {
  short ret = 0;
  if (ehi_ == NULL) ehi_ = ExpHbaseInterface::newInstance(heap_, (char *)"", (char *)"", COM_STORAGE_HBASE, FALSE);
  // Assert
  if (ehi_->init(NULL) != HBASE_ACCESS_SUCCESS) return -1;

  NAString tabName(ORDER_SEQ_MD_TABLE);
  NAString famName(SEABASE_DEFAULT_COL_FAMILY);
  NAString qualName(ORDER_SEQ_DEFAULT_QUAL);
  NAString rowId = Int64ToNAString(sga.getSGObjectUID().get_value());

  ret = ehi_->getNextValue(tabName, rowId, famName, qualName, sga.getSGIncrement(), seqVal, isSkipWalForIncrColVal());
  if (seqVal == sga.getSGIncrement())  // sequence id does not exist
  {
    ret = -1582;
    ehi_->deleteSeqRow(tabName, rowId);
  }
  ehi_->close();

  if (ret < 0) return ret;

  return 0;
}

short SeqGenEntry::getCurrSeqValOrder(SequenceGeneratorAttributes &sga, long &seqVal) {
  short ret = 0;
  if (ehi_ == NULL) ehi_ = ExpHbaseInterface::newInstance(heap_, (char *)"", (char *)"", COM_STORAGE_HBASE, FALSE);
  // Assert
  if (ehi_->init(NULL) != HBASE_ACCESS_SUCCESS) return -1;

  NAString tabName(ORDER_SEQ_MD_TABLE);
  NAString famName(SEABASE_DEFAULT_COL_FAMILY);
  NAString qualName(ORDER_SEQ_DEFAULT_QUAL);
  NAString rowId = Int64ToNAString(sga.getSGObjectUID().get_value());

  ret = ehi_->getNextValue(tabName, rowId, famName, qualName, 0, seqVal, isSkipWalForIncrColVal());
  if (seqVal == 0) ret = -1582;

  ehi_->close();

  if (ret < 0) return ret;

  return 0;
}

short SeqGenEntry::getCurrSeqVal(SequenceGeneratorAttributes &sga, long &seqVal) {
  short rc = 0;

  if (redefTime_ != sga.getSGRedefTime()) {
    redefTime_ = sga.getSGRedefTime();
    setFetchNewRange(TRUE);
  }

  if (getFetchNewRange()) {
    rc = fetchNewRange(sga);
    if (rc) return rc;
  }

  seqVal = cachedCurrValue_;

  return 0;
}

short SeqGenEntry::validateSeqValOrder(SequenceGeneratorAttributes &sga, long &seqVal) {
  long seqValTmp = seqVal;
  long maxVal = sga.getSGMaxValue();
  long minVal = sga.getSGMinValue();
  long modOp = maxVal - minVal + 1;
  int cliRC = 0;

  if (((seqVal > maxVal) || (seqVal < 0)) && (!sga.getSGCycleOption())) {
    return -1579;
  }

  if (seqVal > maxVal) {
    if (sga.getSGCycleOption()) {
      long valueToAdd = modOp - (maxVal + 1) % (modOp);
      seqVal = (seqValTmp + valueToAdd) % modOp + minVal;
    }
  }
  long cache = sga.getSGCache();
  if (cache == 0)  // no cache
    cache = 1;
  long xdcInterval = sga.getSGIncrement() * cache;
  if ((seqVal % xdcInterval) == 0) {
    long endValue = seqVal + xdcInterval + sga.getSGIncrement();
    cliRC = SQL_EXEC_OrderSeqXDCCliInterface(&cliInterfaceArr_, &sga, endValue);
  }
  return cliRC;
}

SequenceValueGenerator::SequenceValueGenerator(CollHeap *heap) : heap_(heap), flags_(0) {
  sgQueue_ = new (heap_) HashQueue(heap);
}

SeqGenEntry *SequenceValueGenerator::getEntry(SequenceGeneratorAttributes &sga) {
  long hashVal = sga.getSGObjectUID().get_value();

  sgQueue()->position((char *)&hashVal, sizeof(hashVal));

  SeqGenEntry *sge = NULL;
  while ((sge = (SeqGenEntry *)sgQueue()->getNext()) != NULL) {
    if (sge->getSGObjectUID() == hashVal) break;
  }

  if (!sge) {
    sge = new (getHeap()) SeqGenEntry(hashVal, getHeap());
    sgQueue()->insert((char *)&hashVal, sizeof(hashVal), sge);
  }

  sge->setRetryNum(getRetryNum());
  sge->setUseDlockImpl(useDlockImpl());
  sge->setSkipWalForIncrColVal(isSkipWalForIncrColVal());
  sge->setUseDtmImpl(useDtmImpl());

  return sge;
}

short SequenceValueGenerator::getNextSeqVal(SequenceGeneratorAttributes &sga, long &seqVal) {
  short ret = 0;
  SeqGenEntry *sge = getEntry(sga);
  if (sga.getSGOrder()) {
    ret = sge->getNextSeqValOrder(sga, seqVal);
    if (ret == 0) ret = sge->validateSeqValOrder(sga, seqVal);
    return ret;
  } else
    return sge->getNextSeqVal(sga, seqVal);
}

short SequenceValueGenerator::getCurrSeqVal(SequenceGeneratorAttributes &sga, long &seqVal) {
  short ret = 0;
  SeqGenEntry *sge = getEntry(sga);
  if (sga.getSGOrder()) {
    ret = sge->getCurrSeqValOrder(sga, seqVal);
    if (ret == 0) ret = sge->validateSeqValOrder(sga, seqVal);
    return ret;
  } else
    return sge->getCurrSeqVal(sga, seqVal);
}

short SequenceValueGenerator::getIdtmSeqVal(SequenceGeneratorAttributes &sga, long &seqVal, ComDiagsArea **diags) {
  Int32 retcode = 0;
  short rc = 0;
  //  system wide unique id option is set, call interface to get the next value from idtmsrv
  Int32 trycount = 0;
  long idtm_nextVal = 0;
  long timeout = sga.getSGTimeout();
  // For debugging only. Remove after perf tests are complete
  // If timeout value is set as -2, then return without calling idtmserver
  if (timeout == -2) {
    seqVal = 1000;
    return rc;
  }
  while (trycount < 5) {
    retcode = msg_seqid_get_id(&idtm_nextVal, timeout);
    if (retcode)
      trycount++;
    else
      break;
  }
  if (retcode) {
    // error indicating problem with idtmsrv
    rc = -1580;
    ExRaiseSqlError(heap_, diags, rc);
    *(*diags) << DgInt0(retcode) << DgString0("next unique sequence value ");

    return rc;
  } else
    seqVal = idtm_nextVal;
  return rc;
}
