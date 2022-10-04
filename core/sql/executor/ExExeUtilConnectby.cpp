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
 * File:         ExExeUtilConnectby.cpp
 * Description:
 *
 *
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include <iostream>
using std::cerr;
using std::endl;

#include <fstream>
using std::ofstream;

#include <stdio.h>

#include "common/ComCextdecs.h"
#include "cli_stdh.h"
#include "ex_stdh.h"
#include "sql_id.h"
#include "ex_transaction.h"
#include "ComTdb.h"
#include "ex_tcb.h"
#include "ComSqlId.h"

#include "ExExeUtil.h"
#include "ex_exe_stmt_globals.h"
#include "exp_expr.h"
#include "exp_clause_derived.h"
#include "ComRtUtils.h"
#include "ExStats.h"
#include "exp/ExpLOBenums.h"
#include "ExpLOBinterface.h"
#include "str.h"
#include "exp/ExpHbaseInterface.h"
#include "ExHbaseAccess.h"
#include "exp/ExpErrorEnums.h"

class rootItem {
 public:
  rootItem(CollHeap *h) { h_ = h; }
  ~rootItem() {}
  int rootId;
  Queue *qptr;
  CollHeap *h_;

  void cleanup() {
    if (qptr == NULL) return;
    for (int i = 0; i < qptr->entries(); i++) {
      connectByStackItem *it = (connectByStackItem *)qptr->get(i);
      it->cleanup();
      NADELETE(it, connectByStackItem, h_);
    }
    NADELETE(qptr, Queue, h_);
  }
};

ExExeUtilConnectbyTcb::ExExeUtilConnectbyTcb(const ComTdbExeUtilConnectby &exe_util_tdb, ex_globals *glob)
    : ExExeUtilTcb(exe_util_tdb, NULL, glob), step_(INITIAL_) {
  Space *space = (glob ? glob->getSpace() : 0);
  CollHeap *heap = (glob ? glob->getDefaultHeap() : 0);
  qparent_.down->allocatePstate(this);
  pool_->get_free_tuple(tuppData_, exe_util_tdb.tupleLen_);
  data_ = tuppData_.getDataPointer();
  currLevel_ = 0;
  resultSize_ = 0;
  currQueue_ = NULL;
  thisQueue_ = NULL;
  prevQueue_ = NULL;
  tmpPrevQueue_ = NULL;
  currRootId_ = 0;
  connBatchSize_ = CONNECT_BY_DEFAULT_BATCH_SIZE;
  currSeedNum_ = 0;
  upQueueIsFull_ = 0;
  resendIt_ = NULL;
  inputDynParamBuf_ = NULL;
  if (exe_util_tdb.inputExpr_) {
    inputDynParamBuf_ = new (glob->getDefaultHeap()) char[exe_util_tdb.inputRowlen_];
  }

  for (int i = 0; i < CONNECT_BY_MAX_LEVEL_SIZE; i++) {
    currArray[i] = NULL;
  }

  resultCache_ = NULL;
  cacheptr_ = 0;
  if (exeUtilTdb().scanExpr_) exeUtilTdb().scanExpr_->fixup(0, getExpressionMode(), this, space, heap, FALSE, glob);
  if (exeUtilTdb().startwith_expr_)
    exeUtilTdb().startwith_expr_->fixup(0, getExpressionMode(), this, space, heap, FALSE, glob);
  pool_->get_free_tuple(workAtp_->getTupp(((ComTdbExeUtilConnectby &)exe_util_tdb).dynParamTuppIndex), 0);
}

ex_tcb_private_state *ExExeUtilConnectbyTcb::allocatePstates(Lng32 &numElems,      // inout, desired/actual elements
                                                             Lng32 &pstateLength)  // out, length of one element
{
  PstateAllocator<ExExeUtilConnectbyTdbState> pa;

  return pa.allocatePstates(this, numElems, pstateLength);
}

short ExExeUtilConnectbyTcb::emitCacheRow(char *ptr) {
  short rc = 0;
  return moveRowToUpQueue(ptr, exeUtilTdb().tupleLen_, &rc, FALSE);
}

short ExExeUtilConnectbyTcb::emitRow(ExpTupleDesc *tDesc, int level, int isleaf, int iscycle, connectByStackItem *vi,
                                     NABoolean chkForStartWith) {
  short retcode = 0, rc = 0;
  char *ptr;
  Lng32 len;
  short nullind = 0;
  short *pn = &nullind;
  short **ind = &pn;
  UInt32 vcActualLen = 0;
  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();

  if (exeUtilTdb().isDual() == TRUE) {
    for (UInt32 i = 0; i < tDesc->numAttrs(); i++) {
      short srcType = REC_BIN32_UNSIGNED;
      Lng32 srcLen = 4;
      int *src = &currLevel_;
      Attributes *attr = tDesc->getAttr(i);
      ::convDoIt((char *)src, srcLen, srcType, 0, 0, &data_[attr->getOffset()], attr->getLength(), attr->getDatatype(),
                 0, 0, NULL, 0, NULL);
    }

    // apply the expression
    ex_expr::exp_return_type evalRetCode = ex_expr::EXPR_TRUE;
    if (exeUtilTdb().scanExpr_) {
      workAtp_->getTupp(exeUtilTdb().sourceDataTuppIndex_).setDataPointer((char *)data_);
      evalRetCode = exeUtilTdb().scanExpr_->eval(pentry_down->getAtp(), workAtp_);
    }

    if (evalRetCode == ex_expr::EXPR_TRUE) {
      retcode = moveRowToUpQueue(data_, exeUtilTdb().tupleLen_, &rc, FALSE);
      return retcode;
    } else
      return 1;
  }

  for (UInt32 i = 2; i < tDesc->numAttrs() - 1; i++)
  // bypass first two columns and ignore the last three system columns
  // but the last column is 'LEVEL"
  {
    cliInterface()->getPtrAndLen(i + 1, ptr, len, ind);

    char *src = ptr;
    Attributes *attr = tDesc->getAttr(i - 2);
    short srcType = 0;
    Lng32 srcLen;
    short valIsNull = 0;
    srcType = attr->getDatatype();
    srcLen = len;
    if ((*ind != NULL) && (((char *)*ind)[0] == -1)) valIsNull = -1;
    if (len == 0) valIsNull = -1;

    if (attr->getNullFlag()) {
      // target is nullable
      if (attr->getNullIndicatorLength() == 2) {
        // set the 2 byte NULL indicator to -1
        *(short *)(&data_[attr->getNullIndOffset()]) = valIsNull;
      } else {
        ex_assert(attr->getNullIndicatorLength() == 4, "NULL indicator must be 2 or 4 bytes");
        *(Lng32 *)(&data_[attr->getNullIndOffset()]) = valIsNull;
      }
    } else
      ex_assert(!valIsNull, "NULL source for NOT NULL stats column");

    if (!valIsNull) {
      if (attr->getVCIndicatorLength() > 0) {
        ::convDoIt(src, srcLen, srcType, 0, 0, &data_[attr->getOffset()], attr->getLength(), attr->getDatatype(), 0, 0,
                   (char *)&vcActualLen, sizeof(vcActualLen), NULL);
        attr->setVarLength(vcActualLen, &data_[attr->getVCLenIndOffset()]);
      } else
        ::convDoIt(src, srcLen, srcType, 0, 0, &data_[attr->getOffset()], attr->getLength(), attr->getDatatype(), 0, 0,
                   NULL, 0, NULL);
    }
  }

  short srcType = REC_BIN32_UNSIGNED;
  Lng32 srcLen = 4;
  int src = isleaf;
  Attributes *attr = tDesc->getAttr(tDesc->numAttrs() - 2);
  if (::convDoIt((char *)&src, srcLen, srcType, 0, 0, &data_[attr->getOffset()], attr->getLength(), attr->getDatatype(),
                 0, 0, 0, 0, NULL) != ex_expr::EXPR_OK) {
    ex_assert(0, "Error from ExStatsTcb::work::convDoIt.");
  }

  src = iscycle;
  attr = tDesc->getAttr(tDesc->numAttrs() - 3);
  if (::convDoIt((char *)&src, srcLen, srcType, 0, 0, &data_[attr->getOffset()], attr->getLength(), attr->getDatatype(),
                 0, 0, 0, 0, NULL) != ex_expr::EXPR_OK) {
    ex_assert(0, "Error from ExStatsTcb::work::convDoIt.");
  }

  ex_expr::exp_return_type evalRetCode = ex_expr::EXPR_TRUE;

  if (chkForStartWith == TRUE) {
    evalRetCode = ex_expr::EXPR_TRUE;
    if (exeUtilTdb().hasDynParamsInStartWith_ == TRUE) {
      if (exeUtilTdb().startwith_expr_) {
        workAtp_->getTupp(exeUtilTdb().sourceDataTuppIndex_).setDataPointer((char *)data_);
        evalRetCode = exeUtilTdb().startwith_expr_->eval(pentry_down->getAtp(), workAtp_);
      }
    }
    if (evalRetCode != ex_expr::EXPR_TRUE) return 300;
  }

  {
    // apply the expression
    if (exeUtilTdb().scanExpr_) {
      // evalRetCode = evalScanExpr((char*)data_,  exeUtilTdb().tupleLen_, FALSE);

      workAtp_->getTupp(exeUtilTdb().sourceDataTuppIndex_).setDataPointer((char *)data_);
      evalRetCode = exeUtilTdb().scanExpr_->eval(pentry_down->getAtp(), workAtp_);
    }
  }

  if (evalRetCode == ex_expr::EXPR_TRUE) {
    char pathBuf[CONNECT_BY_MAX_PATH_SIZE];
    memset(pathBuf, 0, CONNECT_BY_MAX_PATH_SIZE);
    char tmpbuf1[256];
    Lng32 pathlength = 0;
    Lng32 dlen = strlen(exeUtilTdb().delimiter_.data());
    Queue *pathq = NULL;
    if (exeUtilTdb().hasPath_ == TRUE) {
      pathq = new (getHeap()) Queue(getHeap());
      pathq->insert(vi->pathItem);
      pathlength = vi->pathLen;
      for (int ii = level; ii > 0; ii--) {
        Queue *tq = getCurrentQueue(ii - 1);
        connectByStackItem *p = (connectByStackItem *)tq->get(vi->parentId - 1);
        if (p == NULL) abort();
        pathq->insert(p->pathItem);
        pathlength += p->pathLen + dlen;
        vi = p;
      }
      for (int pi = pathq->entries(); pi > 0; pi--) {
        if (pi - 1 == 0)
          sprintf(tmpbuf1, "%s", (char *)(pathq->get(pi - 1)));
        else
          sprintf(tmpbuf1, "%s%s", (char *)(pathq->get(pi - 1)), exeUtilTdb().delimiter_.data());
        strcat(pathBuf, tmpbuf1);
      }
      NADELETE(pathq, Queue, getHeap());
    }

    attr = tDesc->getAttr(tDesc->numAttrs() - 1);

    if (pathlength > CONNECT_BY_MAX_PATH_SIZE) pathlength = CONNECT_BY_MAX_PATH_SIZE;

    if (::convDoIt(pathBuf, pathlength, REC_BYTE_V_ASCII, 0, 0, &data_[attr->getOffset()], attr->getLength(),
                   attr->getDatatype(), 0, 0, (char *)&vcActualLen, sizeof(vcActualLen), NULL) != ex_expr::EXPR_OK) {
      ex_assert(0, "Error from ExStatsTcb::work::convDoIt.");
    }
    attr->setVarLength(vcActualLen, &data_[attr->getVCLenIndOffset()]);
    retcode = moveRowToUpQueue(data_, exeUtilTdb().tupleLen_, &rc, FALSE);

    // save into cache
    if (exeUtilTdb().nodup_ == TRUE) {
      if (resultCache_ == NULL) {
        resultCache_ = new (getHeap()) Queue(getHeap());
      } else {
        char *dptr = new (getHeap()) char[exeUtilTdb().tupleLen_];
        memcpy(dptr, data_, exeUtilTdb().tupleLen_);
        resultCache_->insert(dptr);
      }
    }
  }
  return retcode;
}

short ExExeUtilConnectbyTcb::emitPrevRow(ExpTupleDesc *tDesc, int level, int isleaf, int iscycle, Queue *pq, int idx) {
  short retcode = 0, rc = 0;
  connectByStackItem *vi = NULL;
  char *ptr;
  Lng32 len;
  short nullInd = 0;
  short *ind = &nullInd;
  short **indadd = &ind;
  UInt32 vcActualLen = 0;
  // get the item
  Queue *thatone;
  int pos = 0, doffset = 0, lastpos = 0;
  for (int i1 = 0; i1 < pq->numEntries(); i1++) {
    lastpos = pos;
    pos += ((Queue *)pq->get(i1))->numEntries();
    if (idx < pos) {
      thatone = (Queue *)pq->get(i1);
      doffset = idx - lastpos;
      break;
    }
  }
  if (level > 1) vi = (connectByStackItem *)currArray[level - 1]->get(doffset);

  if (thatone == NULL) return 0;  // TODO

  OutputInfo *ti = (OutputInfo *)thatone->get(doffset);
  // tDesc->numAttrs() is the number of the colunms in vitual table
  // The virtual table has all columns same as original table
  // plus 4 new columns
  // This loop is to retrieve all original column value plus the "LEVEL"
  // So the total number of loop should be original table col num N
  // plus 1
  // Since the loop start with 2, so the end is tDesc->numAttrs() - 1
  for (UInt32 i = 2; i < tDesc->numAttrs() - 1; i++) {
    ti->get(i, ptr, len);
    char *src = ptr;
    Attributes *attr = tDesc->getAttr(i - 2);
    short srcType = 0;
    Lng32 srcLen;
    short valIsNull = 0;
    srcType = attr->getDatatype();
    srcLen = len;
    if (len == 0) valIsNull = -1;

    if (attr->getNullFlag()) {
      // target is nullable
      if (attr->getNullIndicatorLength() == 2) {
        // set the 2 byte NULL indicator to -1
        *(short *)(&data_[attr->getNullIndOffset()]) = valIsNull;
      } else {
        ex_assert(attr->getNullIndicatorLength() == 4, "NULL indicator must be 2 or 4 bytes");
        *(Lng32 *)(&data_[attr->getNullIndOffset()]) = valIsNull;
      }
    } else
      ex_assert(!valIsNull, "NULL source for NOT NULL stats column");

    if (!valIsNull) {
      if (attr->getVCIndicatorLength() > 0) {
        ::convDoIt(src, srcLen, srcType, 0, 0, &data_[attr->getOffset()], attr->getLength(), attr->getDatatype(), 0, 0,
                   (char *)&vcActualLen, sizeof(vcActualLen), NULL);
        attr->setVarLength(vcActualLen, &data_[attr->getVCLenIndOffset()]);
      } else
        ::convDoIt(src, srcLen, srcType, 0, 0, &data_[attr->getOffset()], attr->getLength(), attr->getDatatype(), 0, 0,
                   NULL, 0, NULL);
    }
  }

  short srcType = REC_BIN32_UNSIGNED;
  Lng32 srcLen = 4;
  int src = isleaf;
  Attributes *attr = tDesc->getAttr(tDesc->numAttrs() - 2);
  if (::convDoIt((char *)&src, srcLen, srcType, 0, 0, &data_[attr->getOffset()], attr->getLength(), attr->getDatatype(),
                 0, 0, 0, 0, NULL) != ex_expr::EXPR_OK) {
    ex_assert(0, "Error from ExStatsTcb::work::convDoIt.");
  }

  src = iscycle;
  attr = tDesc->getAttr(tDesc->numAttrs() - 3);
  if (::convDoIt((char *)&src, srcLen, srcType, 0, 0, &data_[attr->getOffset()], attr->getLength(), attr->getDatatype(),
                 0, 0, 0, 0, NULL) != ex_expr::EXPR_OK) {
    ex_assert(0, "Error from ExStatsTcb::work::convDoIt.");
  }

  // apply the expression
  ex_expr::exp_return_type evalRetCode = ex_expr::EXPR_TRUE;
  if (exeUtilTdb().scanExpr_) {
    ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
    workAtp_->getTupp(exeUtilTdb().sourceDataTuppIndex_).setDataPointer((char *)data_);
    evalRetCode = exeUtilTdb().scanExpr_->eval(pentry_down->getAtp(), workAtp_);
  }

  if (evalRetCode == ex_expr::EXPR_TRUE) {
    char pathBuf[CONNECT_BY_MAX_PATH_SIZE];
    memset(pathBuf, 0, CONNECT_BY_MAX_PATH_SIZE);
    char tmpbuf1[256];
    Lng32 pathlength = 0;
    Lng32 dlen = strlen(exeUtilTdb().delimiter_.data());
    Queue *pathq = new (getHeap()) Queue(getHeap());
    ;
    if (exeUtilTdb().hasPath_ == TRUE) {
      ti->get(1, ptr, len);
      pathq->insert(ptr);
      pathlength = len;
      for (int ii = level - 1; ii > 0; ii--) {
        Queue *tq = getCurrentQueue(ii - 1);
        connectByStackItem *p = (connectByStackItem *)tq->get(vi->parentId - 1);
        if (p == NULL) abort();
        pathq->insert(p->pathItem);
        pathlength += p->pathLen + dlen;
        vi = p;
      }
      for (int pi = pathq->entries(); pi > 0; pi--) {
        if (pi - 1 == 0)
          sprintf(tmpbuf1, "%s", (char *)(pathq->get(pi - 1)));
        else
          sprintf(tmpbuf1, "%s%s", (char *)(pathq->get(pi - 1)), exeUtilTdb().delimiter_.data());
        strcat(pathBuf, tmpbuf1);
      }
      NADELETE(pathq, Queue, getHeap());
    }

    attr = tDesc->getAttr(tDesc->numAttrs() - 1);

    if (pathlength > CONNECT_BY_MAX_PATH_SIZE) pathlength = CONNECT_BY_MAX_PATH_SIZE;

    if (::convDoIt(pathBuf, pathlength, REC_BYTE_V_ASCII, 0, 0, &data_[attr->getOffset()], attr->getLength(),
                   attr->getDatatype(), 0, 0, (char *)&vcActualLen, sizeof(vcActualLen), NULL) != ex_expr::EXPR_OK) {
      ex_assert(0, "Error from ExStatsTcb::work::convDoIt.");
    }
    attr->setVarLength(vcActualLen, &data_[attr->getVCLenIndOffset()]);
    retcode = moveRowToUpQueue(data_, exeUtilTdb().tupleLen_, &rc, FALSE);
  }
  return retcode;
}

void ExExeUtilConnectbyTcb::replaceStartWithStringWithValue(Queue *vals, const NAString sqlstr, char *out,
                                                            int newsize) {
  // search for ?
  string src(sqlstr.data());
  string dynp("?");
  char tmpbuf[newsize + 1];
  memset(tmpbuf, 0, newsize + 1);

  int i = 0;
  size_t pos = 0;
  size_t currpos = 0;
  size_t prev = 0;
  while (1) {
    currpos = src.find(dynp, pos);
    if (currpos == string::npos) break;

    if (i + 1 > vals->entries()) break;

    strcat(tmpbuf, src.substr(prev, currpos - prev).c_str());
    strcat(tmpbuf, " '");
    strcat(tmpbuf, (char *)vals->get(i));
    strcat(tmpbuf, "' ");

    pos = currpos + dynp.length();
    prev = pos;
    NADELETEBASIC((char *)vals->get(i), getHeap());
    i++;
  }
  strcat(tmpbuf, src.substr(prev, currpos - prev).c_str());

  strcpy(out, tmpbuf);

  // NADELETEBASIC(tmpbuf, getHeap());
}

void releaseCurrentQueue(Queue *q, CollHeap *h) {
  for (int i = 0; i < q->numEntries(); i++) {
    connectByStackItem *entry = (connectByStackItem *)q->get(i);
    entry->cleanup();
    NADELETE(entry, connectByStackItem, h);
  }
}

short haveDupSeed(Queue *q, connectByStackItem *it, int len, int level) {
  for (int i = 0; i < q->numEntries(); i++) {
    connectByStackItem *entry = (connectByStackItem *)q->get(i);
    if (entry == NULL) continue;
    if (len != entry->len) continue;
    if (memcmp(entry->seedValue, it->seedValue, len) == 0) {
      if (entry->level < level)
        return 1;
      else
        return -1;
    }
  }
  return 0;
}

short haveRootDupSeed(Queue *q, connectByStackItem *it, int len, int level) {
  short rc = 0;
  for (int i = 0; i < q->numEntries(); i++) {
    rootItem *entry = (rootItem *)q->get(i);
    if (entry == NULL) continue;
    rc = haveDupSeed(entry->qptr, it, len, level);
    if (rc == -1 || rc == 1) return rc;
  }
  return 0;
}

static short checkRootDup(connectByStackItem *it, int len, Queue *q) {
  short rc = 0;
  if (q == NULL) return rc;
  for (int i = 0; i < q->numEntries(); i++) {
    connectByStackItem *entry = (connectByStackItem *)q->get(i);
    if (entry == NULL) continue;
    if (len != entry->len) continue;
    if (memcmp(entry->seedValue, it->seedValue, len) == 0) {
      return 1;
    }
  }
  return rc;
}

short ExExeUtilConnectbyTcb::checkDuplicate(connectByStackItem *it, int len, int level) {
  short rc = 0;
  if (level == 1)  // the first level
  {
    Queue *q = currArray[0];
    if (q == NULL) return rc;
    rc = haveDupSeed(q, it, len, level);
    if (rc == -1 || rc == 1) return rc;
  } else {
    for (int i = 0; i < level - 1; i++) {
      Queue *q = currArray[i];
      if (q == NULL) continue;
      rc = haveDupSeed(q, it, len, level);
      if (rc == -1 || rc == 1) return rc;
    }
  }
  return rc;
}

Queue *getCurrQueue(int id, Queue *q) {
  for (int i = 0; i < q->numEntries(); i++) {
    rootItem *ri = (rootItem *)q->get(i);
    if (ri->rootId == id) return ri->qptr;
  }
  return NULL;
}

short ExExeUtilConnectbyTcb::work() {
  short retcode = 0;
  char q1[CONNECT_BY_MAX_SQL_TEXT_SIZE];  // TODO: the max len of supported query len
  void *uppderid = NULL;
  char *ptr, *ptr1;
  Lng32 len, len1;
  short rc;
  int tmpCounter = 0;
  NAString nq11, nq21;
  memset(q1, 0, sizeof(q1));
  int matchRowNum = 0;
  int specialFlag = 0;
  // if no parent request, return
  if (qparent_.down->isEmpty()) return WORK_OK;

  // if no room in up queue, won't be able to return data/status.
  // Come back later.
  if (qparent_.up->isFull()) return WORK_OK;

  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
  ex_queue_entry *up_entry = qparent_.up->getTailEntry();
  ex_tcb_private_state *pstate = pentry_down->pstate;

  // check for DUAL
  if (exeUtilTdb().isDual() == FALSE) {
    if (exeUtilTdb().hasStartWith_ == TRUE) {
      if (exeUtilTdb().hasDynParamsInStartWith_ != TRUE)
        sprintf(q1, "SELECT %s , cast (%s as VARCHAR(60) ), * , cast( 1 as INT) FROM %s WHERE %s ;",
                (exeUtilTdb().parentColName_).data(), (exeUtilTdb().pathColName_).data(),
                (exeUtilTdb().connTableName_).data(), (exeUtilTdb().startWithExprString_).data());
      else {
        sprintf(q1, "SELECT %s , cast (%s as VARCHAR(60) ), * , cast( 1 as INT) FROM %s ",
                (exeUtilTdb().parentColName_).data(), (exeUtilTdb().pathColName_).data(),
                (exeUtilTdb().connTableName_).data());
      }
      nq11 = q1;
    } else {
      sprintf(q1, "SELECT %s ,  cast (%s as VARCHAR(60) ) , * , cast(1 as INT)  FROM %s WHERE %s is null ;",
              (exeUtilTdb().parentColName_).data(), (exeUtilTdb().pathColName_).data(),
              (exeUtilTdb().connTableName_).data(), (exeUtilTdb().childColName_).data());
      nq11 = q1;
    }
  }
  ExpTupleDesc *tDesc = exeUtilTdb().workCriDesc_->getTupleDescriptor(exeUtilTdb().sourceDataTuppIndex_);

  Lng32 fsDatatype = 0;
  Lng32 length = 0;
  Lng32 vcIndLen = 0;
  Lng32 indOffset = 0;
  Lng32 varOffset = 0;

  // if (exeUtilTdb().hasPath_ == TRUE || exeUtilTdb().hasIsLeaf_ == TRUE || exeUtilTdb().orderSiblingsByCol_ != "")
  if (exeUtilTdb().hasPath_ == TRUE || exeUtilTdb().hasIsLeaf_ == TRUE) {
    connBatchSize_ = 1;
  }

  if (exeUtilTdb().hasIsLeaf_ == TRUE && prevQueue_ == NULL) {
    prevQueue_ = new (getHeap()) Queue(getHeap());  // when this delete? TODO
  }
  cachework_ = 0;
  while (1) {
    pentry_down = qparent_.down->getHeadEntry();
    if ((pentry_down->downState.request == ex_queue::GET_NOMORE) && (step_ != DONE_)) {
      step_ = DONE_;  // DONE;
    }
    switch (step_) {
      case INITIAL_:
        // if resultCache is not empty return now
        if (resultCache_ != NULL && exeUtilTdb().nodup_ == TRUE) {
          for (int i1 = cacheptr_; i1 < resultCache_->numEntries(); i1++) {
            // emitrow
            short rc1 = emitCacheRow((char *)resultCache_->get(i1));
            if (rc1 != 0)
              return rc1;
            else
              cacheptr_++;
          }

          cachework_ = 1;
          step_ = DONE_;
          break;
        }

        seedQueue_ = new (getHeap()) Queue(getHeap());
        if (exeUtilTdb().hasDynParamsInStartWith_ != TRUE)  // no input dyn param to handle
          step_ = EVAL_START_WITH_;
        else
          step_ = EVAL_INPUT_;

        currRootId_ = 0;
        currLevel_ = 0;
        nq21cnt_ = 0;
        if (exeUtilTdb().isDual() == TRUE) {
          step_ = DUAL_;
          currLevel_ = 1;
        }
        break;
      case EVAL_INPUT_:
        if (exeUtilTdb().hasDynParamsInStartWith_ == TRUE) {
          workAtp_->getTupp(exeUtilTdb().dynParamTuppIndex).setDataPointer(inputDynParamBuf_);
          ex_expr::exp_return_type exprRetCode = exeUtilTdb().inputExpr()->eval(pentry_down->getAtp(), workAtp_);
          if (exprRetCode == ex_expr::EXPR_ERROR) {
            step_ = ERROR_;
            break;
          }
          Queue *dynParamsVal = new (getHeap()) Queue(getHeap());
          ExpTupleDesc *tDescd = exeUtilTdb().workCriDesc_->getTupleDescriptor(exeUtilTdb().dynParamTuppIndex);
          int newsize = 0;
          for (UInt32 i = 0; i < tDescd->numAttrs(); i++) {
            Attributes *attr = tDescd->getAttr(i);
            char *tmpbuffer = new (getHeap()) char[attr->getLength() + 1];
            memset(tmpbuffer, 0, attr->getLength() + 1);
            int len = 0;
            if (attr->getVCIndicatorLength() == sizeof(short))
              len = *(short *)&inputDynParamBuf_[attr->getVCLenIndOffset()];
            else
              len = *(Lng32 *)&inputDynParamBuf_[attr->getVCLenIndOffset()];
            newsize += attr->getLength() + 1;
            // since dynparam already cast to visible string, so strncpy should be OK
            char *srcptr = inputDynParamBuf_ + attr->getOffset();
            strncpy(tmpbuffer, srcptr, len);
            dynParamsVal->insert(tmpbuffer);
          }
          newsize += exeUtilTdb().startWithExprString_.length() + 1;
          char newstr[newsize];  // = new (getHeap()) char[newsize];
          memset(newstr, 0, newsize);
          replaceStartWithStringWithValue(dynParamsVal, exeUtilTdb().startWithExprString_, newstr, newsize);
          nq11.append(" WHERE ");
          nq11.append(newstr);
          nq11.append(";");
          NADELETE(dynParamsVal, Queue, getHeap());
          // NADELETEBASIC(newstr, getHeap());
        }
        step_ = EVAL_START_WITH_;
        break;

      case DUAL_:
        rc = emitRow(tDesc, currLevel_, 0, 0, NULL, FALSE);
        if (rc == 1)  // DONE
        {
          step_ = DONE_;
          break;
        }
        currLevel_++;
        // to prevent endless loop give a big enough maxDeep_ value : 0.1 billion
        // If it really needs to iterate over 0.1 billion times
        // I will revisit this code
        if (currLevel_ > 100000000)  // exeUtilTdb().maxDeep_
        {
          ComDiagsArea *diags = getDiagsArea();
          if (diags == NULL) {
            setDiagsArea(ComDiagsArea::allocate(getHeap()));
            diags = getDiagsArea();
          }
          *diags << DgSqlCode(-EXE_CONNECT_BY_MAX_REC_ERROR);

          step_ = ERROR_;
        }
        break;
      case EVAL_START_WITH_: {
        short rc = 0;
        int rootId = 0;

        // get the stmt

        if (exeUtilTdb().hasIsLeaf_ == TRUE) {
          Queue *rootRow = new (getHeap()) Queue(getHeap());
          rc = cliInterface()->fetchAllRows(rootRow, nq11.data(), 0, FALSE, FALSE, TRUE);
          if (rc < 0) {
            NADELETE(rootRow, Queue, getHeap());
            return WORK_BAD_ERROR;
          }
          // populate the seedQueue and currArray
          rootRow->position();
          if (rootRow->numEntries() == 0) {
            step_ = DONE_;
            break;
          }

          for (int i1 = 0; i1 < rootRow->numEntries(); i1++) {
            OutputInfo *vi = (OutputInfo *)rootRow->getNext();
            vi->get(0, ptr, len, fsDatatype, NULL, NULL);
            connectByStackItem *it = new (getHeap()) connectByStackItem(getHeap());

            char *tmp = new (getHeap()) char[len];
            memcpy(tmp, ptr, len);
            it->seedValue = tmp;
            it->level = currLevel_;
            it->type = fsDatatype;
            it->len = len;
            it->parentId = -1;
            if (exeUtilTdb().hasPath_ == TRUE) {
              vi->get(1, ptr1, len1);
              char *tmp1 = new (getHeap()) char[len1 + 1];
              memset(tmp1, 0, len1 + 1);
              memcpy(tmp1, ptr1, len1);
              it->pathLen = len1;
              it->pathItem = tmp1;
            }
            if (checkDuplicate(it, len, currLevel_) == 0) {
              matchRowNum++;
              Queue *cq = new (getHeap()) Queue(getHeap());
              rootItem *ri = new (getHeap()) rootItem(getHeap());
              ri->rootId = rootId;
              rootId++;
              cq->insert(it);
              ri->qptr = cq;
              seedQueue_->insert(ri);
            }

            // populate prevQueue
            prevQueue_->insert(rootRow);
          }  // for(int i1 = 0; i1 < rootRow->numEntries(); i1 ++)
        } else {
          if (upQueueIsFull_ == 0) {
            retcode = cliInterface()->fetchRowsPrologue(nq11.data(), FALSE, FALSE);
          } else {
            // first resend
            short rc2 = emitRow(tDesc, currLevel_, 0, 0, resendIt_, FALSE);
            if (rc2 != 0) {
              return rc2;
            }
            upQueueIsFull_ = 0;
          }
          Queue *currRootQueue = new (getHeap()) Queue(getHeap());  // delete before go to next state
          Queue *cq = NULL;                                         // delete when seedQueue_ was destroyed
          rootItem *ri = NULL;                                      // delete when seedQueue_ was destroyed
          while ((rc >= 0) && (rc != 100)) {
            rc = cliInterface()->fetch();
            if (rc < 0) {
              cliInterface()->fetchRowsEpilogue(0, TRUE);
              return rc;
            }

            if (rc == 100) continue;

            connectByStackItem *it =
                new (getHeap()) connectByStackItem(getHeap());  // delete when seedQueue_ was destroyed
            cliInterface()->getPtrAndLen(1, ptr, len);
            cliInterface()->getAttributes(1, FALSE, fsDatatype, length, vcIndLen, &indOffset, &varOffset);

            char *tmp = new (getHeap()) char[len];  // delete when seedQueue_ was destroyed
            memcpy(tmp, ptr, len);
            it->seedValue = tmp;
            it->level = currLevel_;
            it->type = fsDatatype;
            it->len = len;
            it->parentId = -1;
            if (exeUtilTdb().hasPath_ == TRUE) {
              cliInterface()->getPtrAndLen(2, ptr1, len1);
              char *tmp1 = new (getHeap()) char[len1 + 1];  // delete when seedQueue_ was destroyed
              memset(tmp1, 0, len1 + 1);
              memcpy(tmp1, ptr1, len1);
              it->pathLen = len1;
              it->pathItem = tmp1;
            }

            if (checkDuplicate(it, len, currLevel_) == 0) {
              short rc2 = 0;
              if (exeUtilTdb().hasDynParamsInStartWith_ != TRUE)
                rc2 = emitRow(tDesc, currLevel_, 0, 0, it, FALSE);
              else {
                rc2 = emitRow(tDesc, currLevel_, 0, 0, it, FALSE);  // set last to TRUE for start with check
              }
              if (rc2 != 0 && rc2 != 300) {
                resendIt_ = it;
                upQueueIsFull_ = 1;
                return rc2;
              }
              if (checkRootDup(it, len, currRootQueue) == 0 && rc2 != 300) {
                if (exeUtilTdb().nodup_ == FALSE)  // remove dup
                {
                  cq = new (getHeap()) Queue(getHeap());     // delete when seedQueue_ was destroyed
                  ri = new (getHeap()) rootItem(getHeap());  // delete when seedQueue_ was destroyed
                  matchRowNum++;
                  cq->insert(it);
                  ri->rootId = rootId;
                  rootId++;
                  ri->qptr = cq;
                  seedQueue_->insert(ri);
                  currRootQueue->insert(it);  // TODO: refactor
                } else {
                  if (cq == NULL) {
                    cq = new (getHeap()) Queue(getHeap());  // delete when seedQueue_ was destroyed
                  }
                  cq->insert(it);
                  matchRowNum++;
                  currRootQueue->insert(it);
                }
              } else {  // release it
                it->cleanup();
                NADELETE(it, connectByStackItem, getHeap());
              }
            }
          }  // end of while to read result set
          if (exeUtilTdb().nodup_ == TRUE) {
            specialFlag = 1;
            ri = new (getHeap()) rootItem(getHeap());  // delete when seedQueue_ was destroyed
            ri->rootId = 0;
            ri->qptr = cq;
            seedQueue_->insert(ri);
          }
          // this currRootQueue is only used for dup detection
          // no need to free the it value in it
          // it value was in seedQueue_ and will be free when DONE_
          NADELETE(currRootQueue, Queue, getHeap());
          cliInterface()->fetchRowsEpilogue(0, FALSE);
        }

        if (seedQueue_->numEntries() == 0) {
          step_ = DONE_;
          break;
        }
        currQueue_ = getCurrQueue(currRootId_, seedQueue_);
        currArray[0] = currQueue_;

        if (matchRowNum > exeUtilTdb().maxSize_) {
          ComDiagsArea *diags = getDiagsArea();
          if (diags == NULL) {
            setDiagsArea(ComDiagsArea::allocate(getHeap()));
            diags = getDiagsArea();
          }
          *diags << DgSqlCode(-EXE_CONNECT_BY_MAX_MEM_ERROR);

          step_ = ERROR_;
        } else
          step_ = NEXT_LEVEL_;
      } break;
      case DO_CONNECT_BY_: {
        currQueue_ = getCurrentQueue(currLevel_ - 1);
        thisQueue_ = getCurrentQueue(currLevel_);
        if (exeUtilTdb().hasIsLeaf_ == TRUE) {
          tmpPrevQueue_ = new (getHeap()) Queue(getHeap());  // TODO: check later
        }
        Lng32 seedNum = currQueue_->numEntries();
        Int8 loopDetected = 0;

        currQueue_->position();
        resultSize_ = 0;

        for (int i = currSeedNum_; i < seedNum;) {
          sprintf(q1, "SELECT %s ,cast (%s as VARCHAR(60) ) , *, cast( %d as INT) FROM %s WHERE ",
                  (exeUtilTdb().parentColName_).data(), (exeUtilTdb().pathColName_).data(), currLevel_ + 1,
                  (exeUtilTdb().connTableName_).data());
          nq21 = q1;
          nq21.append((exeUtilTdb().childColName_).data());
          nq21.append(" in ( ");
          Lng32 sybnum = 0;

          for (int batchIdx = 0; batchIdx < connBatchSize_ && i < seedNum;) {
            connectByStackItem *vi = (connectByStackItem *)currQueue_->get(i);
            Lng32 tmpLevel = vi->level;
            i++;
            if (tmpLevel == currLevel_ - 1) {
              sybnum++;
              uppderid = ((connectByStackItem *)vi)->seedValue;
              if (vi->len > 0) {
                batchIdx++;
                char tmpbuf[128];
                char tmpbuf1[128];
                memset(tmpbuf, 0, 128);
                memset(tmpbuf1, 0, 128);
                if (vi->type >= REC_MIN_NUMERIC && vi->type <= REC_MAX_NUMERIC)
                  sprintf(tmpbuf, "%d ", *(int *)uppderid);
                else {
                  strcpy(tmpbuf, " '");
                  strncpy(tmpbuf1, (char *)uppderid, vi->len);
                  strcat(tmpbuf, tmpbuf1);
                  strcat(tmpbuf, "'");
                }

                nq21.append(tmpbuf);
              } else  // this is a null
              {
                // do nothing, null is not equal to any other
                nq21.append(" null ");
              }
              if (i == seedNum || batchIdx == connBatchSize_)
                continue;
              else
                nq21.append(" , ");
            }  // if( tmpLevel == currLevel)

          }  // for(int batchIdx = 0; batchIdx < 10 && i < seedNum; batchIdx ++)

          if (exeUtilTdb().orderSiblingsByCol_ != "") {
            nq21.append(" ) ORDER BY ");
            nq21.append((exeUtilTdb().childColName_).data());
            nq21.append(" ,");
            nq21.append(exeUtilTdb().orderSiblingsByCol_);
            nq21.append(" asc;");
          } else
            nq21.append(" );");

          if (sybnum == 0)  // end
          {
            step_ = NEXT_LEVEL_;
            break;
          }
          if (exeUtilTdb().hasIsLeaf_ == TRUE) {
            Queue *allrows = new (getHeap()) Queue(getHeap());  // TODO DELETE
            rc = cliInterface()->fetchAllRows(allrows, nq21.data(), 0, FALSE, FALSE, TRUE);
            if (rc < 0) {
              NADELETE(allrows, Queue, getHeap());
              return WORK_BAD_ERROR;
            }
            allrows->position();
            if (allrows->numEntries() == 0)  // no child
            {
              // emit parent
              if (emitPrevRow(tDesc, currLevel_, 1, 0, prevQueue_, i - 1) == -1) {
                upQueueIsFull_ = 1;
                return -1;
              }
            } else {
              // emit parent
              if (emitPrevRow(tDesc, currLevel_, 0, 0, prevQueue_, i - 1) == -1) {
                upQueueIsFull_ = 1;
                return -1;
              }
            }
            tmpPrevQueue_->insert(allrows);  // TODO: delete it

            for (int i1 = 0; i1 < allrows->numEntries(); i1++) {
              resultSize_++;
              OutputInfo *vi = (OutputInfo *)allrows->getNext();
              vi->get(0, ptr, len);
              connectByStackItem *it = new (getHeap()) connectByStackItem(getHeap());  // TODO
              char *tmp = new (getHeap()) char[len];                                   // TODO
              memcpy(tmp, ptr, len);
              it->seedValue = tmp;
              it->level = currLevel_;
              it->type = fsDatatype;
              it->len = len;
              it->parentId = i;
              if (exeUtilTdb().hasPath_ == TRUE) {
                vi->get(1, ptr1, len1);
                char *tmp1 = new (getHeap()) char[len1 + 1];
                memset(tmp1, 0, len1 + 1);
                memcpy(tmp1, ptr1, len1);
                it->pathLen = len1;
                it->pathItem = tmp1;
              }
              short rc1 = checkDuplicate(it, len, currLevel_);  // loop detection

              if (rc1 == 0) {
                thisQueue_->insert(it);
                matchRowNum++;
              }  // if(rc1== 0)
              else if (rc1 == 1) {
                // thisQueue_->insert(it);
                matchRowNum++;
                if (specialFlag != 1) {
                  loopDetected = 1;
                  if (exeUtilTdb().noCycle_ == FALSE) {
                    ComDiagsArea *diags = getDiagsArea();
                    if (diags == NULL) {
                      setDiagsArea(ComDiagsArea::allocate(getHeap()));
                      diags = getDiagsArea();
                    }
                    *diags << DgSqlCode(-EXE_CONNECT_BY_LOOP_ERROR);
                    step_ = ERROR_;
                  }
                }
              }
            }
          } else {
            if (upQueueIsFull_ == 0) {
              retcode = cliInterface()->fetchRowsPrologue(nq21.data(), FALSE, FALSE);
              nq21cnt_++;
            } else {
              // first resend
              short rc2 = emitRow(tDesc, currLevel_, 0, 0, resendIt_, FALSE);
              if (rc2 != 0) {
                return rc2;
              }
              upQueueIsFull_ = 0;
            }

            rc = 0;
            tmpCounter = 0;
            while ((rc >= 0) && (rc != 100)) {
              rc = cliInterface()->fetch();
              if (rc < 0) {
                cliInterface()->fetchRowsEpilogue(0, TRUE);
                return rc;
              }
              if (rc == 100) continue;
              resultSize_++;
              cliInterface()->getPtrAndLen(1, ptr, len);

              // memory free:
              // put into thiseQueue_ , thisQueue put into currArray[] and will be cleanup in NEXT_ROOT
              connectByStackItem *it = new (getHeap()) connectByStackItem(getHeap());

              char *tmp = new (getHeap()) char[len];
              memcpy(tmp, ptr, len);
              it->seedValue = tmp;
              it->level = currLevel_;
              it->type = fsDatatype;
              it->len = len;
              it->parentId = i;
              if (exeUtilTdb().hasPath_ == TRUE) {
                cliInterface()->getPtrAndLen(2, ptr1, len1);
                char *tmp1 = new (getHeap()) char[len1 + 1];
                memset(tmp1, 0, len1 + 1);
                memcpy(tmp1, ptr1, len1);
                it->pathLen = len1;
                it->pathItem = tmp1;
              }
              short rc1 = checkDuplicate(it, len, currLevel_);  // loop detection

              if (rc1 == 0) {
                thisQueue_->insert(it);
                short rc2 = emitRow(tDesc, currLevel_, 0, 0, it, FALSE);
                if (rc2 != 0) {
                  // save context
                  resendIt_ = it;
                  upQueueIsFull_ = 1;
                  return rc2;
                }
                matchRowNum++;
              }  // if(rc1== 0)
              else if (rc1 == 1) {
                if (specialFlag != 1) {
                  loopDetected = 1;
                  if (exeUtilTdb().noCycle_ == FALSE) {
                    ComDiagsArea *diags = getDiagsArea();
                    if (diags == NULL) {
                      setDiagsArea(ComDiagsArea::allocate(getHeap()));
                      diags = getDiagsArea();
                    }
                    *diags << DgSqlCode(-EXE_CONNECT_BY_LOOP_ERROR);
                    step_ = ERROR_;
                    // release it
                    it->cleanup();
                    NADELETE(it, connectByStackItem, getHeap());
                  } else {
                    it->cleanup();
                    NADELETE(it, connectByStackItem, getHeap());
                    continue;
                  }

                } else {
                  emitRow(tDesc, currLevel_, 0, 0, it, FALSE);
                  it->cleanup();
                  NADELETE(it, connectByStackItem, getHeap());
                }
              }
            }  // while ((rc >= 0)
            cliInterface()->fetchRowsEpilogue(0, FALSE);
          }

        }  // for(int i=0; i< seedNum; i++)
        // this level is done
        if (exeUtilTdb().hasIsLeaf_ == TRUE) {
          // release prevQueue_
          for (int i = 0; i < prevQueue_->numEntries(); i++) {
            NADELETE((Queue *)prevQueue_->get(i), Queue, getHeap());
          }
          NADELETE(prevQueue_, Queue, getHeap());
          prevQueue_ = tmpPrevQueue_;
        }

        if (loopDetected == 1) {
          if (exeUtilTdb().noCycle_ == TRUE) {
            step_ = NEXT_LEVEL_;
          } else
            step_ = ERROR_;
          break;
        }
        if (resultSize_ == 0)
          step_ = NEXT_ROOT_;
        else
          step_ = NEXT_LEVEL_;
      } break;
      case NEXT_LEVEL_: {
        Queue *currQueue = new (getHeap()) Queue(getHeap());  // will be free in NEXT_ROOT
        currLevel_++;
        currArray[currLevel_] = currQueue;

        currSeedNum_ = 0;

        if (currLevel_ > exeUtilTdb().maxDeep_) {
          ComDiagsArea *diags = getDiagsArea();
          if (diags == NULL) {
            setDiagsArea(ComDiagsArea::allocate(getHeap()));
            diags = getDiagsArea();
          }
          *diags << DgSqlCode(-EXE_CONNECT_BY_MAX_REC_ERROR);

          step_ = ERROR_;
        } else
          step_ = DO_CONNECT_BY_;
      } break;
      case NEXT_ROOT_: {
        currRootId_++;
        currLevel_ = 1;
        currQueue_ =
            getCurrQueue(currRootId_, seedQueue_);  // in fact, this is the queue for all level 1 seeds for this root
        currArray[0] = currQueue_;

        // clear currArray
        for (int i = 1; i < CONNECT_BY_MAX_LEVEL_SIZE; i++) {
          if (currArray[i] != NULL)  // comehere
          {
            releaseCurrentQueue(currArray[i], getHeap());
            NADELETE(currArray[i], Queue, getHeap());
            currArray[i] = NULL;
          }
        }

        currSeedNum_ = 0;

        if (currQueue_ == NULL)
          step_ = DONE_;
        else {
          Queue *currQueue =
              new (getHeap()) Queue(getHeap());  // this is the level 2 queue, empty now, will fill in in nq21 run
          // so it will be free in NEXT_ROOT code above when current root is done
          currArray[currLevel_] = currQueue;
          step_ = DO_CONNECT_BY_;
        }
      } break;
      case ERROR_: {
        if (qparent_.up->isFull()) return WORK_OK;
        if (handleError()) return WORK_OK;

        step_ = DONE_;
      } break;
      case DONE_:
        if (qparent_.up->isFull()) return WORK_OK;

        retcode = handleDone();
        if (retcode == 1) return WORK_OK;
        if (cachework_ == 0) {
          for (int i = 0; i < seedQueue_->entries(); i++) {
            rootItem *ri = (rootItem *)seedQueue_->get(i);
            ri->cleanup();
            NADELETE(ri, rootItem, getHeap());
          }
          NADELETE(seedQueue_, Queue, getHeap());
        }

        step_ = INITIAL_;
        currSeedNum_ = 0;

        if (qparent_.down->isEmpty()) {
          // clear cache
          if (resultCache_ != NULL) {
            for (int i = 0; i < resultCache_->entries(); i++) {
              NADELETEBASIC((char *)resultCache_->get(i), getHeap());
            }
            NADELETE(resultCache_, Queue, getHeap());
          }
          resultCache_ = NULL;
          return WORK_OK;
        }

        break;
    }
  }

  return WORK_OK;
}

ex_tcb *ExExeUtilConnectbyTdb::build(ex_globals *glob) {
  ExExeUtilConnectbyTcb *exe_util_tcb;

  exe_util_tcb = new (glob->getSpace()) ExExeUtilConnectbyTcb(*this, glob);

  exe_util_tcb->registerSubtasks();

  return (exe_util_tcb);
}

ExExeUtilConnectbyTdbState::ExExeUtilConnectbyTdbState() {}
ExExeUtilConnectbyTdbState::~ExExeUtilConnectbyTdbState() {}
