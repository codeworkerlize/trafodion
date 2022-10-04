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
 * File:         ExConnectByTempTable.cpp
 * Description:  class to materialize result set for ConnectBy operator
 *               but can be used elsewhere as well if suitable
 *               
 * Created:      8/20/2019
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "executor/ex_stdh.h"
#include "comexe/ComTdb.h"
#include "executor/ex_tcb.h"

#include "ExConnectByTempTable.h"

//
// Build a connectByTempTable tcb
//
ex_tcb * ExConnectByTempTableTdb::build(ex_globals * glob)
{
  // first build the child
  ex_tcb * child_tcb = tdbChild_->build(glob);
  
  ExConnectByTempTableTcb * connectbytemptable_tcb = 
    new(glob->getSpace()) ExConnectByTempTableTcb(*this, *child_tcb, glob);
  
  // add this tcb to the schedule
  connectbytemptable_tcb->registerSubtasks();

  return (connectbytemptable_tcb);
}

//
// Constructor for connectbytemptable_tcb
//
ExConnectByTempTableTcb::ExConnectByTempTableTcb(const ExConnectByTempTableTdb & connectbytemptable_tdb, 
             const ex_tcb & child_tcb,    // child queue pair
             ex_globals *glob
             ) : ex_tcb( connectbytemptable_tdb, 1, glob),
                             step_(INITIAL_)
{
  childTcb_ = &child_tcb;
  
  Space * space = glob->getSpace();
  CollHeap * heap = (glob ? glob->getDefaultHeap() : NULL);
  
  // Allocate the buffer pool
  pool_ = new(space) sql_buffer_pool(connectbytemptable_tdb.numBuffers_,
                     connectbytemptable_tdb.bufferSize_,
                     space);
  
  // get the queue that child use to communicate with me
  qchild_  = child_tcb.getParentQueue(); 
  
  // Allocate the queue to communicate with parent
  qparent_.down = new(space) ex_queue(ex_queue::DOWN_QUEUE,
                      connectbytemptable_tdb.queueSizeDown_,
                      connectbytemptable_tdb.criDescDown_,
                      space);
  
  // Allocate the private state in each entry of the down queue
  ExConnectByTempTablePrivateState p(this);
  qparent_.down->allocatePstate(&p, this);

  qparent_.up = new(space) ex_queue(ex_queue::UP_QUEUE,
                    connectbytemptable_tdb.queueSizeUp_,
                    connectbytemptable_tdb.criDescUp_,
                    space);

  probeBytes_ = new(space) char[ connectbytemptable_tdb.probeLen_ ];
  probeInputBytes_ = new(space) char[ connectbytemptable_tdb.probeLen_ ];

  workAtp_ = allocateAtp(connectbytemptable_tdb.workCriDesc_ , getSpace());

  probeHashTupp_.init(sizeof(probeHashVal_),
		    NULL,
		    (char *) (&probeHashVal_));
  workAtp_->getTupp(connectbytemptable_tdb.hashValIdx_) = &probeHashTupp_;

  if (selectPred())
    selectPred()->fixup(0, getExpressionMode(), this, space, heap,
                glob->computeSpace(), glob);

  hashProbeExpr()->fixup(0, getExpressionMode(), this, space, heap,
                glob->computeSpace(), glob);

  probeEncodeTupp_.init(connectbytemptable_tdb.probeLen_,
		    NULL,
		    (char *) (probeBytes_));
  workAtp_->getTupp(connectbytemptable_tdb.encodedProbeDataIdx_) = &probeEncodeTupp_;
  encodeProbeExpr()->fixup(0, getExpressionMode(), this, space, heap,
                glob->computeSpace(), glob);


  probeInputHashTupp_.init(sizeof(probeInputHashVal_),
		    NULL,
		    (char *) (&probeInputHashVal_));
  workAtp_->getTupp(connectbytemptable_tdb.hashInputValIdx_) = &probeInputHashTupp_;

  hashInputProbeExpr()->fixup(0, getExpressionMode(), this, space, heap,
                glob->computeSpace(), glob);

  probeInputEncodeTupp_.init(connectbytemptable_tdb.probeLen_,
		    NULL,
		    (char *) (probeInputBytes_));
  workAtp_->getTupp(connectbytemptable_tdb.encodeInputProbeDataIdx_) = &probeInputEncodeTupp_;
  encodeInputProbeExpr()->fixup(0, getExpressionMode(), this, space, heap,
                glob->computeSpace(), glob);





  if (moveInnerExpr())
    moveInnerExpr()->fixup(0, getExpressionMode(), this, space, heap,
                glob->computeSpace(), glob);

  hashTable_ = new(space) ExConnectByHashTable(space, 
                connectbytemptable_tdb.cacheSize_, connectbytemptable_tdb.probeLen_, this);

  dataPopulated_ = FALSE;
};

ExConnectByTempTableTcb::~ExConnectByTempTableTcb()
{
  freeResources();
};
  
////////////////////////////////////////////////////////////////////////
// Free Resources
//
void ExConnectByTempTableTcb::freeResources()
{
  delete qparent_.up;
  delete qparent_.down;
};

////////////////////////////////////////////////////////////////////////
// Register subtasks 
//
void ExConnectByTempTableTcb::registerSubtasks()
{
  ExScheduler *sched = getGlobals()->getScheduler();
  ex_queue_pair pQueue = getParentQueue();
  // register events for parent queue
  ex_assert(pQueue.down && pQueue.up,"Parent down queue must exist");
  sched->registerInsertSubtask(ex_tcb::sWork, this, pQueue.down);
  sched->registerCancelSubtask(sCancel, this, pQueue.down);
  sched->registerUnblockSubtask(ex_tcb::sWork,this, pQueue.up);

  // register events for child queues
  const ex_queue_pair cQueue = getChild(0)->getParentQueue();

  sched->registerUnblockSubtask(ex_tcb::sWork,this, cQueue.down);
  sched->registerInsertSubtask(ex_tcb::sWork, this, cQueue.up);
  if (getPool())
    getPool()->setStaticMode(FALSE);
}

////////////////////////////////////////////////////////////////////////////
// This is where the action is.
////////////////////////////////////////////////////////////////////////////
short ExConnectByTempTableTcb::work()
{
  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  while (1) // exit via return
    {
      switch (step_)
    {
    case INITIAL_:
      {
        nextRequest_ = 0;
        if(dataPopulated_ == FALSE)
        {
          if(qparent_.down->isFull())
            return WORK_OK;

          ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
          ex_queue_entry * centry = qchild_.down->getTailEntry();
          //get data from child
          centry->downState.request = ex_queue::GET_ALL;
          centry->downState.requestValue = 11;
          centry->downState.parentIndex = qparent_.down->getHeadIndex();
          centry->passAtp(pentry_down);
          qchild_.down->insert();
          step_ = POPULATE_DATA_;
        }
        else
          step_ = PROCESS_REQUEST_;
      }
      break;
    case POPULATE_DATA_:
      {
        if (qchild_.up->isEmpty())
          return WORK_OK;

        ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
        ExConnectByTempTablePrivateState& pstate
          = *((ExConnectByTempTablePrivateState*) pentry_down->pstate);

        ex_queue_entry *reply = qchild_.up->getHeadEntry();
        switch( reply->upState.status )
        {
          case ex_queue::Q_OK_MMORE:
            {
            if ((hashProbeExpr()->eval(reply->getAtp(),workAtp_)) != 
                ex_expr::EXPR_OK)
              ex_assert(0, "Unexpected result from hashProbeExpr");

              if ((encodeProbeExpr()->eval(reply->getAtp(),workAtp_)) != ex_expr::EXPR_OK)
                ex_assert(0, "Unexpected result from hashEncodeExpr");

              ExConnectByHashEntry *pcEntry_ = hashTable_->addEntry( probeHashVal_, probeBytes_);
              
              MoveStatus moveRetCode = 
                        moveReplyToCache(*reply, pcEntry_);
              if (moveRetCode == MOVE_BLOCKED)
              {
                return WORK_POOL_BLOCKED;
              }             
            }
            break;
          case ex_queue::Q_NO_DATA:  //population is over
            {
              step_ = PROCESS_REQUEST_;
              dataPopulated_ = TRUE;
              qchild_.up->removeHead();
            }
            break;
          default:
            {
              ex_assert(0, "Unknown upstate.status in child up queue");
              break;
            }
        } //switch reply->upState.status
      }
      break;
    case PROCESS_REQUEST_:
      {
        if (qparent_.down->isEmpty())
          return WORK_OK;

        if (qparent_.up->isFull())
          return WORK_OK;

        //get request
        ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
        ExConnectByTempTablePrivateState& pstate = *((ExConnectByTempTablePrivateState*) pentry_down->pstate);
        switch (pentry_down->downState.request)
        {
          case ex_queue::GET_N: //ConnectBy should not issue GET_N, this is for future
          case ex_queue::GET_ALL:
            {
            if ((hashInputProbeExpr()->eval(pentry_down->getAtp(),workAtp_)) != 
                ex_expr::EXPR_OK)
            ex_assert(0, "Unexpected result from hashInputExpr");
            if ((encodeInputProbeExpr()->eval(pentry_down->getAtp(),workAtp_)) != 
                ex_expr::EXPR_OK)
            ex_assert(0, "Unexpected result from encodeInputExpr");

            if (hashTable_->findEntry(
                  probeInputHashVal_, probeInputBytes_, nextRequest_, pstate.pcEntry_
                                    ) == ExConnectByHashTable::FOUND)
              {
                 makeReplyToParentUp(pentry_down, pstate,
                            ex_queue::Q_OK_MMORE);
                 nextRequest_++;
              }
            else
              {
                //return NO DATA
                step_ = DONE_;
                nextRequest_ = 0;
              }          
           } //case ex_queue::GET_ALL
           break;
          case ex_queue::GET_NOMORE:
            {
              step_ = DONE_;
            }          
	       break;
          default:
          {
            ex_assert(0, "ExConnectByTempTableTcb cannot handle this request");
            break;
          } 
        } //switch (pentry->downState.request)
      }
      break;
    case DONE_:
      {
        ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
        ExConnectByTempTablePrivateState& pstate = *((ExConnectByTempTablePrivateState*) pentry_down->pstate);
        makeReplyToParentUp(pentry_down, pstate,
                            ex_queue::Q_NO_DATA);
        step_ = INITIAL_;
        qparent_.down->removeHead();
        return WORK_CALL_AGAIN;
      }
      break;

    case CANCEL_:
      {
            // ignore all up rows from child. wait for Q_NO_DATA.
        if (qchild_.up->isEmpty())
          return WORK_OK;

        ex_queue_entry * cUpEntry = qchild_.up->getHeadEntry();
        ex_queue_entry * pUpEntry = qparent_.up->getTailEntry();

        switch(cUpEntry->upState.status)
          {
          case ex_queue::Q_OK_MMORE:
          case ex_queue::Q_SQLERROR:
        {
          qchild_.up->removeHead();
        }
          break;
          
          case ex_queue::Q_NO_DATA:
        {
          step_ = DONE_;
        }
          break;
          
          case ex_queue::Q_INVALID: 
        {
          ex_assert(0, "ExConnectByTempTableTcb::work() Invalid state returned by child");
        }; break;
          }
      }
    break;
    
    case ERROR_:
      {
      }
      break;

    } // switch
    } // while

  return 0;
}


ExConnectByTempTableTcb::MoveStatus ExConnectByTempTableTcb::moveReplyToCache(ex_queue_entry &reply, ExConnectByHashEntry *pcEntry)
{
  if (moveInnerExpr())
    {
      if (pool_->get_free_tuple(pcEntry->innerRowTupp_,connectbytemptableTdb().recLen_))
        return MOVE_BLOCKED;

      workAtp_->getTupp(connectbytemptableTdb().innerRowDataIdx_) = 
            pcEntry->innerRowTupp_;

      ex_expr::exp_return_type innerMoveRtn = 
        moveInnerExpr()->eval(reply.getAtp(),workAtp_);
      
      if (innerMoveRtn == ex_expr::EXPR_ERROR)
        return MOVE_ERROR;
    }
  else
    {
      ex_assert(pcEntry->innerRowTupp_.isAllocated() == FALSE, 
               "Incorrectly initialized inneRowTupp_");
    }
  qchild_.up->removeHead();
  return MOVE_OK;
}

void  ExConnectByTempTableTcb::makeReplyToParentUp(ex_queue_entry *pentry_down,
                           ExConnectByTempTablePrivateState &pstate,
                           ex_queue::up_status reply_status)
{
  ex_queue_entry * up_entry = qparent_.up->getTailEntry();
  Int32 rowQualifies = 1;
  
  up_entry->copyAtp(pentry_down);
  if ((reply_status == ex_queue::Q_OK_MMORE) &&
      (pstate.pcEntry_->innerRowTupp_.isAllocated()))
  {
    up_entry->getAtp()->getTupp(connectbytemptableTdb().tuppIndex_) = 
            pstate.pcEntry_->innerRowTupp_;
       if (selectPred())
        {
          ex_expr::exp_return_type evalRetCode =
            selectPred()->eval(up_entry->getAtp(), 0);

          if (evalRetCode == ex_expr::EXPR_FALSE)
            rowQualifies = 0;
          else if (evalRetCode != ex_expr::EXPR_TRUE)
            {
              ex_assert(evalRetCode == ex_expr::EXPR_ERROR,
                        "invalid return code from expr eval");
               reply_status = ex_queue::Q_SQLERROR;
            }
        }
  }
  if(rowQualifies == 1) {
  up_entry->upState.parentIndex = 
  pentry_down->downState.parentIndex;
  up_entry->upState.downIndex = qparent_.down->getHeadIndex();
  up_entry->upState.setMatchNo(pstate.matchCount_);
  up_entry->upState.status = reply_status;

  qparent_.up->insert();
  }
}


short ExConnectByTempTableTcb::cancel()
{
  // if no parent request, return
  if (qparent_.down->isEmpty())
    return WORK_OK;

  ex_queue_entry * pentry_down = qparent_.down->getHeadEntry();
  ExConnectByTempTablePrivateState *pstate = (ExConnectByTempTablePrivateState*) pentry_down->pstate;

  if (pentry_down->downState.request == ex_queue::GET_NOMORE)
    {
      step_ = CANCEL_;
      qchild_.down->cancelRequest();
    }
  
  return WORK_OK;
}

///////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for sort_private_state
///////////////////////////////////////////////////////////////////////////////
ExConnectByTempTablePrivateState::ExConnectByTempTablePrivateState(const ExConnectByTempTableTcb *  tcb)
{
    pcEntry_ = NULL;
}
ExConnectByTempTablePrivateState::~ExConnectByTempTablePrivateState()
{
};

ex_tcb_private_state * ExConnectByTempTablePrivateState::allocate_new(const ex_tcb *tcb)
{
  return new(((ex_tcb *)tcb)->getSpace()) ExConnectByTempTablePrivateState((ExConnectByTempTableTcb *) tcb);
};

ExConnectByHashTable::ExConnectByHashTable(Space *space,
                 ULng32 numEntries, 
                 ULng32 probeLength,
                 ExConnectByTempTableTcb *tcb) :
    space_(space),
    numBuckets_(numEntries),
    probeLen_(probeLength),
    tcb_(tcb),
    buckets_(NULL),
    entries_(NULL),
    nextSlot_(0)
{
    buckets_ = new(space_) ExConnectByHashEntry *[numBuckets_];

    // Initialize all the buckets to "empty".    
    memset((char *)buckets_, 0, numBuckets_ * sizeof(ExConnectByHashEntry *));

    // Calculate the real size for each ExConnectByHashEntry -- the probeData_ 
    // array is one byte so subtract that from probeLength.
    sizeofExConnectByHashEntry_ = ROUND8(sizeof(ExConnectByHashEntry) + (probeLength - 1));

    // Get the size in bytes of the ExConnectByHashEntry array.
    const Int32 totalExConnectByHashEntrysizeInBytes = numEntries * sizeofExConnectByHashEntry_ * CONNECT_BY_HASHTABLE_SIZE_FACTOR;
	
    max_slots_ = numEntries * CONNECT_BY_HASHTABLE_SIZE_FACTOR;

    entries_ = new(space_) char[totalExConnectByHashEntrysizeInBytes];

    memset(entries_, 0, totalExConnectByHashEntrysizeInBytes);
};

///////////////////////////////////////////////////////////////////
ExConnectByHashTable::~ExConnectByHashTable()
{
  if (buckets_ != NULL)
    NADELETEBASIC(buckets_, space_);
  buckets_ = NULL;

  if (entries_ != NULL)
    NADELETEBASIC(entries_, space_);
  entries_ = NULL;
}

///////////////////////////////////////////////////////////////////
ExConnectByHashTable::FoundOrNotFound
ExConnectByHashTable::findEntry( ULng32 probeHashVal, 
                         char * probeBytes, 
                         UInt32 getIndex, 
                         ExConnectByHashEntry * &pcEntry )
{
  FoundOrNotFound retcode = NOTFOUND;
  const Int32 bucketNum = probeHashVal % numBuckets_;
  UInt32 chainLength = 1;
  UInt32 hitSeq = 0;  //must be same as getIndex for a FOUND 

  pcEntry = buckets_[bucketNum];

  while (pcEntry)
    {
      if ((pcEntry->probeHashVal_ == probeHashVal) &&
          (memcmp((char *) pcEntry->probeData_, probeBytes, probeLen_) == 0))
        {
          // This is a hit.
          if(hitSeq == getIndex)  //to rename this!
          {
            break;
          }
          hitSeq ++;
        }
      pcEntry = pcEntry->nextHashVal_;
      chainLength++;
    }
  if(pcEntry)
  {
    pcEntry->refCnt_++;
    pcEntry->flags_.useBit_ = 1;
    retcode = FOUND;
  }
  return retcode;
  
}

///////////////////////////////////////////////////////////////////
ExConnectByHashEntry *ExConnectByHashTable::addEntry(
                ULng32 probeHashVal, 
                char * probeBytes)
{
  bool foundVictim = false;
  ExConnectByHashEntry *pce;

  const Int32 bucketNum = probeHashVal % numBuckets_;
  ExConnectByHashEntry *bucketHead= buckets_[bucketNum];
  ExConnectByHashEntry *bucketptr= bucketHead;
  //go the end of the bucket
  if(bucketHead == NULL)
  {
    pce = getSlot(); //allocate a new entry
    buckets_[bucketNum] = pce;
  }
  else
  {
    pce = bucketHead;
    while(pce->nextHashVal_) pce=pce->nextHashVal_;
      pce->nextHashVal_ = getSlot(); //allocate a new entry
    pce = pce->nextHashVal_ ;
  }
  // Now initialize other members:

  pce->probeHashVal_ = probeHashVal;
  pce->refCnt_ = 1;
  pce->upstateStatus_ = ex_queue::Q_INVALID;
  if (pce->diagsArea_)
    {
      pce->diagsArea_->decrRefCount();
      pce->diagsArea_ = NULL;
    }
  pce->innerRowTupp_.release();
  memcpy(pce->probeData_, probeBytes, probeLen_);

  return pce;
}

///////////////////////////////////////////////////////////////////
ExConnectByHashEntry *ExConnectByHashTable::getSlot()
{
  if (nextSlot_ == max_slots_)
    nextSlot_ = 0;

  ULng32 offsetToEntry = nextSlot_++ * sizeofExConnectByHashEntry_;
  
  return (ExConnectByHashEntry *) &entries_[offsetToEntry]; 
}

