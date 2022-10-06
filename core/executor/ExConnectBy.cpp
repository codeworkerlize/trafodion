
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExConnectBy.cpp
 * Description:  class to do Connect By
 *
 *
 * Created:      8/2/2019
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ExConnectBy.h"

#include "comexe/ComTdb.h"
#include "executor/ex_stdh.h"
#include "executor/ex_tcb.h"
#include "exp_clause_derived.h"

//
// Build a ConnectBy tcb
//
ex_tcb *ExConnectByTdb::build(ex_globals *glob) {
  ex_tcb *s_child_tcb = tdbSChild_->build(glob);
  ex_tcb *c_child_tcb = NULL;
  if (tdbCChild_) c_child_tcb = tdbCChild_->build(glob);
  ExConnectByTcb *connectby_tcb = new (glob->getSpace()) ExConnectByTcb(*this, *s_child_tcb, *c_child_tcb, glob);

  // add this tcb to the schedule
  connectby_tcb->registerSubtasks();

  return (connectby_tcb);
}

//
// Constructor for connectby_tcb
//
ExConnectByTcb::ExConnectByTcb(const ExConnectByTdb &connectby_tdb, const ex_tcb &s_child_tcb,
                               const ex_tcb &c_child_tcb, ex_globals *glob)
    : ex_tcb(connectby_tdb, 1, glob), step_(INITIAL_) {
  childStartWithTcb_ = &s_child_tcb;
  childConnectByTcb_ = &c_child_tcb;

  Space *space = glob->getSpace();
  CollHeap *heap = (glob ? glob->getDefaultHeap() : NULL);

  // Allocate the buffer pool
  pool_ = new (space) sql_buffer_pool(connectby_tdb.numBuffers_, connectby_tdb.bufferSize_, space);

  // Allocate the queue to communicate with parent
  qparent_.down =
      new (space) ex_queue(ex_queue::DOWN_QUEUE, connectby_tdb.queueSizeDown_, connectby_tdb.criDescDown_, space);
  // Allocate the private state in each entry of the down queue
  ExConnectByPrivateState p(this);
  qparent_.down->allocatePstate(&p, this);
  qparent_.up = new (space) ex_queue(ex_queue::UP_QUEUE, connectby_tdb.queueSizeUp_, connectby_tdb.criDescUp_, space);

  qStartWithchild_ = childStartWithTcb_->getParentQueue();
  qConnectBychild_ = childConnectByTcb_->getParentQueue();
  workAtp_ = NULL;
  currentLevel_ = 1;
  errorNum_ = 8043;  // generic ConnectBy error

  if (connectby_tdb.leftMoveExpr_) {
    ex_expr::exp_return_type rc;
    rc = connectby_tdb.leftMoveExpr_->fixup(0, getExpressionMode(), this, space, heap, FALSE, glob);
  }
  if (connectby_tdb.rightMoveExpr_) {
    ex_expr::exp_return_type rc;
    rc = connectby_tdb.rightMoveExpr_->fixup(0, getExpressionMode(), this, space, heap, FALSE, glob);
  }
  if (connectby_tdb.condExpr_)
    (void)connectby_tdb.condExpr_->fixup(0, getExpressionMode(), this, space, heap, FALSE, glob);

  if (connectby_tdb.priorCondExpr_)
    (void)connectby_tdb.priorCondExpr_->fixup(0, getExpressionMode(), this, space, heap, FALSE, glob);

  if (connectby_tdb.priorPredExpr_) {
    ex_expr::exp_return_type rc;
    rc = connectby_tdb.priorPredExpr_->fixup(0, getExpressionMode(), this, space, heap, FALSE, glob);
  }
  if (connectby_tdb.priorValMoveExpr1_) {
    ex_expr::exp_return_type rc;
    rc = connectby_tdb.priorValMoveExpr1_->fixup(0, getExpressionMode(), this, space, heap, FALSE, glob);
  }
  if (connectby_tdb.priorValMoveExpr2_) {
    ex_expr::exp_return_type rc;
    rc = connectby_tdb.priorValMoveExpr2_->fixup(0, getExpressionMode(), this, space, heap, FALSE, glob);
  }
  if (connectby_tdb.leftPathExpr_) {
    ex_expr::exp_return_type rc;
    rc = connectby_tdb.leftPathExpr_->fixup(0, getExpressionMode(), this, space, heap, FALSE, glob);
  }

  if (connectby_tdb.rightPathExpr_) {
    ex_expr::exp_return_type rc;
    rc = connectby_tdb.rightPathExpr_->fixup(0, getExpressionMode(), this, space, heap, FALSE, glob);
  }
  if (connectby_tdb.workCriDesc_) {
    workAtp_ = allocateAtp(connectby_tdb.workCriDesc_, space);
  }

  initialParentDownAtp_ = allocateAtp(connectby_tdb.criDescDown_, space);
  currentRoot_ = NULL;
  cache_ = NULL;
  doUseCache_ = FALSE;  // turn this to TRUE when fix the cache bug
};

ExConnectByTcb::~ExConnectByTcb() {
  if (workAtp_) {
    workAtp_->release();
    deallocateAtp(workAtp_, getGlobals()->getSpace());
    workAtp_ = NULL;
  }
  freeResources();
};

////////////////////////////////////////////////////////////////////////
// Free Resources
//
void ExConnectByTcb::freeResources() {
  if (pool_) {
    delete pool_;
    pool_ = NULL;
  }
  delete qparent_.up;
  delete qparent_.down;
};

////////////////////////////////////////////////////////////////////////
// Register subtasks
//
void ExConnectByTcb::registerSubtasks() {
  ExScheduler *sched = getGlobals()->getScheduler();
  ex_queue_pair pQueue = getParentQueue();
  // register events for parent queue
  ex_assert(pQueue.down && pQueue.up, "Parent down queue must exist");
  sched->registerInsertSubtask(ex_tcb::sWork, this, pQueue.down);
  sched->registerCancelSubtask(sCancel, this, pQueue.down);
  sched->registerUnblockSubtask(ex_tcb::sWork, this, pQueue.up);
  // register events for child queues
  sched->registerUnblockSubtask(sWork, this, qStartWithchild_.down);
  sched->registerInsertSubtask(sWork, this, qStartWithchild_.up);

  // const ex_queue_pair cQueue = getChild(1)->getParentQueue();
  sched->registerUnblockSubtask(sWork, this, qConnectBychild_.down);
  sched->registerInsertSubtask(sWork, this, qConnectBychild_.up);
  if (getPool()) getPool()->setStaticMode(FALSE);
}

short ExConnectByTcb::setPseudoValue(int lvl, int isleaf, int iscycle, char *path) {
  if (pool_->get_free_tuple(pcoldata_, connectbyTdb().pseudoOutputRowLen_)) {
    return -1;
  }

  workdata_ = pcoldata_.getDataPointer();

  if (connectbyTdb().pathPseudoColRowAtpIndex_ > 0)  // has path required
  {
    if (pool_->get_free_tuple(pathColTupp_, connectbyTdb().pathOutputRowLen_)) {
      return -1;
    }
    pathdata_ = pathColTupp_.getDataPointer();
  }

  // get the tuple desc
  ExpTupleDesc *tDesc = connectbyTdb().getCriDescUp()->getTupleDescriptor(connectbyTdb().fixedPseudoColRowAtpIndex_);

  short srcType = REC_BIN32_UNSIGNED;
  int srcLen = 4;
  int src = lvl;
  Attributes *attr = tDesc->getAttr(0);  // level is the first
  if (::convDoIt((char *)&src, srcLen, srcType, 0, 0, &workdata_[attr->getOffset()], attr->getLength(),
                 attr->getDatatype(), 0, 0, 0, 0, NULL) != ex_expr::EXPR_OK) {
    return -1;
  }

  src = lvl;
  attr = tDesc->getAttr(3);  // rownum is the fourth
  if (::convDoIt((char *)&src, srcLen, srcType, 0, 0, &workdata_[attr->getOffset()], attr->getLength(),
                 attr->getDatatype(), 0, 0, 0, 0, NULL) != ex_expr::EXPR_OK) {
    return -1;
  }

  src = isleaf;
  attr = tDesc->getAttr(2);  // isleaf is the first
  if (::convDoIt((char *)&src, srcLen, srcType, 0, 0, &workdata_[attr->getOffset()], attr->getLength(),
                 attr->getDatatype(), 0, 0, 0, 0, NULL) != ex_expr::EXPR_OK) {
    return -1;
  }

  src = iscycle;
  attr = tDesc->getAttr(1);  // iscycle is the first
  if (::convDoIt((char *)&src, srcLen, srcType, 0, 0, &workdata_[attr->getOffset()], attr->getLength(),
                 attr->getDatatype(), 0, 0, 0, 0, NULL) != ex_expr::EXPR_OK) {
    return -1;
  }
  if (connectbyTdb().pathPseudoColRowAtpIndex_ > 0)  // has path required
  {
    ExpTupleDesc *pDesc = connectbyTdb().getCriDescUp()->getTupleDescriptor(connectbyTdb().pathPseudoColRowAtpIndex_);
    char *pathBuf = path;
    int pathlength = str_len(path);
    if (pathlength > 3000) pathlength = 3000;
    UInt32 vcActualLen = 0;

    Attributes *attr = pDesc->getAttr(0);
    if (::convDoIt(pathBuf, pathlength, REC_BYTE_V_ASCII, 0, 0, &pathdata_[attr->getOffset()], 3000,
                   attr->getDatatype(), 0, 0, (char *)&vcActualLen, sizeof(vcActualLen), NULL) != ex_expr::EXPR_OK) {
      return -1;
    }
    if (attr->getVCIndicatorLength() > 0) {
      attr->setVarLength(vcActualLen, &pathdata_[attr->getVCLenIndOffset()]);
    }
    if (attr->getNullFlag()) {
      if (vcActualLen == -1) {
        *(short *)&pathdata_[attr->getNullIndOffset()] = -1;
      } else {
        *(short *)&pathdata_[attr->getNullIndOffset()] = 0;
      }
    }
  }
  if (path != NULL) NADELETEBASIC(path, getHeap());

  return 0;
}

short ExConnectByTcb::moveRightChildDataToParent(ExConnectByTreeNode *node, short state) {
  short ret = 0;
  ex_expr::exp_return_type retCode = ex_expr::EXPR_TRUE;
  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
  ex_queue_entry *pUpEntry = qparent_.up->getTailEntry();
  char *ptr = NULL;
  char *ptr1 = NULL;
  short releaseUpEntry = 0;

  if (state == 1) {
    pUpEntry->copyAtp(pentry_down);
    short rc = pool_->get_free_tuple(pUpEntry->getAtp()->getTupp(connectbyTdb().returnRowAtpIndex_),
                                     connectbyTdb().outputRowLen_);
    if (rc != 0) {
      return WORK_POOL_BLOCKED;
    }

    if (connectbyTdb().rightMoveExpr_) connectbyTdb().rightMoveExpr_->eval(node->getAtp(), pUpEntry->getAtp());

    short rfcd = pUpEntry->getAtp()->getTupp(connectbyTdb().returnRowAtpIndex_).getRefCount();

    ptr = pUpEntry->getAtp()->getTupp(connectbyTdb().returnRowAtpIndex_).getDataPointer();  // for debug only
    pUpEntry->getAtp()->getTupp(connectbyTdb().fixedPseudoColRowAtpIndex_) = pcoldata_;

    if (connectbyTdb().pathPseudoColRowAtpIndex_ > 0)  // has path required
    {
      pUpEntry->getAtp()->getTupp(connectbyTdb().pathPseudoColRowAtpIndex_) = pathColTupp_;
    }

    if (connectbyTdb().condExpr_) {
      retCode = connectbyTdb().condExpr_->eval(pUpEntry->getAtp(), workAtp_);
    }

    if (retCode == ex_expr::EXPR_TRUE) {
      if (node->getSentOut() == 0) {
        pUpEntry->upState.status = ex_queue::Q_OK_MMORE;
        pUpEntry->upState.downIndex = qparent_.down->getHeadIndex();
        pUpEntry->upState.parentIndex = pentry_down->downState.parentIndex;
        ret = 0;
        // move to cache
        if (connectbyTdb().useCache() == TRUE && doUseCache_ == TRUE) {
          atp_struct *r = allocateAtp(connectbyTdb().criDescUp_, getSpace());
          // atp_struct * r = allocateAtp(pUpEntry->getAtp()->getCriDesc(),getSpace());
          r->copyAtp(pUpEntry->getAtp());
          ptr1 = r->getTupp(connectbyTdb().returnRowAtpIndex_).getDataPointer();  // for debug only
          char *ptr2 = r->getTupp(connectbyTdb().fixedPseudoColRowAtpIndex_).getDataPointer();
          short rfc = r->getTupp(connectbyTdb().returnRowAtpIndex_).getRefCount();
          cache_->insert(r);
        }
        qparent_.up->insert();
      }
    } else {
      releaseUpEntry = 1;
      ret = -1;
    }
    if (NOT connectbyTdb().priorValMoveExpr2_) {
      ex_expr::exp_return_type rcc = ex_expr::EXPR_TRUE;
      if (connectbyTdb().priorCondExpr_) {
        rcc = connectbyTdb().priorCondExpr_->eval(pUpEntry->getAtp(), workAtp_);
      }
      if (rcc != ex_expr::EXPR_TRUE)
        ret = -1;
      else
        ret = 0;
    }
  }
  if (releaseUpEntry == 1) pUpEntry->getAtp()->release();

  node->setSentOut();
  return ret;
}

short ExConnectByTcb::moveFromCacheToParent() {
  ex_queue_entry *pUpEntry = qparent_.up->getTailEntry();
  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();

  // pentry_down->copyAtp(initialParentDownAtp_);
  pUpEntry->copyAtp(pentry_down);

  // get atp from cache
  atp_struct *therow = cache_->getNext();
  if (therow == NULL)  // no data
  {
    pUpEntry->upState.status = ex_queue::Q_NO_DATA;
    pUpEntry->upState.setMatchNo(0);
    pUpEntry->upState.parentIndex = pentry_down->downState.parentIndex;
    qparent_.up->insert();
    return 100;
  } else {
    pUpEntry->getAtp()->copyAtp(therow);
    char *ptr = pUpEntry->getAtp()->getTupp(connectbyTdb().returnRowAtpIndex_).getDataPointer();  // for debug only
    pUpEntry->upState.status = ex_queue::Q_OK_MMORE;
    pUpEntry->upState.downIndex = qparent_.down->getHeadIndex();
    pUpEntry->upState.parentIndex = pentry_down->downState.parentIndex;
    qparent_.up->insert();
    return 0;
  }

  return 0;
}

short ExConnectByTcb::moveChildDataToParent(int who, short state) {
  ex_expr::exp_return_type retCode = ex_expr::EXPR_TRUE;
  short ret = 0;
  if (who == 1) {
    ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();

    ex_queue_entry *cUpEntry = qStartWithchild_.up->getHeadEntry();
    ex_queue_entry *pUpEntry = qparent_.up->getTailEntry();

    pUpEntry->copyAtp(pentry_down);
    if (state == 1) {
      // allocat buffer
      short rc = pool_->get_free_tuple(pUpEntry->getAtp()->getTupp(connectbyTdb().returnRowAtpIndex_),
                                       connectbyTdb().outputRowLen_);
      if (rc != 0) {
        return WORK_POOL_BLOCKED;
      }
      // move data to parent
      if (connectbyTdb().leftMoveExpr_) connectbyTdb().leftMoveExpr_->eval(cUpEntry->getAtp(), pUpEntry->getAtp());
      pUpEntry->getAtp()->getTupp(connectbyTdb().fixedPseudoColRowAtpIndex_) = pcoldata_;
      if (connectbyTdb().pathPseudoColRowAtpIndex_ > 0)  // has path required
      {
        pUpEntry->getAtp()->getTupp(connectbyTdb().pathPseudoColRowAtpIndex_) = pathColTupp_;
      }
      // evalute the condExpr
      if (connectbyTdb().condExpr_) {
        retCode = connectbyTdb().condExpr_->eval(pUpEntry->getAtp(), workAtp_);
      }
    }
    if (retCode == ex_expr::EXPR_TRUE) {
      pUpEntry->upState.status = cUpEntry->upState.status;
      pUpEntry->upState.downIndex = qparent_.down->getHeadIndex();
      pUpEntry->upState.parentIndex = pentry_down->downState.parentIndex;
      pUpEntry->upState.setMatchNo(cUpEntry->upState.getMatchNo());

      // move to cache
      if (connectbyTdb().useCache() == TRUE && pUpEntry->upState.status != ex_queue::Q_NO_DATA && doUseCache_ == TRUE) {
        atp_struct *r = allocateAtp(pUpEntry->getAtp()->getCriDesc(), getSpace());
        r->copyAtp(pUpEntry->getAtp());
        cache_->insert(r);
      }
      // insert into parent up queue
      qparent_.up->insert();
      ret = 0;
    } else {
      pUpEntry->getAtp()->release();
      ret = -1;
    }

    qStartWithchild_.up->removeHead();
  }
  if (who == 2) {
    ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();

    ex_queue_entry *cUpEntry = qConnectBychild_.up->getHeadEntry();
    ex_queue_entry *pUpEntry = qparent_.up->getTailEntry();
    char *ptr = NULL;
    pUpEntry->copyAtp(pentry_down);
    if (state == 1) {
      short rc = pool_->get_free_tuple(pUpEntry->getAtp()->getTupp(connectbyTdb().returnRowAtpIndex_),
                                       connectbyTdb().outputRowLen_);
      if (rc != 0) {
        return WORK_POOL_BLOCKED;
      }
      if (connectbyTdb().rightMoveExpr_) connectbyTdb().rightMoveExpr_->eval(cUpEntry->getAtp(), pUpEntry->getAtp());
      ptr = pUpEntry->getAtp()->getTupp(connectbyTdb().returnRowAtpIndex_).getDataPointer();
      pUpEntry->getAtp()->getTupp(connectbyTdb().fixedPseudoColRowAtpIndex_) = pcoldata_;
      if (connectbyTdb().pathPseudoColRowAtpIndex_ > 0)  // has path required
      {
        pUpEntry->getAtp()->getTupp(connectbyTdb().pathPseudoColRowAtpIndex_) = pathColTupp_;
      }
      pUpEntry->upState.status = cUpEntry->upState.status;
      pUpEntry->upState.downIndex = qparent_.down->getHeadIndex();
      pUpEntry->upState.parentIndex = pentry_down->downState.parentIndex;
      // move to cache
      if (connectbyTdb().useCache() == TRUE && doUseCache_ == TRUE) {
        atp_struct *r = allocateAtp(connectbyTdb().criDescUp_, getSpace());
        r->copyAtp(pUpEntry->getAtp());
        cache_->insert(r);
      }
      qparent_.up->insert();
    }

    qConnectBychild_.up->removeHead();
  }

  return ret;
}

////////////////////////////////////////////////////////////////////////////
// This is where the action is.
////////////////////////////////////////////////////////////////////////////
short ExConnectByTcb::work() {
  // if no parent request, return
  if (qparent_.down->isEmpty()) return WORK_OK;

  while (1)  // exit via return
  {
    ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
    if ((pentry_down->downState.request == ex_queue::GET_NOMORE) && step_ != DONE_) {
      step_ = CANCEL_;
    } else if ((pentry_down->downState.request == ex_queue::GET_ALL ||
                pentry_down->downState.request == ex_queue::GET_N) &&
               step_ == INITIAL_) {
      if (cache_ != NULL && connectbyTdb().useCache() == TRUE && doUseCache_ == TRUE) {
        if (cache_->filled() == TRUE) step_ = RETURN_FROM_CACHE_;
      }
    }
    switch (step_) {
      case INITIAL_: {
        if (qStartWithchild_.down->isFull()) return WORK_OK;
        if (qparent_.down->isEmpty()) return WORK_OK;
        currentRoot_ = NULL;
        ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
        ExConnectByPrivateState *pstate = (ExConnectByPrivateState *)pentry_down->pstate;
        leftChildQueueEmpty_ = 0;
        rightChildQueueEmpty_ = 0;

        toRightChildAtp_ = allocateAtp(connectbyTdb().rightDownCriDesc_, getSpace());
        initialParentDownAtp_ = allocateAtp(connectbyTdb().criDescDown_, getSpace());

        const ex_queue::down_request &request = pentry_down->downState.request;

        ex_queue_entry *centry = qStartWithchild_.down->getTailEntry();
        centry->downState.request = ex_queue::GET_ALL;
        centry->downState.requestValue = 11;
        centry->downState.parentIndex = qparent_.down->getHeadIndex();
        initialParentDownAtp_->copyAtp(pentry_down);
        centry->passAtp(pentry_down);

        qStartWithchild_.down->insert();

        if (connectbyTdb().useCache() == TRUE && cache_ == NULL) {
          cache_ = new (getHeap()) ExConnectByCache();
          cache_->setRootLen(connectbyTdb().priorPredHostVarLen_);
        }

        // allocate a root
        if (currentRoot_ == NULL) {
          currentRoot_ = new (getHeap()) ExConnectByTree();
          currentRoot_->priorTuppDataLen_ = connectbyTdb().priorPredHostVarLen_;
        }
        step_ = PROCESS_START_WITH_;
      } break;
      case RETURN_FROM_CACHE_: {
        if (qparent_.up->isFull()) return WORK_OK;
        short rc = moveFromCacheToParent();
        if (rc == -1)
          step_ = ERROR_;
        else if (rc == 100)
          step_ = DONE_;
      } break;
      case PROCESS_START_WITH_: {
        if ((qStartWithchild_.up->isEmpty()) || (qparent_.up->isFull())) return WORK_OK;
        ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
        ex_queue_entry *cUpEntry = qStartWithchild_.up->getHeadEntry();
        ex_queue_entry *pUpEntry = qparent_.up->getTailEntry();
        pentry_down->copyAtp(initialParentDownAtp_);  // maybe we can copy from cUpEntry?
        char *tmpPathBuffer = NULL;
        int tmpPathLen = 0;
        currentLevel_ = 1;
        currentPath_ = NULL;
        switch (cUpEntry->upState.status) {
          case ex_queue::Q_OK_MMORE: {
            // move path data first
            if (connectbyTdb().pathPseudoColRowAtpIndex_ > 0)  // has path required
            {
              if (connectbyTdb().leftPathExpr_) {
                // allocate a buffer for path item
                short rc = pool_->get_free_tuple(workAtp_->getTupp(connectbyTdb().pathExprAtpIndex_),
                                                 connectbyTdb().pathItemLength_);
                if (rc != 0) {
                  errorNum_ = 8042;
                  step_ = ERROR_;
                  break;
                }

                ex_expr::exp_return_type evalRetCode = connectbyTdb().leftPathExpr_->eval(cUpEntry->getAtp(), workAtp_);
                if (evalRetCode != ex_expr::EXPR_OK) {
                  errorNum_ = 8042;
                  step_ = ERROR_;
                  break;
                }
                char *ptr = workAtp_->getTupp(connectbyTdb().pathExprAtpIndex_).getDataPointer();
                ExpTupleDesc *tDesc = connectbyTdb().workCriDesc_->getTupleDescriptor(connectbyTdb().pathExprAtpIndex_);
                Attributes *attr = tDesc->getAttr(0);
                if (attr->getVCIndicatorLength() > 0) {
                  if (attr->getVCIndicatorLength() == sizeof(short))
                    tmpPathLen = *(short *)&ptr[attr->getVCLenIndOffset()];
                  else
                    tmpPathLen = *(int *)&ptr[attr->getVCLenIndOffset()];
                } else
                  tmpPathLen = attr->getLength();

                tmpPathBuffer = new (getHeap()) char[tmpPathLen + 1];
                tmpPathBuffer[tmpPathLen] = 0;
                char *srcptr = ptr + attr->getOffset();
                str_cpy_all(tmpPathBuffer, (const char *)srcptr, tmpPathLen);
              } else {
                errorNum_ = 8042;
                step_ = ERROR_;
                break;
              }
            }
            // put tmpPathBuffer into the node
            atp_struct *newAtp = allocateAtp(cUpEntry->getAtp()->getCriDesc(), getSpace());
            if (newAtp == NULL) {
              errorNum_ = 8042;
              step_ = ERROR_;
              break;
            }
            newAtp->copyAtp(cUpEntry->getAtp());
            ExConnectByTreeNode *rootNode = new (getHeap()) ExConnectByTreeNode(newAtp, 1, getSpace(), getHeap());
            if (tmpPathBuffer != NULL) rootNode->setPathItem(tmpPathBuffer, tmpPathLen);
            currentRoot_->setRoot(rootNode);
            if (tmpPathBuffer != NULL) {
              currentPath_ = new (getHeap()) char[3000];
              memset(currentPath_, 0, 3000);
              currentRoot_->getPath(currentRoot_->getRoot(), currentRoot_->getRoot(), currentPath_, connectbyTdb().del_,
                                    TRUE);
            } else
              currentPath_ = NULL;
            short rc = setPseudoValue(currentLevel_, 0, 0, currentPath_);
            if (rc != 0) {
              errorNum_ = 8043;
              step_ = ERROR_;
              break;
            }

            rc = pool_->get_free_tuple(workAtp_->getTupp(connectbyTdb().priorPredAtpIndex_),
                                       connectbyTdb().priorPredHostVarLen_);
            if (rc != 0) {
              errorNum_ = 8042;
              step_ = ERROR_;
              break;
            }

            if (connectbyTdb().priorValMoveExpr1_) {
              ex_expr::exp_return_type evalRetCode =
                  connectbyTdb().priorValMoveExpr1_->eval(cUpEntry->getAtp(), workAtp_);
              if (evalRetCode != ex_expr::EXPR_OK) {
                short rc1 = moveChildDataToParent(1, 1);
                step_ = PROCESS_START_WITH_;
                break;
              }
            }
            // save the hostvar
            tupp currentHostVarTupp = workAtp_->getTupp(connectbyTdb().priorPredAtpIndex_);
            currentRoot_->currTupp_[currentLevel_ - 1] = workAtp_->getTupp(connectbyTdb().priorPredAtpIndex_);
            if (connectbyTdb().useCache() == TRUE && cache_ != NULL) {
              short duprc = cache_->insertRoot(workAtp_->getTupp(connectbyTdb().priorPredAtpIndex_).getDataPointer());
              if (duprc == 1)  // dup
              {
                // do cleanup
                if (currentRoot_ != NULL) currentRoot_->cleanup(currentLevel_);
                qStartWithchild_.up->removeHead();
                break;
              }
              cache_->insertRootNode(rootNode);
            }
            short rc1 = moveChildDataToParent(1, 1);
            step_ = PROCESS_CONNECT_BY_INIT_;
          } break;

          case ex_queue::Q_NO_DATA: {
            moveChildDataToParent(1, 0);
            step_ = DONE_;
          } break;

          case ex_queue::Q_SQLERROR: {
            qStartWithchild_.down->cancelRequest();
            moveChildDataToParent(1, 0);
            step_ = ERROR_;
          } break;

          case ex_queue::Q_INVALID: {
            ex_assert(0, "ExFirstNTcb::work() Invalid state return by child.");
          } break;
        }  // switch cUpEntry status
      } break;
      case PROCESS_CONNECT_BY_INIT2_: {
        ExConnectByTreeNode *newNode = currentRoot_->getCurrentOutputNode(currentLevel_);
        ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
        if (currentLevel_ == CONNECT_BY_MAX_LEVEL_NUM) {
          errorNum_ = 8040;
          step_ = ERROR_;
          break;
        }
        currentLevel_++;
        ex_queue_entry *centry = qConnectBychild_.down->getTailEntry();
        centry->downState.request = ex_queue::GET_ALL;
        centry->downState.requestValue = 11;
        centry->downState.parentIndex = qparent_.down->getHeadIndex();
        pentry_down->copyAtp(initialParentDownAtp_);
        toRightChildAtp_->copyAtp(pentry_down->getAtp());
        short downqueueindex = connectbyTdb().priorValsValsDownAtpIndex_;
        short workqueueindex = connectbyTdb().priorPredAtpIndex_;
        toRightChildAtp_->getTupp(downqueueindex) = workAtp_->getTupp(workqueueindex);
        centry->passAtp(toRightChildAtp_);
        qConnectBychild_.down->insert();
        step_ = PROCESS_CONNECT_BY_NEXT_LEVEL_;
      } break;
      case PROCESS_CONNECT_BY_INIT_: {
        if (qConnectBychild_.down->isFull()) return WORK_OK;
        ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
        currentLevel_ = 2;
        ex_queue_entry *centry = qConnectBychild_.down->getTailEntry();
        centry->downState.request = ex_queue::GET_ALL;
        centry->downState.requestValue = 11;
        centry->downState.parentIndex = qparent_.down->getHeadIndex();
        // toRightChildAtp_->copyAtp(leftUpEntry->getAtp());
        pentry_down->copyAtp(initialParentDownAtp_);
        toRightChildAtp_->copyAtp(pentry_down->getAtp());
        short downqueueindex = connectbyTdb().priorValsValsDownAtpIndex_;
        short workqueueindex = connectbyTdb().priorPredAtpIndex_;
        toRightChildAtp_->getTupp(downqueueindex) = workAtp_->getTupp(workqueueindex);
        centry->passAtp(toRightChildAtp_);
        qConnectBychild_.down->insert();
        step_ = PROCESS_CONNECT_BY_NEXT_LEVEL_;
      } break;
      case PROCESS_CONNECT_BY_NEXT_LEVEL_:  // get all result rows for this level
      {
        if ((qConnectBychild_.up->isEmpty()) || (qparent_.up->isFull())) return WORK_OK;
        ex_queue_entry *cUpEntry = qConnectBychild_.up->getHeadEntry();
        ex_queue_entry *pUpEntry = qparent_.up->getTailEntry();
        char *tmpPathBuffer = NULL;
        int tmpPathLen = 0;
        switch (cUpEntry->upState.status) {
          case ex_queue::Q_OK_MMORE: {
            // deep first, so should go down, but before go down
            // we need to save the current values
            // allocate buffer to save the get row
            atp_struct *newAtp = allocateAtp(cUpEntry->getAtp()->getCriDesc(), getSpace());
            if (newAtp == NULL) {
              errorNum_ = 8042;
              step_ = ERROR_;
            }
            newAtp->copyAtp(cUpEntry->getAtp());
            // cUpEntry->getAtp()->release(); //have a copy now
            if (connectbyTdb().pathPseudoColRowAtpIndex_ > 0)  // has path required
            {
              if (connectbyTdb().rightPathExpr_) {
                short rc = pool_->get_free_tuple(workAtp_->getTupp(connectbyTdb().pathExprAtpIndex_),
                                                 connectbyTdb().pathItemLength_);
                if (rc != 0) {
                  errorNum_ = 8042;
                  step_ = ERROR_;
                  break;
                }

                ex_expr::exp_return_type evalRetCode =
                    connectbyTdb().rightPathExpr_->eval(cUpEntry->getAtp(), workAtp_);
                if (evalRetCode != ex_expr::EXPR_OK) {
                  errorNum_ = 8042;
                  step_ = ERROR_;
                  break;
                }
                char *ptr = workAtp_->getTupp(connectbyTdb().pathExprAtpIndex_).getDataPointer();
                ExpTupleDesc *tDesc = connectbyTdb().workCriDesc_->getTupleDescriptor(connectbyTdb().pathExprAtpIndex_);
                Attributes *attr = tDesc->getAttr(0);
                if (attr->getVCIndicatorLength() > 0) {
                  if (attr->getVCIndicatorLength() == sizeof(short))
                    tmpPathLen = *(short *)&ptr[attr->getVCLenIndOffset()];
                  else
                    tmpPathLen = *(int *)&ptr[attr->getVCLenIndOffset()];
                } else
                  tmpPathLen = attr->getLength();

                tmpPathBuffer = new (getHeap()) char[tmpPathLen];
                char *srcptr = ptr + attr->getOffset();
                memcpy(tmpPathBuffer, (void *)srcptr, tmpPathLen);
              } else {
                errorNum_ = 8042;
                step_ = ERROR_;
                break;
              }
            }
            ExConnectByTreeNode *newNode =
                new (getHeap()) ExConnectByTreeNode(newAtp, currentLevel_, getSpace(), getHeap());
            currentRoot_->insert(newNode);
            if (tmpPathBuffer != NULL) {
              newNode->setPathItem(tmpPathBuffer, tmpPathLen);
            }
            qConnectBychild_.up->removeHead();
          } break;

          case ex_queue::Q_NO_DATA: {
            moveChildDataToParent(2, 0);
            currentRoot_->advanceOutputPos(currentLevel_);
            step_ = PROCESS_CONNECT_BY_OUTPUT_;
          } break;

          case ex_queue::Q_SQLERROR: {
            qConnectBychild_.down->cancelRequest();
            step_ = ERROR_;
          } break;

          case ex_queue::Q_INVALID: {
            ex_assert(0, "ExConnectByTcb::work() Invalid state return by child.");
            step_ = ERROR_;  // todo
          } break;

        }  // switch cUpEntry status
      } break;

      case PROCESS_CONNECT_BY_OUTPUT_: {
        if (qparent_.up->isFull()) return WORK_OK;

        NABoolean currentIsLeaf_ = currentRoot_->IsEmpty(currentLevel_);
        ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();

        if (currentLevel_ > 2) {
          ExConnectByTreeNode *prevNode = currentRoot_->getPrevNode(currentLevel_ - 1);
          if (prevNode) {
            if (connectbyTdb().pathPseudoColRowAtpIndex_ > 0)  // has path required
            {
              currentPath_ = new (getHeap()) char[3000];
              memset(currentPath_, 0, 3000);
              currentRoot_->getPath(currentRoot_->getRoot(), prevNode, currentPath_, connectbyTdb().del_, TRUE);
            } else
              currentPath_ = NULL;

            setPseudoValue(prevNode->getLevel(), currentIsLeaf_, 0, currentPath_);
            // short workqueueindex = connectbyTdb().priorPredAtpIndex_;
            // workAtp_->getTupp(workqueueindex) = currentRoot_->getHostVarTupp(currentLevel_);
            pentry_down->copyAtp(initialParentDownAtp_);  // maybe we can copy from cUpEntry?

            // output previous level
            short rc1 = moveRightChildDataToParent(prevNode, 1);
            if (rc1 == -1) {
              if (connectbyTdb().priorValMoveExpr2_)
                // no idea what to do here
                ;
              else {
                step_ = PROCESS_CONNECT_BY_UP_LEVEL_;
                return WORK_CALL_AGAIN;
              }
            }
          }
        }

        // drive the traversal
        ExConnectByTreeNode *newNode = currentRoot_->getNextOutputNode(currentLevel_);
        if (newNode != NULL) {
          short rc = pool_->get_free_tuple(workAtp_->getTupp(connectbyTdb().priorPredAtpIndex_),
                                           connectbyTdb().priorPredHostVarLen_);
          if (rc != 0) {
            errorNum_ = 8042;
            step_ = ERROR_;
            break;
          }

          if (connectbyTdb().priorValMoveExpr2_) {
            ex_expr::exp_return_type evalRetCode = connectbyTdb().priorValMoveExpr2_->eval(newNode->getAtp(), workAtp_);
            if (evalRetCode != ex_expr::EXPR_OK) {
              if (currentLevel_ >= 2) {
                ExConnectByTreeNode *prevNode = currentRoot_->getPrevNode(currentLevel_);
                if (prevNode) {
                  if (connectbyTdb().pathPseudoColRowAtpIndex_ > 0)  // has path required
                  {
                    currentPath_ = new (getHeap()) char[3000];
                    currentRoot_->getPath(currentRoot_->getRoot(), prevNode, currentPath_, connectbyTdb().del_, TRUE);
                  } else
                    currentPath_ = NULL;

                  setPseudoValue(prevNode->getLevel(), currentIsLeaf_, 0, currentPath_);
                  pentry_down->copyAtp(initialParentDownAtp_);  // maybe we can copy from cUpEntry?

                  short rc1 = moveRightChildDataToParent(prevNode, 1);
                  if (rc1 == -1) {
                    errorNum_ = 8043;
                    step_ = ERROR_;
                    break;
                  }
                }
              }
              step_ = PROCESS_CONNECT_BY_UP_LEVEL_;
              break;
            }
          }
          if (currentRoot_->currTuppFilled_[currentLevel_ - 1] == 0) {
            currentRoot_->currTupp_[currentLevel_ - 1] = workAtp_->getTupp(connectbyTdb().priorPredAtpIndex_);
            currentRoot_->currTuppFilled_[currentLevel_ - 1] = 1;
          }
          if (connectbyTdb().priorValMoveExpr2_) {
            if (connectbyTdb().noPrior() == FALSE) {
              if (currentRoot_->hasLoop(currentLevel_) == TRUE) {
                if (connectbyTdb().noCycle())
                  step_ = CANCEL_;
                else {
                  errorNum_ = 8038;
                  step_ = ERROR_;
                }
                break;
              }
            }
          }

          step_ = PROCESS_CONNECT_BY_INIT2_;
          return WORK_CALL_AGAIN;
          // we should release the saved tupp?
        } else  // done with current level
        {
          currentRoot_->advanceOutputPos(currentLevel_ - 1);
          step_ = PROCESS_CONNECT_BY_UP_LEVEL_;
        }
      } break;

      case PROCESS_CONNECT_BY_UP_LEVEL_: {
        currentLevel_--;
        if (currentLevel_ == 1)  // over one tree
        {
          currentRoot_->cleanup(currentLevel_);
          step_ = PROCESS_START_WITH_;
        } else
          step_ = PROCESS_CONNECT_BY_OUTPUT_;
      } break;
      case CANCEL_: {
        if (qparent_.up->isFull()) return WORK_OK;

        // cleanup to the level in currentRoot_ tree
        if (currentRoot_ != NULL) currentRoot_->cleanup(currentLevel_);
        currentLevel_ = 1;  // reset

        if (!qConnectBychild_.up->isEmpty()) {
          ex_queue_entry *cUpEntry = qConnectBychild_.up->getHeadEntry();
          qConnectBychild_.up->removeHead();
          if (cUpEntry->upState.status == ex_queue::Q_NO_DATA) rightChildQueueEmpty_ = 1;
        } else
          rightChildQueueEmpty_ = 1;

        if (!qStartWithchild_.up->isEmpty()) {
          ex_queue_entry *cUpEntry = qStartWithchild_.up->getHeadEntry();
          qStartWithchild_.up->removeHead();
          if (cUpEntry->upState.status == ex_queue::Q_NO_DATA) leftChildQueueEmpty_ = 1;
        } else
          leftChildQueueEmpty_ = 1;

        if (leftChildQueueEmpty_ == 1 && rightChildQueueEmpty_ == 1) {
          ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
          ex_queue_entry *up_entry = qparent_.up->getTailEntry();
          up_entry->upState.parentIndex = pentry_down->downState.parentIndex;
          up_entry->upState.setMatchNo(0);
          up_entry->upState.status = ex_queue::Q_NO_DATA;

          qparent_.up->insert();
          step_ = DONE_;
        }
      } break;
      case DONE_: {
        if (qparent_.up->isFull()) return WORK_OK;

        qparent_.down->removeHead();
        if (connectbyTdb().useCache() == TRUE && cache_ != NULL) cache_->cleanRoot();

        if (toRightChildAtp_) {
          toRightChildAtp_->release();
          deallocateAtp(toRightChildAtp_, getSpace());
        }
        if (initialParentDownAtp_) {
          initialParentDownAtp_->release();
          deallocateAtp(initialParentDownAtp_, getSpace());
        }

        step_ = INITIAL_;
        if (currentRoot_ != NULL) {
          currentRoot_->cleanup(CONNECT_BY_MAX_LEVEL_NUM);
          NADELETE(currentRoot_, ExConnectByTree, getHeap());
          currentRoot_ = NULL;
        }
        if (qparent_.down->isEmpty()) {
          if (qStartWithchild_.down->isEmpty() && qConnectBychild_.down->isEmpty()) {
            return WORK_OK;
          } else
            step_ = CANCEL_;
        }
      } break;
      case ERROR_: {
        short rc = 0;
        if (handleError(rc)) return rc;
        step_ = CANCEL_;
      } break;
    }  // switch
  }    // while

  return 0;
}

short ExConnectByTcb::cancel() {
  // if no parent request, return
  if (qparent_.down->isEmpty()) return WORK_OK;
  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
  ExConnectByPrivateState *pstate = (ExConnectByPrivateState *)pentry_down->pstate;

  if (pentry_down->downState.request == ex_queue::GET_NOMORE) {
    step_ = CANCEL_;
    qStartWithchild_.down->cancelRequest();
    qConnectBychild_.down->cancelRequest();
  }

  return WORK_OK;
}

short ExConnectByTcb::handleError(short &rc) {
  if (qparent_.up->isFull()) return WORK_OK;
  ex_queue_entry *pentry_down = qparent_.down->getHeadEntry();
  ex_queue_entry *up_entry = qparent_.up->getTailEntry();
  ComDiagsArea *diagsArea = NULL;
  diagsArea = ComDiagsArea::allocate(this->getGlobals()->getDefaultHeap());
  ExRaiseSqlError(getHeap(), &diagsArea, (ExeErrorCode)(errorNum_));
  pentry_down->setDiagsArea(diagsArea);
  up_entry->copyAtp(pentry_down);
  up_entry->upState.parentIndex = pentry_down->downState.parentIndex;
  up_entry->upState.downIndex = qparent_.down->getHeadIndex();
  up_entry->upState.status = ex_queue::Q_SQLERROR;
  qparent_.up->insert();

  return 0;
}
///////////////////////////////////////////////////////////////////////////////
// Constructor and destructor for sort_private_state
///////////////////////////////////////////////////////////////////////////////
ExConnectByPrivateState::ExConnectByPrivateState(const ExConnectByTcb *tcb) {}
ExConnectByPrivateState::~ExConnectByPrivateState(){};

ex_tcb_private_state *ExConnectByPrivateState::allocate_new(const ex_tcb *tcb) {
  return new (((ex_tcb *)tcb)->getSpace()) ExConnectByPrivateState((ExConnectByTcb *)tcb);
};

int ExConnectByTree::insert(ExConnectByTreeNode *n) {
  ExConnectByTreeNode *parentNode = NULL;
  short level = n->getLevel();
  if (level == 1) return -1;  // never should have level 1 pass in
  Queue *currentNodeList = currArray[level - 1];
  if (level == 2)  // parent is root
    parentNode = root_;
  else {
    Queue *parentNodeList = currArray[level - 2];
    parentNode = (ExConnectByTreeNode *)parentNodeList->get(curPos_[level - 2] - 1);
  }
  n->setParent(parentNode);
  if (currentNodeList == NULL)  // this is the first time for this level
  {
    currentNodeList = new (collHeap()) Queue(collHeap());
    currArray[level - 1] = currentNodeList;
  }
  currentNodeList->insert(n);
  last_[level - 1]++;
  return 0;
}

ExConnectByTreeNode *ExConnectByTree::getCurrentOutputNode(short level) {
  if (level == 1) return NULL;
  Queue *currentNodeList = currArray[level - 1];
  ExConnectByTreeNode *ret = NULL;
  if (currentNodeList == NULL) return NULL;
  if (currentNodeList->entries() == 0) return NULL;
  ret = (ExConnectByTreeNode *)currentNodeList->get(curPos_[level - 1] - 1);
  return ret;
}

ExConnectByTreeNode *ExConnectByTree::getNextOutputNode(short level) {
  if (level == 1) return NULL;
  Queue *currentNodeList = currArray[level - 1];
  ExConnectByTreeNode *ret = NULL;
  if (currentNodeList == NULL) return NULL;
  if (currentNodeList->entries() == 0) return NULL;
  if (curPos_[level - 1] >= last_[level - 1]) {
    // free all nodes in this level
    for (int i = 0; i < currentNodeList->entries(); i++) {
      ExConnectByTreeNode *item = (ExConnectByTreeNode *)currentNodeList->get(i);
      item->cleanup();
      NADELETE(item, ExConnectByTreeNode, collHeap());
    }
    curPos_[level - 1] = 0;
    last_[level - 1] = 0;        // clear the current level
    lastOutput_[level - 1] = 0;  // clear the current level
    NADELETE(currentNodeList, Queue, collHeap());
    currArray[level - 1] = NULL;
    currTupp_[level - 1].release();
    currTuppFilled_[level - 1] = 0;
    return NULL;
  }
  ret = (ExConnectByTreeNode *)currentNodeList->get(curPos_[level - 1]);
  curPos_[level - 1]++;
  return ret;
}

ExConnectByTreeNode *ExConnectByTree::getPrevNode(short level) {
  if (level == 1) return NULL;
  Queue *currentNodeList = currArray[level - 1];
  ExConnectByTreeNode *ret = NULL;
  if (currentNodeList == NULL) return NULL;
  if (currentNodeList->entries() == 0) return NULL;
  for (int i = 0; i < lastOutput_[level - 1]; i++) {
    ret = (ExConnectByTreeNode *)currentNodeList->get(i);
    if (ret->getSentOut() == 0) return ret;
  }
  return ret;
}
void ExConnectByTreeNode::cleanup() {
  theRow_->release();
  deallocateAtp(theRow_, getMySpace());
  if (pathItem_) {
    NADELETEBASIC(pathItem_, getMyHeap());
    pathItem_ = NULL;
  }
  theRow_ = NULL;
}

// deep first recursive call
void ExConnectByTree::getPath(ExConnectByTreeNode *r, ExConnectByTreeNode *n, char *out, NAString del,
                              NABoolean getPathStarted) {
  int pathItemLen = 0;
  // from root to node n
  str_cat(out, del.data(), out);
  char *charptr = r->getPathItem(&pathItemLen);
  pathItemLen = r->getPathLen();
  char tmpStr[3000];
  memset(tmpStr, 0, 3000);
  if (pathItemLen > 0 && charptr != NULL) {
    memcpy(tmpStr, charptr, pathItemLen);
    str_cat(out, tmpStr, out);
  } else {
    tmpStr[0] = 0;
  }

  ExConnectByTreeNode *nextNode = NULL;

  int level = r->getLevel() + 1;

  // finish condition:
  if (r == n || level == n->getLevel() + 1)
    return;
  else {
    // get next level first node
    nextNode = getCurrentOutputNode(level);
    getPath(nextNode, n, out, del, FALSE);
  }
}

NABoolean ExConnectByTree::tuppIsSame(tupp t1, tupp t2) {
  char *d1 = t1.getDataPointer();
  char *d2 = t2.getDataPointer();
  if (str_cmp(d1, d2, priorTuppDataLen_) == 0) return TRUE;
  return FALSE;
}
NABoolean ExConnectByTree::hasLoop(int n) {
  // check the currTupp_ for dup
  for (int i = 0; i < n - 1; i++) {
    if (tuppIsSame(currTupp_[i], currTupp_[n - 1]) == TRUE) return TRUE;
  }
  return FALSE;
}

// do clean up from root to level
void ExConnectByTree::cleanup(short level) {
  for (short i = 0; i < level; i++) {
    Queue *currentNodeList = currArray[i];
    if (currentNodeList == NULL) continue;

    for (int j = 0; j < currentNodeList->entries(); j++) {
      ExConnectByTreeNode *item = (ExConnectByTreeNode *)currentNodeList->get(j);
      item->cleanup();
      NADELETE(item, ExConnectByTreeNode, collHeap());
    }
    curPos_[i] = 0;
    last_[i] = 0;        // clear the current level
    lastOutput_[i] = 0;  // clear the current level
    NADELETE(currentNodeList, Queue, collHeap());
    currArray[i] = NULL;
    currTupp_[i].release();
    currTuppFilled_[i] = 0;
  }
}

short ExConnectByCache::insert(atp_struct *row) {
  theRows_->insert(row);
  return 0;
}

short ExConnectByCache::insertRoot(char *ptr) {
  // search for dup of n
  for (int i = 0; i < theRoots_->numEntries(); i++) {
    char *r = (char *)theRoots_->get(i);
    if (str_cmp(r, ptr, rootLen_) == 0) return 1;
  }
  theRoots_->insert(ptr);
  return 0;
}

short ExConnectByCache::insertRootNode(ExConnectByTreeNode *n) {
  theRootNodes_->insert(n);
  return 0;
}
