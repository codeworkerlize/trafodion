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
#ifndef EX_CONNECT_BY_H
#define EX_CONNECT_BY_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExConnectBy.h
 * Description:
 *
 *
 * Created:      8/10/2019
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
#include "common/Int64.h"
#include "common/NABoolean.h"
#include "comexe/ComTdbConnectBy.h"

#define CONNECT_BY_MAX_LEVEL_NUM 500

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ExConnectByTdb;
class ExConnectByTree;
class ExConnectByTreeNode;
class ExConnectByCache;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class ex_tcb;

// -----------------------------------------------------------------------
// ExConnectBydb
// -----------------------------------------------------------------------
class ExConnectByTdb : public ComTdbConnectBy {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExConnectByTdb() {}

  virtual ~ExConnectByTdb() {}

  // ---------------------------------------------------------------------
  // Build a TCB for this TDB. Redefined in the Executor project.
  // ---------------------------------------------------------------------
  virtual ex_tcb *build(ex_globals *globals);

 private:
  // ---------------------------------------------------------------------
  // !!!!!!! IMPORTANT -- NO DATA MEMBERS ALLOWED IN EXECUTOR TDB !!!!!!!!
  // *********************************************************************
  // The Executor TDB's are only used for the sole purpose of providing a
  // way to supplement the Compiler TDB's (in comexe) with methods whose
  // implementation depends on Executor objects. This is done so as to
  // decouple the Compiler from linking in Executor objects unnecessarily.
  //
  // When a Compiler generated TDB arrives at the Executor, the same data
  // image is "cast" as an Executor TDB after unpacking. Therefore, it is
  // a requirement that a Compiler TDB has the same object layout as its
  // corresponding Executor TDB. As a result of this, all Executor TDB's
  // must have absolutely NO data members, but only member functions. So,
  // if you reach here with an intention to add data members to a TDB, ask
  // yourself two questions:
  //
  // 1. Are those data members Compiler-generated?
  //    If yes, put them in the ComTdbConnectBy instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//
// Task control block
//
class ExConnectByTcb : public ex_tcb {
  friend class ExConnectByTdb;
  friend class ExConnectByPrivateState;

  enum ConnectByStep {
    INITIAL_,
    PROCESS_START_WITH_,
    PROCESS_CONNECT_BY_,
    PROCESS_CONNECT_BY_INIT_,
    PROCESS_CONNECT_BY_INIT2_,
    PROCESS_CONNECT_BY_NEXT_LEVEL_,
    PROCESS_CONNECT_BY_UP_LEVEL_,
    PROCESS_CONNECT_BY_CLEAR_UP_LEVEL_,
    PROCESS_CONNECT_BY_OUTPUT_,
    PROCESS_CONNECT_BY_SAVE_ROW_,
    RETURN_FROM_CACHE_,
    DONE_,
    CANCEL_,
    ERROR_
  };
  const ex_tcb *childStartWithTcb_;
  const ex_tcb *childConnectByTcb_;

  ex_queue_pair qparent_;
  ex_queue_pair qStartWithchild_;
  ex_queue_pair qConnectBychild_;

  ConnectByStep step_;

  atp_struct *workAtp_;

  // Stub to cancel() subtask used by scheduler.
  static ExWorkProcRetcode sCancel(ex_tcb *tcb) { return ((ExConnectByTcb *)tcb)->cancel(); }
  static ExWorkProcRetcode sWork(ex_tcb *tcb) { return ((ExConnectByTcb *)tcb)->work(); }

  char *workdata_;
  char *pathdata_;
  char *priorHostVarData_;

  Int32 currentLevel_;
  char *currentPath_;

  tupp pcoldata_;
  tupp pathColTupp_;
  tupp priorValueTuple_;

  atp_struct *toRightChildAtp_;
  atp_struct *initialParentDownAtp_;

  short leftChildQueueEmpty_;
  short rightChildQueueEmpty_;

 public:
  // Constructor
  ExConnectByTcb(const ExConnectByTdb &connectby_tdb, const ex_tcb &s_child_tcb, const ex_tcb &c_child_tcb,
                 ex_globals *glob);

  ~ExConnectByTcb();

  short moveChildDataToParent(int who, short state);
  short moveRightChildDataToParent(ExConnectByTreeNode *node, short state);
  short moveFromCacheToParent();

  void freeResources();  // free resources

  short handleError(short &rc);
  short work();                     // when scheduled to do work
  virtual void registerSubtasks();  // register work procedures with scheduler
  short cancel();                   // for the fickle.

  inline ExConnectByTdb &connectbyTdb() const { return (ExConnectByTdb &)tdb; }

  ex_queue_pair getParentQueue() const { return qparent_; }

  virtual Int32 numChildren() const { return 2; }
  virtual const ex_tcb *getChild(Int32 p) const {
    if (p == 0) return childStartWithTcb_;
    if (p == 1) return childConnectByTcb_;
    return NULL;
  }

  short setPseudoValue(Int32 level, Int32 isleaf, Int32 iscycle, char *path);

  ExConnectByTree *currentRoot_;

  ExConnectByCache *cache_;

  short errorNum_;

  NABoolean doUseCache_;
};

///////////////////////////////////////////////////////////////////
class ExConnectByPrivateState : public ex_tcb_private_state {
  friend class ExConnectByTcb;

 public:
  ExConnectByPrivateState(const ExConnectByTcb *tcb);  // constructor
  ex_tcb_private_state *allocate_new(const ex_tcb *tcb);
  ~ExConnectByPrivateState();  // destructor
};

#if 1
class ExConnectByTreeNode : public NABasicObject {
 public:
  ExConnectByTreeNode(atp_struct *t, short level, CollHeap *space, CollHeap *h)  // constructor
  {
    theRow_ = t;
    level_ = level;
    alreadySentOut_ = 0;
    space_ = space;
    pathItem_ = NULL;
    heap_ = h;
    val_ = NULL;
    valLen_ = 0;
    atpidx_ = 0;
  }
  ~ExConnectByTreeNode() {}  // destructor
  void setValue(char *v) { val_ = v; }
  char *getValue() { return val_; }
  void setParent(ExConnectByTreeNode *p) { parent_ = p; }
  ExConnectByTreeNode *getParent() { return parent_; }
  void setSbyling(ExConnectByTreeNode *s) { sybling_ = s; }
  ExConnectByTreeNode *getSbyling() { return sybling_; }
  void setSentOut() { alreadySentOut_ = 1; }
  short getSentOut() { return alreadySentOut_; }
  short getLevel() { return level_; }
  atp_struct *getAtp() { return theRow_; }
  void cleanup();
  CollHeap *getMySpace() { return space_; }
  CollHeap *getMyHeap() { return heap_; }
  void setPathItem(char *p, Int32 len) {
    pathItem_ = p;
    pathLen_ = len;
  }
  char *getPathItem(Int32 *len) {
    return pathItem_;
    *len = pathLen_;
  }
  Int32 getPathLen() { return pathLen_; }

  char *getPriorItem() { return val_; }
  Int32 getPriorLen() { return valLen_; }
  void setPriorLen(Int32 l) { valLen_ = l; }

  void setPriorItem(char *p, Int32 len) {
    val_ = p;
    valLen_ = len;
  }

  NABoolean isEqual(ExConnectByTreeNode *r) {
    char *ptr = r->getAtp()->getTupp(atpidx_).getDataPointer();
    char *ptr1 = getAtp()->getTupp(atpidx_).getDataPointer();
    if (ptr == ptr1) return TRUE;
    if (str_cmp(ptr, ptr1, valLen_) == 0) return TRUE;
    return FALSE;
  }

 private:
  char *val_;
  Int32 valLen_;
  char *pathItem_;
  Int32 pathLen_;
  Int32 level_;
  Int32 type_;
  ExConnectByTreeNode *sybling_;
  ExConnectByTreeNode *parent_;
  Queue *children_;
  short alreadySentOut_;
  CollHeap *space_;
  CollHeap *heap_;
  short atpidx_;

  // the data row
  // Oracle is deep-first traversal, so when a row is get from right child
  // Only the first row can be output, others need to stay in the tree
  // and sent out in a proper time
  // so this is the copy of the tupp
  atp_struct *theRow_;
};

class ExConnectByTree : public NABasicObject {
 public:
  ExConnectByTree()  // constructor
  {
    root_ = NULL;
    rootPathBuffer_ = NULL;
    priorTuppDataLen_ = 0;
    for (int i = 0; i < CONNECT_BY_MAX_LEVEL_NUM; i++) {
      currArray[i] = NULL;
      last_[i] = 0;
      lastOutput_[i] = 0;
      curPos_[i] = 0;
      currTuppFilled_[i] = 0;
    }
  }

  ~ExConnectByTree() {}

  // insert a node into the tree
  // if duplicated, it is a cycle
  Int32 insert(ExConnectByTreeNode *n);
  void setRoot(ExConnectByTreeNode *r) { root_ = r; }
  ExConnectByTreeNode *getRoot() { return root_; }

  // generate the path for SYS_CONNECT_BY_PATH
  void getPath(ExConnectByTreeNode *r, ExConnectByTreeNode *n, char *out, NAString del, NABoolean s);

  NABoolean hasLoop(int level);
  NABoolean tuppIsSame(tupp a, tupp b);

  // ExConnectByTreeNode * getNextSybling(ExConnectByTreeNode *n, Int32 level) { return NULL; }
  ExConnectByTreeNode *getPrevNode(short level);
  ExConnectByTreeNode *getNextOutputNode(short level);
  ExConnectByTreeNode *getCurrentOutputNode(short level);
  NABoolean IsEmpty(short level) {
    if (curPos_[level - 1] >= last_[level - 1]) return TRUE;
    return FALSE;
  }

  tupp getHostVarTupp(short level) { return currTupp_[level - 1]; }
  short currTuppFilled_[CONNECT_BY_MAX_LEVEL_NUM];
  void advanceOutputPos(short level) {
    if (lastOutput_[level - 1] < last_[level - 1]) lastOutput_[level - 1]++;
  }

  void cleanup(short level);

  char *rootPathBuffer_;
  Int32 priorTuppDataLen_;
  tupp currTupp_[CONNECT_BY_MAX_LEVEL_NUM];

 private:
  ExConnectByTreeNode *root_;

  // ugly implementation for now
  Queue *currArray[CONNECT_BY_MAX_LEVEL_NUM];
  Int32 last_[CONNECT_BY_MAX_LEVEL_NUM];
  Int32 lastOutput_[CONNECT_BY_MAX_LEVEL_NUM];
  Int32 curPos_[CONNECT_BY_MAX_LEVEL_NUM];
};

#endif

class ExConnectByCache : public NABasicObject {
 public:
  ExConnectByCache()  // constructor
  {
    theRows_ = new (collHeap()) Queue(collHeap());
    theRoots_ = new (collHeap()) Queue(collHeap());
    theRootNodes_ = new (collHeap()) Queue(collHeap());
    currPos_ = 0;
    rootLen_ = 0;
  }
  ~ExConnectByCache() {
    NADELETE(theRows_, Queue, collHeap());
    NADELETE(theRoots_, Queue, collHeap());
    NADELETE(theRootNodes_, Queue, collHeap());
  }

  short insert(atp_struct *r);
  short insertRoot(char *n);
  short insertRootNode(ExConnectByTreeNode *n);
  void setRootLen(Int32 l) { rootLen_ = l; }
  void cleanRoot() {
    for (int i = 0; i < theRoots_->entries(); i++) theRoots_->remove();
  }

  atp_struct *getNext() {
    Int32 prevPos = currPos_;
    currPos_++;
    if (currPos_ >= theRows_->entries()) {
      currPos_ = 0;
      return NULL;
    }
    return (atp_struct *)(theRows_->get(prevPos));
  }

  ExConnectByTreeNode *getNextRoot() {
    Int32 prevPos = currRootPos_;
    currRootPos_++;
    if (currRootPos_ >= theRootNodes_->entries()) {
      currRootPos_ = 0;
      return NULL;
    }
    return (ExConnectByTreeNode *)(theRootNodes_->get(prevPos));
  }
  NABoolean filled() {
    if (theRows_->entries() > 0)
      return TRUE;
    else
      return FALSE;
  }

 private:
  Queue *theRows_;
  Queue *theRoots_;
  Queue *theRootNodes_;
  Int32 currPos_;
  Int32 currRootPos_;
  Int32 rootLen_;
};
#endif
