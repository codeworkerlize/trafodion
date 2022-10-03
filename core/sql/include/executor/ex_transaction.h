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
#ifndef EX_TRANSACTION_H
#define EX_TRANSACTION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ex_transaction.h
 * Description:  TCB/TDB for transaction-related SQL statements
 *               and ExTransaction object that represents a TMF transaction
 *               
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "ex_stdh.h"
#include "comexe/ComTdb.h"
#include "ex_tcb.h"
#include "ex_error.h"
#include "common/Int64.h"


#define NULL_SAVEPOINT_ID -1

struct TmfPhandle_Struct {
  short  Data[10];
};


class CliGlobals;
class ExpHbaseInterface;


enum SavepointState {
  SP_SPNONE          = 0,
  SP_GENERATED       = 1,
  SP_TO_COMMIT       = 2,
  SP_TO_ROLLBACK     = 3,
  SP_COMMIT_FAILED   = 4,
  SP_ROLLBACK_FAILED = 5
};


class ExSavepointElement
{
public:
  ExSavepointElement() {}
  ExSavepointElement(NAString svptName, Int64 svptId, SavepointState state) :
    savepointName_(svptName),
    savepointId_(svptId),
    savepointState_(state)
  {}

  inline NAString getElemSavepointName() { return savepointName_; }
  inline Int64 getElemSavepointId() { return savepointId_; }
  inline SavepointState getElemSavepointState() { return savepointState_; }

  void setElemSavepointId(Int64 svptId) { savepointId_ = svptId; }
  void setElemSavepointState(SavepointState newState) { savepointState_ = newState; }

private:
  NAString savepointName_;
  Int64 savepointId_;
  SavepointState savepointState_;
};

class ExSavepointStack : public NAList<ExSavepointElement>
{
public:
  ExSavepointStack(CollHeap* h = NULL) :
    lastSvptId_(-1),
    NAList<ExSavepointElement>(h ? h : STMTHEAP) {};

  short removeByName(NAString svptName)
  {
    short ret = -1;
    for (int i = 0; i < entries(); i++)
    {
      if (at(i).getElemSavepointName() == svptName)
      {
        removeAt(i);
        ret = 0;
      }
    }

    return ret;
  }

  void push(NAString svptName, Int64 svptId, SavepointState state)
  {
    ExSavepointElement svpt(svptName, svptId, state);
    removeByName(svptName);
    append(svpt);
    lastSvptId_ = svptId;
  }

  short pop(ExSavepointElement &elem)
  {
    return getLast(elem) ? 0 : -1;
  }

  short popToSvpt(NAString svptName)
  {
    short ret = 0;
    int cnt = 0;

    for (; cnt < entries(); cnt++)
    {
      if (at(entries() - cnt - 1).getElemSavepointName() == svptName)
        break;
    }

    // no savepoint found
    if (cnt >= entries())
      return -1;

    ExSavepointElement elem;
    for (int i = 0; 0 == ret && i < cnt; i++)
      ret += pop(elem);

    return ret;
  }

  Int64 getCurrentSvptId()
  {
    if (isEmpty())
      return NULL_SAVEPOINT_ID;

    return at(entries()-1).getElemSavepointId();
  }

  Int64 getCurrentPSvptId()
  {
    if (entries() > 1)
      return at(entries() - 2).getElemSavepointId();
    else
      return NULL_SAVEPOINT_ID;
  }

  Int64 getCurrentPPSvptId()
  {
    if (entries() > 2)
      return at(entries() - 3).getElemSavepointId();
    else
      return NULL_SAVEPOINT_ID;
  }

  NABoolean setCurrentSvptId(Int64 svptId)
  {
    if (isEmpty())
      return FALSE;

    at(entries() - 1).setElemSavepointId(svptId);
    return TRUE;
  }

  SavepointState getCurrentSvptState()
  {
    if (isEmpty())
      return SP_SPNONE;

    return at(entries()-1).getElemSavepointState();
  }

  NABoolean setCurrentSvptState(SavepointState svptState)
  {
    if (isEmpty())
      return FALSE;

    at(entries() - 1).setElemSavepointState(svptState);
    return TRUE;
  }

  void resetLastSvptId() { lastSvptId_ = -1; }
  Int64 getLastSvptId() { return lastSvptId_; }

private:
  Int64 lastSvptId_;
};

class ExTransaction : public NABasicObject
{
public:

  // this flag is part of savepoint id to deliver flags to coprocessor side
  enum SavepointFlags {
    SP_FLAG_IS_IMPLICIT = 0x0100000000000000,
    SP_FLAG_UNKNOWN     = 0x8000000000000000//should not be used
  };

  ExTransaction(CliGlobals * cliGlob, CollHeap *heap);
  virtual ~ExTransaction();
  
  //////////////////////////////////////////////////////////////
  // Methods to perform transaction related operations.
  // Used for explicit (called by application via cli calls) 
  // and implicit (called internally when a statement requires
  // a transaction and one is not already running) transaction
  // operations.
  //////////////////////////////////////////////////////////////
  short beginTransaction();
  short suspendTransaction();
  short resumeTransaction();

  short joinTransaction(Int64 transID);
  short suspendTransaction(Int64 transID);

  //////////////////////////////////////////////////////////////
  // RollbackStatement ALWAYS rollbacks Xn. Used unless stmt
  // atomicity support is in.
  // RollbackTransaction
  // only rollbacks if executor started the Xn.
  //////////////////////////////////////////////////////////////
  short rollbackStatement();
  short rollbackTransaction(NABoolean isWaited = FALSE);
  short rollbackTransactionWaited();
  short doomTransaction();
  short waitForRollbackCompletion(Int64 transid);

  void cleanupTransaction();
  short commitTransaction();

  short inheritTransaction();
  short validateTransaction();
  short setProcessTransToContextTrans
    (TmfPhandle_Struct * oldProcessTransId, /* out */
     NABoolean & processTransactionChanged /* out */);
  short resetProcessTrans(TmfPhandle_Struct * oldProcessTransId);

  NABoolean xnInProgress()	{ return getTransMode()->xnInProgress(); }

  TransMode * getTransMode()	{ return transMode_; }
  TransMode * getUserTransMode();
  TransMode * getTransModeNext(){ return &transModeNext_; }
  short setTransMode(TransMode * trans_mode);

  SavepointState getSavepointState();
  void setSavepointState(SavepointState s);
  NABoolean savepointInProgress();

  short commitSavepoint(Int64 savepointId, Int64 parentSvptId);
  short rollbackSavepoint(Int64 savepointId);
  short rollbackToSavepoint(NAString svptName);

  // Soln 10-050210-4624
  /* This method retrieves from TMF and returns the TCBREF associated with the
   * currently active transaction. If the optional parameters, transId and
   * txHandle, are supplied, the transaction identifier and handle also are
   * returned.
   */
  short getCurrentXnId(Int64 *tcbref, 
                       Int64 *transId = 0, short *txHandle = NULL);

  short getCurrentSvptIdWithFlag(Int64 *svptId, Int64 *pSvptId);

  short getCurrentTxHandle(short * txHandle);

  NABoolean getIsPropropagateTX();

  // this method returns the current Xn Id as known to executor.
  Int64 getExeXnId()		{ return exeXnId_; }

  // Soln 10-050210-4624
  // This method returns the id of the current transaction.
  Int64 getTransid()            { return transid_;}

  // this method returns the current savepoint id, if dp2 savepoint is
  // being used for this stmt. -1, if they are not.
  Int64 getSavepointId()
  {
    return savepointStack_->getCurrentSvptId();
  }
  Int64 getPSavepointId()
  {
    return savepointStack_->getCurrentPSvptId();
  }
  Int64 getPPSavepointId()
  {
    return savepointStack_->getCurrentPPSvptId();
  }
  Int64 getSavepointIdWithFlag();
  Int64 getPSavepointIdWithFlag();
  Int64 getPPSavepointIdWithFlag();

  NAString getStackSavepointName()
  {
    if (savepointStack_->isEmpty())
      return NAString("None");

    return (savepointStack_->at(savepointStack_->entries()-1).getElemSavepointName());
  }

  void resetStackLastSvptId() { savepointStack_->resetLastSvptId(); }
  Int64 getStackLastSvptId() { return savepointStack_->getLastSvptId(); }

  // this method generates a savepoint id. Its a juliantimestamp, for now.
  // If a transaction is not in progress, no savepoint id is generated.
  //void generateSavepointId(NABoolean implicitSP = FALSE);
  short pushSavepoint(NAString svptName)
  {
    Int64 svptId = NULL_SAVEPOINT_ID;
    if (xnInProgress())
    {
      svptId = 1 + (getStackLastSvptId() > implicitSavepointId_ ?
                                  getStackLastSvptId() : implicitSavepointId_);

      savepointStack_->push(svptName, svptId, SP_GENERATED);
      setSavepointState(SP_GENERATED);

      return 0;
    }

    return -1;
  }
  short popSavepoint()
  {
    ExSavepointElement elem;
    return savepointStack_->pop(elem);
  }
  short removeSavepoint(NAString svptName)
  {
    return savepointStack_->removeByName(svptName);
  }
  void clearSavepoint()
  {
    savepointStack_->clear();
    savepointStack_->resetLastSvptId();
  }

  void setSavepointId(Int64 sid)
  { savepointStack_->setCurrentSvptId(sid); }

  NABoolean exeStartedXn() 	{ return exeStartedXn_; }

  NABoolean exeStartedXnDuringRecursiveCLICall() 	
    { return exeStartedXnDuringRecursiveCLICall_; }

  NABoolean &implicitXn() 	{ return implicitXn_; }

  NABoolean userEndedExeXn() 	{ return userEndedExeXn_; }

  void setTransId(Int64 transid);
  void disableAutoCommit();
  void enableAutoCommit();
  
  // returns TRUE, if auto commit is on. Otherwise, returns FALSE.
  NABoolean autoCommit()
  {
    if ((transMode_) &&
	(transMode_->autoCommit() == TransMode::ON_))
      return TRUE;
    else
      return FALSE;
  }

 // returns TRUE, if multi commit is on. Otherwise, returns FALSE.
  NABoolean multiCommit()
  {
    if ((transMode_) &&
	(transMode_->multiCommit() == TransMode::MC_ON_))
      return TRUE;
    else
      return FALSE;
  }
  
  ComDiagsArea *getDiagsArea() {return transDiagsArea_;}
  inline CliGlobals *getCliGlobals() { return cliGlob_;}

  void updateROVal(Int16 roval) 
  {
   roVal_ = roval;
  }
  Int16 getROVal()            { return roVal_;}

  void updateRBVal(Int16 rbval) 
  {
   rbVal_ = rbval;
  }
  Int16 getRBVal()            { return rbVal_;}

  void updateAIVal(Int32 aival) 
  {
   aiVal_ = aival;
  }
  Int32 getAIVal()            { return aiVal_;}

  void setMayAlterDb(NABoolean x)
  {
    mayAlterDb_ = x;
  }

  NABoolean mayAlterDb()  { return mayAlterDb_; }
  
  void setMayHoldLock(NABoolean x)
  {
    mayHoldLock_ = x;
  }

  NABoolean mayHoldLock()  { return mayHoldLock_; }

  void resetXnState0();
  void saveAndResetXnState0();
  void saveXnState0();
  void restoreXnState0();
  void resetXnState();
  void resetSavedXnState();

  // implicit savepoint when autocommit savepoint on
  Int64 getImplicitSavepointId()        { return implicitSavepointId_; }
  void setImplicitSavepointId(Int64 sid) { implicitSavepointId_ = sid; }
  void setImplicitPSavepointId(Int64 psid) { implicitPSavepointId_ = psid; }
  void generateImplicitSavepointId();
  Int64 getImplicitSavepointIdWithFlag();
  Int64 getImplicitPSavepointIdWithFlag() { return implicitPSavepointId_; }

  SavepointState getImplicitSavepointState() { return implicitSavepointState_; }
  void setImplicitSavepointState(SavepointState s);
  NABoolean implicitSavepointInProgress()	{ return (implicitSavepointState_ != SP_SPNONE); }

  short commitImplicitSavepoint(Int64 implicitSvptId, Int64 parentSvptId);
  short rollbackImplicitSavepoint(Int64 implicitSvptId);

  NABoolean getAutoCommitSavepointOn();

private:

  // pointer to executor globals. Used to get to the open
  // cursor list. This list is needed to close all open
  // cursors at commit/rollback time.
  CliGlobals * cliGlob_;

  // transaction attributes 
  TransMode * transMode_;

  // user transaction attributes that may be different to default trans mode
  TransMode * userTransMode_;

  TransMode   transModeNext_;	// mode to use AFTER this trans completes
                                // (see ComTdbControl.cpp)

  // Diagnostics Area for the transaction operation.
  // The transaction methods such as beginTransaction()... can be
  // called by the cli directly. This member is provided to
  // hold the diagnostics for such callers, who's request
  // are not sent thro' the conventional down queues.
  ComDiagsArea *transDiagsArea_;
  ComCondition* errorCond_;
  CollHeap* heap_;
  
  short local_or_encompassing_trans;

  // TRUE, if executor started the transaction.
  NABoolean exeStartedXn_;

  // The next flag is set if the executor started this transaction
  // during a recursive call (e.g. a metadata lookup or resource
  // fork read during fixup). Such implicitly started transactions
  // must be committed before returning from the top-level CLI
  // call, otherwise data integrity problems could result in
  // multi-context applications.
  NABoolean exeStartedXnDuringRecursiveCLICall_;  

  // TRUE, if executor started the transaction implicitely.
  // FALSE, if the app started Xn by calling CLI or by starting
  // it in the application.
  NABoolean implicitXn_;

  // set to TRUE, if user application ended (committed/aborted/started)
  // an executor started Xn (xnInProgress_ is TRUE).
  // According to ANSI, only a SQL agent that started the XN can
  // end it.Int32 ExTransaction::checkAndWaitSnapshotInProgress()
  // Exe will stay in this state until user application 'ends' the
  // transaction by calling SQL commit/rollback.
  NABoolean userEndedExeXn_;

  // transaction id as known to executor, if a Xn is running.
  // Otherwise, set to -1.
  Int64 exeXnId_;

  // Soln 10-050210-4624
  // The actual transaction id as known to the executor. Note that exeXnId_ is
  // actually the TCBREF associated with the transaction. exeXnId_ will be used
  // in all the messaging with DP2, MXCMP and MXESP. 
  // This field will be used for interacting with TMF.
  Int64 transid_;

  // user named savepoints
  ExSavepointStack * savepointStack_;

  Int64 implicitSvptBatchCommitCount_;

  Int64 implicitPSavepointId_;
  Int64 implicitSavepointId_;
  SavepointState implicitSavepointState_;

  TmfPhandle_Struct exeTxHandle_;

  // set to TRUE, if the autoCommit mode was temporarily
  // disabled because a BEGIN WORK statement was issued.
  NABoolean autoCommitDisabled_;

  // If a volatile schema already exists when this transaction is started,
  // remember that.
  // if a volatile schema was created after this transaction was started,
  // and this transaction is aborted, then this flag is used to reset the
  // volatile schema state in executor and mxcmp.
  NABoolean volatileSchemaExists_;

  // Creates the diagsArea for the caller. This also sets
  // the optional integer and optional string for the diagnostics
  // area.
  void createDiagsArea(Int32 error, Int32 retCode, const char *string);

  Int16 roVal_;
  Int16 rbVal_;
  Int32 aiVal_;

  // Two flags that help track whether a transaction can be suspended.
  NABoolean mayAlterDb_;
  NABoolean mayHoldLock_;
  int transtag_;

  // values saved during suspendxn and restored during resumexn
  Int64 savedExeXnId_;
  Int64 savedTransId_;
  int   savedTransTag_;
  NABoolean savedExeStartedXn_;
  NABoolean savedXnInProgress_;
  NABoolean savedImplicitXn_;

  ExpHbaseInterface * ehi_;
};

///////////////////////////////////////////////////////
// class ExTransTdb
///////////////////////////////////////////////////////
#include "comexe/ComTdbTransaction.h"

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ExTransTdb;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class ex_tcb;

// -----------------------------------------------------------------------
// ExTransTdb
// -----------------------------------------------------------------------
class ExTransTdb : public ComTdbTransaction
{
public:

  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExTransTdb()
  {}

  virtual ~ExTransTdb()
  {}

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
  //    If yes, put them in the ComTdbTransaction instead.
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
class ExTransTcb : public ex_tcb
{
  friend class ExTransTdb;
  friend class ExTransPrivateState;

public:
  enum Step
  {
    EMPTY_,
    PROCESSING_,
    DONE_,
    CANCELLED_
  };

  ExTransTcb(const ExTransTdb & trans_tdb,
	    ex_globals * glob = 0);

  ~ExTransTcb();

  virtual short work();
  ex_queue_pair getParentQueue() const{return qparent_;}; 
  inline Int32 orderedQueueProtocol() const{return ((const ExTransTdb &)tdb).orderedQueueProtocol();}

  void freeResources(){};
  
  Int32 numChildren() const { return 0; }   

  const ex_tcb* getChild(Int32 /*pos*/) const { return 0; }

  void beginTransaction(ExTransaction *ta);
  void commitTransaction(ExTransaction *ta);
  void rollbackTransaction(ExTransaction *ta);

private:
  ex_queue_pair	qparent_;

  atp_struct *workAtp_;
  
  inline ExTransTdb & transTdb() const{return (ExTransTdb &) tdb;};
  inline ex_expr * diagAreaSizeExpr() const 
  { return transTdb().diagAreaSizeExpr_; };

  // if diagsArea is not NULL, then its error code is used.
  // Otherwise, err is used to handle error.
  void handleErrors(ex_queue_entry *pentry_down,  
		    ComDiagsArea* diagsArea,
		    ExeErrorCode err = EXE_INTERNAL_ERROR);

};


class ExTransPrivateState : public ex_tcb_private_state
{
  friend class ExTransTcb;
  
public:	
  ExTransPrivateState(const ExTransTcb * tcb); //constructor
  ~ExTransPrivateState();	// destructor
  ex_tcb_private_state * allocate_new(const ex_tcb * tcb);

private:
  ExTransTcb::Step step_;

};

#endif
