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
#ifndef EX_CONNECT_BY_TEMP_TABLE_H
#define EX_CONNECT_BY_TEMP_TABLE_H


/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExConnectByTempTable.h
 * Description:  
 *               
 *               
 * Created:      8/20/2019
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "Int64.h"
#include "NABoolean.h"
#include "ComTdbConnectByTempTable.h"

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ExConnectByTempTableTdb;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class ex_tcb;

// -----------------------------------------------------------------------
// ExConnectByTempTableTdb
// -----------------------------------------------------------------------
class ExConnectByTempTableTdb : public ComTdbConnectByTempTable
{
public:

  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExConnectByTempTableTdb()
  {}

  virtual ~ExConnectByTempTableTdb()
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
  //    If yes, put them in the ComTdbConnectByTempTable instead.
  //    If no, they should probably belong to someplace else (like TCB).
  // 
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

class ExConnectByHashTable;
class ExConnectByHashEntry;
class ExConnectByTempTablePrivateState;

#define CONNECT_BY_HASHTABLE_SIZE_FACTOR 10

//
// Task control block
//
class ExConnectByTempTableTcb : public ex_tcb
{
  friend class   ExConnectByTempTableTdb;
  friend class   ExConnectByTempTablePrivateState;

  enum ConnectByTempTableStep {
    INITIAL_,
    POPULATE_DATA_,
    PROCESS_REQUEST_,
    DONE_,
    CANCEL_,
    ERROR_
    };

  const ex_tcb * childTcb_;

  ex_queue_pair  qparent_;
  ex_queue_pair  qchild_;

  ConnectByTempTableStep step_;

  atp_struct     * workAtp_;

  tupp_descriptor  probeHashTupp_;
  ULng32 probeHashVal_;

  tupp_descriptor  probeInputHashTupp_;
  ULng32 probeInputHashVal_;

  UInt32 nextRequest_;  // idx of next value to find, given same key return many rows
  
  tupp_descriptor probeEncodeTupp_;
  char * probeBytes_;

  tupp_descriptor probeInputEncodeTupp_;
  char * probeInputBytes_;

  ExConnectByHashTable* hashTable_;

  NABoolean dataPopulated_;

  // Stub to cancel() subtask used by scheduler. 
  static ExWorkProcRetcode sCancel(ex_tcb *tcb) 
  { return ((ExConnectByTempTableTcb *) tcb)->cancel(); }
  
public:
  // Constructor
  ExConnectByTempTableTcb(const ExConnectByTempTableTdb & connectbytemptable_tdb,    
          const ex_tcb &    child_tcb,    // child queue pair
          ex_globals *glob
          );
  
  ~ExConnectByTempTableTcb();  
  
  void freeResources();  // free resources
  
  short work();                     // when scheduled to do work
  virtual void registerSubtasks();  // register work procedures with scheduler
  short cancel();                   // for the fickle.

  inline ExConnectByTempTableTdb & connectbytemptableTdb() const { return (ExConnectByTempTableTdb &) tdb; }

 
  ex_queue_pair getParentQueue() const { return qparent_;}


  virtual Int32 numChildren() const { return 1; }   
  virtual const ex_tcb* getChild(Int32 /*pos*/) const { return childTcb_; }

  inline ex_expr * hashProbeExpr() const 
      { return connectbytemptableTdb().hashProbeExpr_; };
  
  inline ex_expr * encodeProbeExpr() const 
      { return connectbytemptableTdb().encodeProbeExpr_; };

  inline ex_expr * hashInputProbeExpr() const 
      { return connectbytemptableTdb().hashInputExpr_; };
  inline ex_expr * encodeInputProbeExpr() const 
      { return connectbytemptableTdb().encodeInputExpr_; };
  inline ex_expr * selectPred() const 
      { return connectbytemptableTdb().scanExpr_; };

  inline ex_expr * moveInnerExpr() const 
      { return connectbytemptableTdb().moveInnerExpr_; };
  enum MoveStatus {
    MOVE_OK,
    MOVE_BLOCKED,
    MOVE_ERROR
  };

  MoveStatus moveReplyToCache(ex_queue_entry &reply, ExConnectByHashEntry *pcEntry);
  void  makeReplyToParentUp(ex_queue_entry *pentry_down, 
                           ExConnectByTempTablePrivateState &pstate, 
                           ex_queue::up_status reply_status);
}; 

///////////////////////////////////////////////////////////////////
class ExConnectByTempTablePrivateState : public ex_tcb_private_state
{
  friend class ExConnectByTempTableTcb;

  ExConnectByTempTableTcb::ConnectByTempTableStep step_;

  ExConnectByHashEntry *pcEntry_;

  Int64 matchCount_; // number of rows returned for this parent row
 
public:
  ExConnectByTempTablePrivateState(const ExConnectByTempTableTcb * tcb);
  ex_tcb_private_state * allocate_new(const ex_tcb * tcb);
  ~ExConnectByTempTablePrivateState();       // destructor
};

class ExConnectByHashEntry
{
  friend class ExConnectByHashTable;
  friend class ExConnectByTempTableTcb;

  void release() {
    ex_assert(refCnt_ != 0, "Temp Table for ConnectBy Cache entry ref count already zero");
    refCnt_--;
  }

  ExConnectByHashEntry *nextHashVal_;          // Collision chain for probes with same
                                // hash value.

  union {
    Lng32 value_;
    struct
    {
      unsigned char useBit_:1;          // for second chance replacement.
      unsigned char canceledPending_:1; // cancel has been propagated.
      unsigned char everUsed_:1;        // to indicate an un-init'd ExConnectByHashEntry.
      unsigned char bitFiller_:5;
      unsigned char byteFiller_[3];
    } flags_;
  };

  ULng32 probeHashVal_;    // hash value of probe data.

  ULng32 refCnt_;          // # down queue entries interested in me.

  ex_queue::up_status upstateStatus_; // from CACHE_MISS's original reply.

  queue_index probeQueueIndex_;   // from the CACHE_MISS's parentQueueIndex

  ComDiagsArea *diagsArea_;   // from CACHE_MISS's original reply.

  tupp innerRowTupp_;              // reply tuple for the probe.

  char probeData_[1];             // variable length encoded data.
};


class ExConnectByHashTable : public NABasicObject
{
  friend class ExConnectByTempTableTcb;
public:
  ExConnectByHashTable(Space *space, ULng32 numEntries, ULng32 probeLength,
          ExConnectByTempTableTcb *tcb);

  ~ExConnectByHashTable();
  
  enum FoundOrNotFound { 
    FOUND,
    NOTFOUND 
  };

  FoundOrNotFound findEntry( ULng32 probeHashVal, 
                               char * probeBytes, 
                               UInt32 nextRequest, 
                               ExConnectByHashEntry * &pcEntry );
  ExConnectByHashEntry * getSlot();

private:

    ExConnectByHashEntry *addEntry(ULng32 probeHashVal, 
                    char * probeBytes );

    ULng32 numBuckets_;

    // How many bytes in the probe data?
    ULng32 probeLen_;

     // This points to an array of collision chain headers.
    ExConnectByHashEntry **buckets_;

            // The collision chain entries allocated as an array.  See ExConnectByHashTable
            // for chain semantics.  Note that we declare this as a char *,
            // because the size of the entries in the array are not known
            // when this C++ code is compiled -- see ExConnectByHashEntry::probeData_.
            // Instead, we do our own pointer arithmetic when we must access
            // entries_ as an array, see getPossibleVictim().
    char *entries_;

            // The size of each of entries_'s  array elements.  We need to 
            // keep track of this in our code in order to do correct pointer
            // arithmetic.
    ULng32 sizeofExConnectByHashEntry_;

            // Remember our heap and use it in the dtor.
    Space *space_;

            // To access the runtime stats.
    ExConnectByTempTableTcb *tcb_;
	
	ULng32 nextSlot_;
	ULng32 max_slots_;

};

#endif
