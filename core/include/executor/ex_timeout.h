
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ex_timeout.h
 * Description:  Executor's TDB and the TCB for the "set timeout" statement
 *
 * Created:      12/27/1999
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#ifndef EX_TIMEOUT_H
#define EX_TIMEOUT_H
// -----------------------------------------------------------------------
#include "comexe/ComTdb.h"
#include "common/Int64.h"
#include "executor/ex_error.h"
#include "executor/ex_stdh.h"
#include "executor/ex_tcb.h"

///////////////////////////////////////////////////////
// class ExTimeoutTdb
///////////////////////////////////////////////////////
#include "comexe/ComTdbTimeout.h"

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class ExTimeoutTdb;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------
class ex_tcb;

// -----------------------------------------------------------------------
// ExTimeoutTdb
// -----------------------------------------------------------------------
class ExTimeoutTdb : public ComTdbTimeout {
 public:
  // ---------------------------------------------------------------------
  // Constructor is only called to instantiate an object used for
  // retrieval of the virtual table function pointer of the class while
  // unpacking. An empty constructor is enough.
  // ---------------------------------------------------------------------
  ExTimeoutTdb() {}

  virtual ~ExTimeoutTdb() {}

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
  //    If yes, put them in the ComTdbTimeout instead.
  //    If no, they should probably belong to someplace else (like TCB).
  //
  // 2. Are the classes those data members belong defined in the executor
  //    project?
  //    If your answer to both questions is yes, you might need to move
  //    the classes to the comexe project.
  // ---------------------------------------------------------------------
};

//
// TCB: Task control block
//
class ExTimeoutTcb : public ex_tcb {
  friend class ExTimeoutTdb;
  friend class ExTimeoutPrivateState;

 public:
  // ctor
  ExTimeoutTcb(const ExTimeoutTdb &timeout_tdb, ExMasterStmtGlobals *glob = 0);

  ~ExTimeoutTcb();  // dtor

  virtual short work();  // do the actual work ...

  ex_queue_pair getParentQueue() const { return qparent_; };

  inline int orderedQueueProtocol() const { return ((const ExTimeoutTdb &)tdb).orderedQueueProtocol(); }

  void freeResources(){};

  int numChildren() const { return 0; }
  const ex_tcb *getChild(int /*pos*/) const { return 0; }

 private:
  // holds the ANSI name of the table (for which the timeout value is set)
  char *theTableName_;

  ex_queue_pair qparent_;

  atp_struct *workAtp_;

  inline ExTimeoutTdb &timeoutTdb() const { return (ExTimeoutTdb &)tdb; };
  inline ex_expr *timeoutValueExpr() const { return timeoutTdb().timeoutValueExpr_; };

  // if diagsArea is not NULL, then its error code is used.
  // Otherwise, err is used to handle error.
  void handleErrors(ex_queue_entry *pentry_down, ComDiagsArea *diagsArea, ExeErrorCode err = EXE_INTERNAL_ERROR);
};

class ExTimeoutPrivateState : public ex_tcb_private_state {
  friend class ExTimeoutTcb;

 public:
  ExTimeoutPrivateState(const ExTimeoutTcb *tcb);  // constructor
  ~ExTimeoutPrivateState();                        // destructor
  ex_tcb_private_state *allocate_new(const ex_tcb *tcb);
};

#endif
