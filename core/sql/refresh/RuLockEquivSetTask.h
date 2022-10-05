
#ifndef _RU_LOCK_EQUIV_SET_TASK_H_
#define _RU_LOCK_EQUIV_SET_TASK_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuLockEquivSetTask.h
* Description:  Definition of class CRULockEquivSetTask.
*
*
* Created:      12/06/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "refresh.h"

#include "RuTask.h"
#include "RuTbl.h"
#include "RuException.h"

//--------------------------------------------------------------//
//	CRULockEquivSetTask
//
//	The purpose of the Lock Equivalence Set task is to execute
//	synchronization between involved base tables by locking all the
//  tables or the tables' logs that are members of this lock
//	equivalence set.
//
//--------------------------------------------------------------//

class REFRESH_LIB_CLASS CRULockEquivSetTask : public CRUTask {
  //---------------------------------------//
  //	PUBLIC AREA
  //---------------------------------------//
 public:
  CRULockEquivSetTask(int id, const CRUTblList &tblList);
  virtual ~CRULockEquivSetTask();

  //-----------------------------------//
  // Accessors
  //-----------------------------------//
 public:
  //-- Implementation of pure virtuals
  virtual CRUTask::Type GetType() const { return CRUTask::LOCK_EQUIV_SET; }

  virtual BOOL HasObject(TInt64 uid) const;

  CRUTblList &GetTableList() { return tblList_; }

  //-----------------------------------//
  // Mutators
  //-----------------------------------//

  //---------------------------------------//
  //	PRIVATE AND PROTECTED AREA
  //---------------------------------------//
 protected:
  virtual CDSString GetTaskName() const;

  // If my only successor has failed - I am obsolete
  virtual void HandleSuccessorFailure(CRUTask &task) {}

  // Create the concrete task executor
  virtual CRUTaskExecutor *CreateExecutorInstance();

  virtual TInt32 GetComputedCost() const { return 0; }

  virtual BOOL IsImmediate() const { return TRUE; }

 private:
  //-- Prevent copying
  CRULockEquivSetTask(const CRULockEquivSetTask &other);
  CRULockEquivSetTask &operator=(const CRULockEquivSetTask &other);

 private:
  CRUTblList tblList_;
};

#endif
