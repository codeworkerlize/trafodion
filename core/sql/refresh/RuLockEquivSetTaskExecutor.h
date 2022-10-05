
#ifndef _RU_LOCK_EQUIV_TASK_EXECUTOR_H_
#define _RU_LOCK_EQUIV_TASK_EXECUTOR_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuLockEquivSetTaskExecutor.h
* Description:  Definition of class CRULockEquivSetTaskExecutor.
*
* Created:      12/06/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "refresh.h"

#include "RuTaskExecutor.h"

class CRUTbl;
class CRULockEquivSetTask;
class CUOFsIpcMessageTranslator;

//--------------------------------------------------------------------------//
//	CRULockEquivSetTaskExecutor
//
//	The executor execute a table synchronization by locking all the
//  tables or the tables' logs that are members of the this
//  lock equivalent set.
//
//	If only the table log is needed , the lock on the log will remain until
//  the epoch incremenet will occure and then it will be released.
//  If only the table is needed (An epoch increment will not occur)
//  the executor will place a lock only on the table which will be released
//  when the last customer of this lock will be refreshed
//
//  The order of placing the lock on the tables in the set is determined by
//  the following rule - The tables that have no lock on refresh attribute
//  will be locked last. This rule is implemented by the order of the tables
//  in the list
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRULockEquivSetTaskExecutor : public CRUTaskExecutor {
 private:
  typedef CRUTaskExecutor inherited;

 public:
  CRULockEquivSetTaskExecutor(CRUTask *pParentTask);
  virtual ~CRULockEquivSetTaskExecutor();

  //----------------------------------//
  //	Public Members
  //----------------------------------//
 public:
  //-- Single execution step.
  //-- Implementation of pure virtual functions
  virtual void Work();
  virtual void Init();

  //----------------------------------//
  //	Protected Members
  //----------------------------------//
 protected:
  //-- Implementation of pure virtual
  virtual int GetIpcBufferSize() const {
    return 0;  // The task is always performed locally
  }

  //----------------------------------//
  //	Private Members
  //----------------------------------//
 private:
  // Not implemented because this executor always runs in the main process

  // These functions serialize/de-serialize the executor's context
  // for the message communication with the remote server process

  // Used in the main process side
  virtual void StoreRequest(CUOFsIpcMessageTranslator &translator) { RUASSERT(FALSE); }
  virtual void LoadReply(CUOFsIpcMessageTranslator &translator) { RUASSERT(FALSE); }

  // Used in the remote process side
  virtual void LoadRequest(CUOFsIpcMessageTranslator &translator) { RUASSERT(FALSE); }
  virtual void StoreReply(CUOFsIpcMessageTranslator &translator) { RUASSERT(FALSE); }

 private:
  //-- Prevent copying
  CRULockEquivSetTaskExecutor(const CRULockEquivSetTaskExecutor &other);
  CRULockEquivSetTaskExecutor &operator=(const CRULockEquivSetTaskExecutor &other);

 private:
  //-- Work() callees
  void FreezeAllTables(CRULockEquivSetTask *pParentTask);
  void SetTimestampToAllTbls(TInt64 ts);
};

#endif
