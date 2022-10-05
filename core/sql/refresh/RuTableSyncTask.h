
#ifndef _RU_TABLE_SYNC_TASK_H_
#define _RU_TABLE_SYNC_TASK_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuTableSyncTask.h
* Description:  Definition of class CRUTableSyncTask.
*
* Created:      12/06/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "refresh.h"

#include "RuLogProcessingTask.h"

class CRURefreshTask;

//--------------------------------------------------------------------------//
//	CRUTableSyncTask
//
//  The task will be responsible for the following actions
//	1.	Increment the epoch of a table that needs it (when ever the log may
//      be consumed in this refresh invocation),this stage may be executed in
//		the remote process
//	2.  Lock the table in case a long lock is needed (long lock is a lock
//      that remains until the last mv that required that lock has been
//		refreshed)
//	3.  Save the syncronization timestamp in the table object for further
//		use
//
//	The epoch increment separates the records that have been logged
//	*before* and *after* the refresh has started (i.e., defines the
//	delta's upper boundary. The read-protected open of a table is a
//	non-transactional shared lock which "freezes" the table for the
//	time of refresh. This is a requirement for ON REQUEST MVs that
//	use both the table and the log for refresh, and for RECOMPUTED mv's that
//  use an mv object join with other objects
//
//
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRUTableSyncTask : public CRULogProcessingTask {
  //---------------------------------------//
  //	Public Memebers
  //---------------------------------------//
 public:
  CRUTableSyncTask(int id, CRUTbl &table);
  virtual ~CRUTableSyncTask();

 public:
  //-- Implementation of pure virtuals
  virtual CRUTask::Type GetType() const { return CRUTask::TABLE_SYNC; }

  //---------------------------------------//
  //	Protected Memebers
  //---------------------------------------//

 protected:
  virtual CDSString GetTaskName() const;

  // Create the concrete task executor
  virtual CRUTaskExecutor *CreateExecutorInstance();

  virtual TInt32 GetComputedCost() const { return 0; }

  virtual BOOL IsImmediate() const { return GetTable().IsNoLockOnRefresh(); }

  //---------------------------------------//
  //	Private Memebers
  //---------------------------------------//
 private:
  //-- Prevent copying
  CRUTableSyncTask(const CRUTableSyncTask &other);
  CRUTableSyncTask &operator=(const CRUTableSyncTask &other);
};

#endif
