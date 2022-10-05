
#ifndef _RU_LOG_CLEANUP_TASK_H_
#define _RU_LOG_CLEANUP_TASK_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuLogCleanupTask.h
* Description:  Definition of class	CRULogCleanupTask
*
* Created:      12/29/1999
* Language:     C++
*
*
******************************************************************************
*/

#include "refresh.h"

#include "RuTbl.h"

#include "RuLogProcessingTask.h"
#include "RuTaskExecutor.h"

// ----------------------------------------------------------------- //
// CRULogCleanupTask
//
//	 The Log Cleanup task class. Supports cleanup of inapplicable
//	 epochs from a single table-log.
//
// ----------------------------------------------------------------- //

class REFRESH_LIB_CLASS CRULogCleanupTask : public CRULogProcessingTask {
 private:
  typedef CRULogProcessingTask inherited;

  //---------------------------------------//
  //	PUBLIC AREA
  //---------------------------------------//
 public:
  CRULogCleanupTask(int id, CRUTbl &table);
  virtual ~CRULogCleanupTask() {}

  //-----------------------------------//
  // Accessors
  //-----------------------------------//

 public:
  TInt32 GetMaxInapplicableEpoch() const { return maxInapplicableEpoch_; }

  //-- Implementation of pure virtual functions
  virtual CRUTask::Type GetType() const { return CRUTask::LOG_CLEANUP; }

  //---------------------------------------//
  //	PRIVATE AND PROTECTED AREA
  //---------------------------------------//
 protected:
  //-- Implementation of pure virtuals
  virtual CDSString GetTaskName() const { return "LC(" + GetTable().GetFullName() + ")"; }

  virtual TInt32 GetComputedCost() const { return 0; }

  virtual BOOL IsImmediate() const { return FALSE; }

  virtual CRUTaskExecutor *CreateExecutorInstance();

 private:
  //-- Prevent copying
  CRULogCleanupTask(const CRULogCleanupTask &other);
  CRULogCleanupTask &operator=(const CRULogCleanupTask &other);

 private:
  void ComputeMaxInapplicableEpoch();

 private:
  TInt32 maxInapplicableEpoch_;
};

#endif
