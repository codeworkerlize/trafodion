
#ifndef _RU_EMP_CHECK_TASK_H_
#define _RU_EMP_CHECK_TASK_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuEmpCheckTask.h
* Description:  Definition of class CRUEmpCheckTask.
*
*
* Created:      04/06/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "refresh.h"

#include "RuLogProcessingTask.h"

//--------------------------------------------------------------------------//
//	CRUEmpCheckTask
//
//	The EmpCheck task (works on a single table) checks whether the delta(s)
//	of the table towards the using MV(s) are empty. The emptiness check's
//	results are recorded in the *emptiness check vector*. For each MV on T,
//	the vector's element V[MV.EPOCH[T]] contains two flags (a bitmap):
//	(1) Are there single-row records in the log starting from epoch MV.EPOCH[T]?
//	(2) Are there range records in the log starting from epoch MV.EPOCH[T]?
//
//	The task's executor applies the EmpCheck algorithm as a generic unit.
//
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRUEmpCheckTask : public CRULogProcessingTask {
 private:
  typedef CRULogProcessingTask inherited;

  //---------------------------------------//
  //	PUBLIC AREA
  //---------------------------------------//
 public:
  CRUEmpCheckTask(int id, CRUTbl &table);
  virtual ~CRUEmpCheckTask();

  //-----------------------------------//
  // Accessors
  //-----------------------------------//
 public:
  //-- Implementation of pure virtuals
  virtual CRUTask::Type GetType() const { return CRUTask::EMP_CHECK; }

  //---------------------------------------//
  //	PRIVATE AND PROTECTED AREA
  //---------------------------------------//
 protected:
  virtual CDSString GetTaskName() const;

  // Refinement of the parent's method
  virtual void PullDataFromExecutor();

  // Create the concrete task executor
  virtual CRUTaskExecutor *CreateExecutorInstance();

  virtual TInt32 GetComputedCost() const { return 0; }

  virtual BOOL IsImmediate() const { return FALSE; }

 private:
  //-- Prevent copying
  CRUEmpCheckTask(const CRUEmpCheckTask &other);
  CRUEmpCheckTask &operator=(const CRUEmpCheckTask &other);
};

#endif
