
#ifndef _RU_LOG_PROC_TASK_H_
#define _RU_LOG_PROC_TASK_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuLogProcessingTask.h
* Description:  Definition of class CRULogProcessingTask
*
*
* Created:      08/29/2000
* Language:     C++
*
*
*
******************************************************************************
*/

#include "refresh.h"

#include "RuTask.h"
#include "RuTbl.h"

class CRUEmpCheckVector;

//---------------------------------------------------------//
//	CRULogProcessingTask
//
//	This abstract class is a base class for the TableSync,
//	EmpCheck, DE and Log Cleanup tasks.
//
//	Every Log Processing task is associated with a single
//	CRUTbl object.
//
//---------------------------------------------------------//

class REFRESH_LIB_CLASS CRULogProcessingTask : public CRUTask {
 private:
  typedef CRUTask inherited;

 public:
  CRULogProcessingTask(int id, CRUTbl &table);
  virtual ~CRULogProcessingTask();

  //-----------------------------------//
  // Accessors
  //-----------------------------------//
 public:
  CRUTbl &GetTable() const { return table_; }

  virtual BOOL HasObject(TInt64 uid) const { return table_.GetUID() == uid; }

  // Implementation of pure virtual (overriding the default behavior).
  // If my only successor has failed - I am obsolete
  virtual void HandleSuccessorFailure(CRUTask &task);

 private:
  CRUTbl &table_;
};

#endif
