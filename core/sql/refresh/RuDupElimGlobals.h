
#ifndef _RU_DUPELIM_GLOBALS_H_
#define _RU_DUPELIM_GLOBALS_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuDupElimGlobals.h
* Description:  Definition of class CRUDupElimGlobals
*
* Created:      06/12/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "refresh.h"
#include "common/Platform.h"
#include "dsplatform.h"

class CUOFsIpcMessageTranslator;

//--------------------------------------------------------------------------//
//	CRUDupElimGlobals
//
//	A collection of values required by the different units of the
//	Duplicate Elimination task executor.
//
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRUDupElimGlobals {
 public:
  CRUDupElimGlobals();
  virtual ~CRUDupElimGlobals() {}

  void Init(BOOL isRangeResolv, BOOL isSingleRowResolv, int updateBmpSize, int nCtrlColumns, int nCKColumns,
            TInt32 lastDEEpoch, TInt32 beginEpoch, TInt32 endEpoch, int rangeLogType,
            BOOL wasPrevDEInvocationCompleted, BOOL isSkipCrossTypeResoultion);

  // Pack/Unpack methods for IPC
  void LoadData(CUOFsIpcMessageTranslator &translator);
  void StoreData(CUOFsIpcMessageTranslator &translator);

  // Which resolvers are created?
  BOOL IsRangeResolv() const { return isRangeResolv_; }
  BOOL IsSingleRowResolv() const { return isSingleRowResolv_; }

  // The number of bytes in the @UPDATE_BITMAP column
  int GetUpdateBmpSize() const { return updateBmpSize_; }
  // The number of control columns to be retrieved by the query
  int GetNumCtrlColumns() const { return nCtrlColumns_; }
  // The number of clustering key columns in the table
  int GetNumCKColumns() const { return nCKColumns_; }
  // The last epoch scanned by DE until now
  TInt32 GetLastDEEpoch() const { return lastDEEpoch_; }
  TInt32 GetBeginEpoch() const { return beginEpoch_; }
  TInt32 GetEndEpoch() const { return endEpoch_; }
  int GetRangeLogType() const { return rangeLogType_; }
  BOOL WasPrevDEInvocationCompleted() const { return wasPrevDEInvocationCompleted_; }
  BOOL IsSkipCrossTypeResoultion() const { return isSkipCrossTypeResoultion_; }

 private:
  BOOL isRangeResolv_;
  BOOL isSingleRowResolv_;
  int updateBmpSize_;
  int nCtrlColumns_;
  int nCKColumns_;
  TInt32 lastDEEpoch_;
  TInt32 beginEpoch_;
  TInt32 endEpoch_;
  int rangeLogType_;
  BOOL wasPrevDEInvocationCompleted_;
  BOOL isSkipCrossTypeResoultion_;
};

#endif
