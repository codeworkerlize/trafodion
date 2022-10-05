
/* -*-C++-*-
******************************************************************************
*
* File:         RuDupElimGlobals.cpp
* Description:  Implementation of class CRUDupElimGlobals
*
* Created:      06/12/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "RuDupElimGlobals.h"
#include "uofsIpcMessageTranslator.h"

//--------------------------------------------------------------------------//
//	Constructor
//--------------------------------------------------------------------------//

CRUDupElimGlobals::CRUDupElimGlobals()
    : isRangeResolv_(FALSE),
      isSingleRowResolv_(FALSE),
      updateBmpSize_(0),
      nCtrlColumns_(0),
      nCKColumns_(0),
      lastDEEpoch_(0),
      beginEpoch_(0),
      endEpoch_(0),
      rangeLogType_(0),
      wasPrevDEInvocationCompleted_(FALSE),
      isSkipCrossTypeResoultion_(FALSE) {}

//--------------------------------------------------------------------------//
//	CRUDupElimGlobals::Init()
//
//	Direct singletone initialization
//--------------------------------------------------------------------------//

void CRUDupElimGlobals::Init(BOOL isRangeResolv, BOOL isSingleRowResolv, int updateBmpSize, int nCtrlColumns,
                             int nCKColumns, TInt32 lastDEEpoch, TInt32 beginEpoch, TInt32 endEpoch,
                             int rangeLogType, BOOL wasPrevDEInvocationCompleted, BOOL isSkipCrossTypeResoultion) {
  isRangeResolv_ = isRangeResolv;
  isSingleRowResolv_ = isSingleRowResolv;
  updateBmpSize_ = updateBmpSize;
  nCtrlColumns_ = nCtrlColumns;
  nCKColumns_ = nCKColumns;
  lastDEEpoch_ = lastDEEpoch;
  beginEpoch_ = beginEpoch;
  endEpoch_ = endEpoch;
  rangeLogType_ = rangeLogType;
  wasPrevDEInvocationCompleted_ = wasPrevDEInvocationCompleted;
  isSkipCrossTypeResoultion_ = isSkipCrossTypeResoultion;
}

//--------------------------------------------------------------------------//
//	CRUDupElimGlobals::LoadData()
//
//	Singletone initialization from the IPC buffer
//--------------------------------------------------------------------------//
void CRUDupElimGlobals::LoadData(CUOFsIpcMessageTranslator &translator) {
  translator.ReadBlock(&isRangeResolv_, sizeof(BOOL));
  translator.ReadBlock(&isSingleRowResolv_, sizeof(BOOL));
  translator.ReadBlock(&updateBmpSize_, sizeof(int));
  translator.ReadBlock(&nCtrlColumns_, sizeof(int));
  translator.ReadBlock(&nCKColumns_, sizeof(int));
  translator.ReadBlock(&lastDEEpoch_, sizeof(TInt32));
  translator.ReadBlock(&beginEpoch_, sizeof(TInt32));
  translator.ReadBlock(&endEpoch_, sizeof(TInt32));
  translator.ReadBlock(&rangeLogType_, sizeof(int));
  translator.ReadBlock(&wasPrevDEInvocationCompleted_, sizeof(BOOL));
  translator.ReadBlock(&isSkipCrossTypeResoultion_, sizeof(BOOL));
}

//--------------------------------------------------------------------------//
//	CRUDupElimGlobals::StoreData()
//
//	Serialization to the IPC buffer
//--------------------------------------------------------------------------//
void CRUDupElimGlobals::StoreData(CUOFsIpcMessageTranslator &translator) {
  translator.WriteBlock(&isRangeResolv_, sizeof(BOOL));
  translator.WriteBlock(&isSingleRowResolv_, sizeof(BOOL));
  translator.WriteBlock(&updateBmpSize_, sizeof(int));
  translator.WriteBlock(&nCtrlColumns_, sizeof(int));
  translator.WriteBlock(&nCKColumns_, sizeof(int));
  translator.WriteBlock(&lastDEEpoch_, sizeof(TInt32));
  translator.WriteBlock(&beginEpoch_, sizeof(TInt32));
  translator.WriteBlock(&endEpoch_, sizeof(TInt32));
  translator.WriteBlock(&rangeLogType_, sizeof(int));
  translator.WriteBlock(&wasPrevDEInvocationCompleted_, sizeof(BOOL));
  translator.WriteBlock(&isSkipCrossTypeResoultion_, sizeof(BOOL));
}
