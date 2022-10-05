
/* -*-C++-*-
******************************************************************************
*
* File:         RuDupElimLogRecord.cpp
* Description:  Implementation of class	CRUIUDLogRecord
*
* Created:      06/12/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "dmprepstatement.h"
#include "ddobject.h"

#include "RuDupElimLogRecord.h"
#include "RuDeltaDef.h"
#include "RuException.h"

//--------------------------------------------------------------------------//
//		CLASS CRUIUDLogRecord - PUBLIC AREA
//--------------------------------------------------------------------------//

//--------------------------------------------------------------------------//
//	Constructors and destructor
//--------------------------------------------------------------------------//

CRUIUDLogRecord::CRUIUDLogRecord(CDMSqlTupleDesc &ckDesc, int updateBmpSize)
    :  // Persistent data members
      ckTuple_(ckDesc),
      syskey_(0),
      epoch_(0),
      opType_(0),
      ignore_(0),
      rangeSize_(0),
      pUpdateBitmap_(NULL),
      // Non-persistent data members
      ckTag_(0),
      action_(0) {
  if (0 != updateBmpSize) {
    pUpdateBitmap_ = new CRUUpdateBitmap(updateBmpSize);
  }
}

CRUIUDLogRecord::CRUIUDLogRecord(const CRUIUDLogRecord &other)
    :  // Persistent data members
      ckTuple_(other.ckTuple_),
      syskey_(other.syskey_),
      epoch_(other.epoch_),
      opType_(other.opType_),
      ignore_(other.ignore_),
      rangeSize_(other.rangeSize_),
      pUpdateBitmap_(NULL),
      // Non-persistent data members
      ckTag_(other.ckTag_) {
  CRUUpdateBitmap *pOtherUpdateBitmap = other.pUpdateBitmap_;

  if (NULL != pOtherUpdateBitmap) {
    pUpdateBitmap_ = new CRUUpdateBitmap(*pOtherUpdateBitmap);
  }
}

CRUIUDLogRecord::~CRUIUDLogRecord() { delete pUpdateBitmap_; }

//--------------------------------------------------------------------------//
//	CRUIUDLogRecord::CopyCKTupleValuesToParams()
//
//	Copy the tuple's values to N consecutive parameters
//	of the statement: firstParam, ... firstParam + N - 1.
//
//--------------------------------------------------------------------------//

void CRUIUDLogRecord::CopyCKTupleValuesToParams(CDMPreparedStatement &stmt, int firstParam) const {
  int len = GetCKLength();
  for (int i = 0; i < len; i++) {
    ckTuple_.GetItem(i).SetStatementParam(stmt, firstParam + i);
  }
}

//--------------------------------------------------------------------------//
//	CRUIUDLogRecord::Build()
//
//	Retrieve the tuple's data from the result set and store it.
//	The tuple's columns are contiguous in the result set,
//	starting from the *startCKColumn* parameter.
//
//--------------------------------------------------------------------------//

void CRUIUDLogRecord::Build(CDMResultSet &rs, int startCKColumn) {
  ReadControlColumns(rs, startCKColumn);
  ReadCKColumns(rs, startCKColumn);
}

//--------------------------------------------------------------------------//
//		CLASS CRUIUDLogRecord - PRIVATE AREA
//--------------------------------------------------------------------------//

//--------------------------------------------------------------------------//
//	CRUIUDLogRecord::ReadControlColumns()
//
//	Get the control columns from the result set (epoch, syskey etc).
//	The operation_type column is stored as a bitmap, and hence
//	requires decoding.
//
//--------------------------------------------------------------------------//

void CRUIUDLogRecord::ReadControlColumns(CDMResultSet &rs, int startCKColumn) {
  // Performance optimization - switch the IsNull check off!
  rs.PresetNotNullable(TRUE);

  // Read the mandatory columns
  epoch_ = rs.GetInt(CRUDupElimConst::OFS_EPOCH + 1);

  opType_ = rs.GetInt(CRUDupElimConst::OFS_OPTYPE + 1);
  if (FALSE == IsSingleRowOp()) {
    // The range records are logged in the negative epochs.
    // Logically, however, they belong to the positive epochs.
    epoch_ = -epoch_;
  }

  if (FALSE == IsSingleRowOp() && FALSE == IsBeginRange()) {
    // End-range record
    rangeSize_ = rs.GetInt(CRUDupElimConst::OFS_RNGSIZE + 1);
  } else {
    rangeSize_ = 0;
  }

  int numCKCols = startCKColumn - 2;  // Count from 1 + syskey
  if (CRUDupElimConst::NUM_IUD_LOG_CONTROL_COLS_EXTEND == numCKCols) {
    // This is DE level 3, read the optional columns
    ignore_ = rs.GetInt(CRUDupElimConst::OFS_IGNORE + 1);

    // The update bitmap buffer must be setup
    RUASSERT(NULL != pUpdateBitmap_);
    // The update bitmap can be a null
    rs.PresetNotNullable(FALSE);

    rs.GetString(CRUDupElimConst::OFS_UPD_BMP + 1, pUpdateBitmap_->GetBuffer(), pUpdateBitmap_->GetSize());
  }

  // Syskey is always the last column before the CK
  syskey_ = rs.GetLargeInt(startCKColumn - 1);
}

//--------------------------------------------------------------------------//
//	CRUIUDLogRecord::ReadCKColumns()
//
//	Retrieve the values of the clustering key columns and store
//	them in an SQL tuple.
//
//--------------------------------------------------------------------------//

void CRUIUDLogRecord::ReadCKColumns(CDMResultSet &rs, int startCKColumn) {
  // Performance optimization - switch the IsNull check off!
  rs.PresetNotNullable(TRUE);

  int len = GetCKLength();

  for (int i = 0; i < len; i++) {
    int colIndex = i + startCKColumn;

    ckTuple_.GetItem(i).Build(rs, colIndex);
  }

  rs.PresetNotNullable(FALSE);
}

// Define the class CRUIUDLogRecordList with this macro
DEFINE_PTRLIST(CRUIUDLogRecord);
