
#ifndef _RU_DUPELIM_LOG_RECORD_H_
#define _RU_DUPELIM_LOG_RECORD_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuDupElimLogRecord.h
* Description:  Definition of class CRUIUDLogRecord
*
* Created:      06/12/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "refresh.h"

#include "dsplatform.h"
#include "dsptrlist.h"
#include "dmresultset.h"
#include "dmSqlTuple.h"

#include "RuDupElimConst.h"

class CRUUpdateBitmap;

//--------------------------------------------------------------------------//
//	CRUIUDLogRecord
//
//	This class stores the columns of a single IUD-log record
//	extracted by the delta computation query: the control columns
//	(@EPOCH, @IGNORE etc.), @TS, and the table's clustering key columns.
//
//	The non-persistent data members of the class are:
//	(1) The clustering key (CK) tag - the number of distinct
//		clustering key values encountered so far.
//	(2) The (delete/update) action to be performed by DE. Applicable only
//		to the single-row records. Set by the range resolver.
//
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRUIUDLogRecord {
 public:
  CRUIUDLogRecord(CDMSqlTupleDesc &ckDesc, int updateBmpSize);
  CRUIUDLogRecord(const CRUIUDLogRecord &other);

  virtual ~CRUIUDLogRecord();

  //----------------------------//
  //	Accessors
  //----------------------------//
 public:
  //-- Persistent attributes
  TInt64 GetSyskey() const { return syskey_; }
  TInt32 GetEpoch() const { return epoch_; }
  TInt32 GetOperationType() const { return opType_; }
  TInt32 GetIgnoreMark() const { return ignore_; }
  TInt32 GetRangeSize() { return rangeSize_; }

  const CRUUpdateBitmap *GetUpdateBitmap() const { return pUpdateBitmap_; }

  //-- Operation type decoding
  BOOL IsSingleRowOp() const { return (0 == (opType_ & CRUDupElimConst::IS_RANGE_RECORD)); }

  BOOL IsBeginRange() const { return (0 != (opType_ & CRUDupElimConst::IS_BEGIN_RANGE)); }

  BOOL IsInsert() const { return (TRUE == IsSingleRowOp() && 0 == (opType_ & CRUDupElimConst::IS_DELETE)); }

  BOOL IsDelete() const { return (TRUE == IsSingleRowOp() && 0 != (opType_ & CRUDupElimConst::IS_DELETE)); }

  BOOL IsPartOfUpdate() const { return (0 != (opType_ & CRUDupElimConst::IS_PART_OF_UPDATE)); }

 public:
  //--- Access to the clustering key tuple

  //	Copy the tuple's values to N consecutive parameters
  //	of the statement: firstParam, ... firstParam + N - 1.
  //	Useful for statements where the whole tuple
  //	participates in a WHERE expression,
  //	or the whole tuple is dumped to the log.
  void CopyCKTupleValuesToParams(CDMPreparedStatement &stmt, int firstParam) const;

  const CDMSqlTuple &GetCKTuple() const { return ckTuple_; }

  int GetCKLength() const { return ckTuple_.GetLength(); }

  BOOL IsClusteringKeyEqualTo(const CRUIUDLogRecord &other) const { return (ckTag_ == other.ckTag_); }

  TInt64 GetCKTag() const { return ckTag_; }

  //-- Decision type decoding
  BOOL WillRangeResolvDeleteMe() const { return (0 != action_ & DO_DELETE); }

  BOOL WillRangeResolvUpdateMe() const { return (0 != action_ & DO_UPDATE_IGN_MARK); }

  //----------------------------//
  //	Mutators
  //----------------------------//
 public:
  // Retrieve the data from the result set
  void Build(CDMResultSet &rs, int startCKColumn);

  void SetCKTag(TInt64 val) { ckTag_ = val; }

  void SetIgnoreMark(TInt32 val) { ignore_ = val; }

  void SetRangeResolvWillDeleteMe() { action_ |= DO_DELETE; }

  void SetRangeResolvWillUpdateMe() { action_ |= DO_UPDATE_IGN_MARK; }

 private:
  //-- Prevent copying
  CRUIUDLogRecord &operator=(const CRUIUDLogRecord &other);

 private:
  // Constructor callees
  void ReadControlColumns(CDMResultSet &rs, int startCKColumn);
  void ReadCKColumns(CDMResultSet &rs, int startCKColumn);

 private:
  TInt64 syskey_;
  TInt32 epoch_;
  TInt32 opType_;
  TInt32 ignore_;
  TInt32 rangeSize_;

  CRUUpdateBitmap *pUpdateBitmap_;

  // The clustering key tuple
  CDMSqlTuple ckTuple_;

  // Take enough room to prevent overflows if the delta is extremely big
  TInt64 ckTag_;

  enum Action {

    DO_DELETE,
    DO_UPDATE_IGN_MARK
  };

  int action_;  // What to do with the current record?
};

// Declare the class CRUIUDLogRecordList with this macro
DECLARE_PTRLIST(REFRESH_LIB_CLASS, CRUIUDLogRecord);

#endif
