
/* -*-C++-*-
****************************************************************************
*
* File:         SequenceGeneratorAttributes.h
* Description:  The attributes of the sequence generator
* Created:      4/22/08
* Language:     C++
*
****************************************************************************/

#ifndef SEQUENCEGENERATORATTRIBUTES_H
#define SEQUENCEGENERATORATTRIBUTES_H

#include "common/ComSmallDefs.h"

class ComSpace;

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class SequenceGeneratorAttributes;

// ***********************************************************************
// SequenceGeneratorAttributes contains all the attributes of a
// sequence generator
// ***********************************************************************
class SequenceGeneratorAttributes : public NABasicObject {
 public:
  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  SequenceGeneratorAttributes(const long psgStartValue, const long psgIncrement, const long psgMaxValue,
                              const long psgMinValue, const ComSequenceGeneratorType psgSGType,
                              const ComSQLDataType psgSQLDataType, const ComFSDataType psgFSDataType,
                              const NABoolean psgCycleOption, const NABoolean psgResetOption, const ComUID psgObjectUID,
                              const long psgCache, const long psgNextValue, const NABoolean psgOrderOption,
                              const long psgEndValue = 0, const long psgRedefTime = 0, const long psgTimeout = -1,
                              CollHeap *h = 0)
      : sgStartValue_(psgStartValue),
        sgIncrement_(psgIncrement),
        sgMaxValue_(psgMaxValue),
        sgMinValue_(psgMinValue),
        sgSGType_(psgSGType),
        sgSQLDataType_(psgSQLDataType),
        sgFSDataType_(psgFSDataType),
        sgCycleOption_(psgCycleOption),
        sgResetOption_(psgResetOption),
        sgObjectUID_(psgObjectUID),
        sgCache_(psgCache),
        sgNextValue_(psgNextValue),
        sgEndValue_(psgEndValue),
        sgRedefTime_(psgRedefTime),
        sgRetryNum_(100),
        sgUseDlockImpl_(false),
        sgOrder_(psgOrderOption),
        sgUseDtmImpl_(false),
        sgReplType_(COM_REPL_NONE) {}

  SequenceGeneratorAttributes(CollHeap *h = 0)
      : sgStartValue_(0),
        sgIncrement_(0),
        sgMaxValue_(0),
        sgMinValue_(0),
        sgSGType_(COM_UNKNOWN_SG),
        sgSQLDataType_(COM_UNKNOWN_SDT),
        sgFSDataType_(COM_UNKNOWN_FSDT),
        sgCycleOption_(FALSE),
        sgResetOption_(FALSE),
        sgObjectUID_(0),
        sgCache_(0),
        sgNextValue_(0),
        sgEndValue_(0),
        sgRedefTime_(0),
        sgRetryNum_(100),
        sgUseDlockImpl_(false),
        sgUseDtmImpl_(false),
        sgOrder_(false),
        sgReplType_(COM_REPL_NONE),
        sgTimeout_(-1) {}

  // copy ctor
  SequenceGeneratorAttributes(const SequenceGeneratorAttributes &sga, CollHeap *h = 0)
      : sgStartValue_(sga.sgStartValue_),
        sgIncrement_(sga.sgIncrement_),
        sgMaxValue_(sga.sgMaxValue_),
        sgMinValue_(sga.sgMinValue_),
        sgSGType_(sga.sgSGType_),
        sgSQLDataType_(sga.sgSQLDataType_),
        sgFSDataType_(sga.sgFSDataType_),
        sgCycleOption_(sga.sgCycleOption_),
        sgResetOption_(sga.sgResetOption_),
        sgObjectUID_(sga.sgObjectUID_),
        sgCache_(sga.sgCache_),
        sgNextValue_(sga.sgNextValue_),
        sgEndValue_(sga.sgEndValue_),
        sgRedefTime_(sga.sgRedefTime_),
        sgRetryNum_(100),
        sgUseDlockImpl_(false),
        sgUseDtmImpl_(false),
        sgOrder_(sga.sgOrder_),
        sgReplType_(sga.sgReplType_),
        sgTimeout_(sga.sgTimeout_) {}

  // ---------------------------------------------------------------------
  // Sequence generator functions
  // ---------------------------------------------------------------------

  const long &getSGStartValue() const { return sgStartValue_; }
  const long &getSGIncrement() const { return sgIncrement_; }
  const long &getSGMaxValue() const { return sgMaxValue_; }
  const long &getSGMinValue() const { return sgMinValue_; }
  const ComSequenceGeneratorType &getSGType() const { return sgSGType_; }
  const ComSQLDataType &getSGSQLDataType() const { return sgSQLDataType_; }
  const ComFSDataType &getSGFSDataType() const { return sgFSDataType_; }
  const NABoolean &getSGCycleOption() const { return sgCycleOption_; }
  const NABoolean &getSGResetOption() const { return sgResetOption_; }
  const ComUID &getSGObjectUID() const { return sgObjectUID_; }
  const long &getSGCache() const { return sgCache_; }
  const long &getSGNextValue() const { return sgNextValue_; }
  const long &getSGEndValue() const { return sgEndValue_; }
  const long &getSGRedefTime() const { return sgRedefTime_; }
  const UInt32 &getSGRetryNum() const { return sgRetryNum_; }
  const NABoolean getSGUseDlockImpl() const { return sgUseDlockImpl_; }
  const NABoolean getSGUseDtmImpl() const { return sgUseDtmImpl_; }
  const NABoolean getSGOrder() const { return sgOrder_; }
  const NABoolean getSGSyncRepl() const { return sgReplType_ == COM_REPL_SYNC; }
  const NABoolean getSGAsyncRepl() const { return sgReplType_ == COM_REPL_ASYNC; }

  void setSGRetryNum(const UInt32 v) { sgRetryNum_ = v; }

  void setSGUseDlockImpl(const NABoolean v) { sgUseDlockImpl_ = v; }

  void setSGUseDtmImpl(const NABoolean v) { sgUseDtmImpl_ = v; }

  void setSGReplType(const ComReplType type) { sgReplType_ = type; }

  void setSGStartValue(const long psgStartValue) { sgStartValue_ = psgStartValue; }

  void setSGIncrement(const long psgIncrement) { sgIncrement_ = psgIncrement; }

  void setSGMaxValue(const long psgMaxValue) { sgMaxValue_ = psgMaxValue; }

  void setSGMinValue(const long psgMinValue) { sgMinValue_ = psgMinValue; }

  void setSGType(const ComSequenceGeneratorType psgSGType) { sgSGType_ = psgSGType; }

  void setSGSQLDataType(const ComSQLDataType psgSQLDataType) { sgSQLDataType_ = psgSQLDataType; }

  void setSGFSDataType(const ComFSDataType psgFSDataType) { sgFSDataType_ = psgFSDataType; }

  void setSGCycleOption(const NABoolean psgCycleOption) { sgCycleOption_ = psgCycleOption; }

  void setSGResetOption(const NABoolean psgResetOption) { sgResetOption_ = psgResetOption; }

  void setSGObjectUID(const ComUID psgObjectUID) { sgObjectUID_ = psgObjectUID; }

  void setSGCache(const long psgCache) { sgCache_ = psgCache; }

  void setSGNextValue(const long psgNextValue) { sgNextValue_ = psgNextValue; }

  void setSGEndValue(const long psgEndValue) { sgEndValue_ = psgEndValue; }

  void setSGRedefTime(const long psgRedefTime) { sgRedefTime_ = psgRedefTime; }

  void setSGOrder(const NABoolean sgOrder) { sgOrder_ = sgOrder; }

  NABoolean isSystemSG() const { return (sgSGType_ == COM_SYSTEM_SG); }

  long getSGTimeout() const { return sgTimeout_; }
  void setSGTimeout(long sgt) { sgTimeout_ = sgt; }
  static void genSequenceName(const NAString &catName, const NAString &schName, const NAString &tabName,
                              const NAString &colName, NAString &seqName);

  const void display(ComSpace *space, NAString *nas, NABoolean noNext = FALSE, NABoolean inShowDDL = FALSE,
                     NABoolean commentOut = FALSE) const;

  NABoolean operator==(const SequenceGeneratorAttributes &other) const {
    return (sgStartValue_ == other.sgStartValue_ && sgIncrement_ == other.sgIncrement_ &&
            sgMaxValue_ == other.sgMaxValue_ && sgMinValue_ == other.sgMinValue_ && sgSGType_ == other.sgSGType_ &&
            sgSQLDataType_ == other.sgSQLDataType_ && sgFSDataType_ == other.sgFSDataType_ &&
            sgCycleOption_ == other.sgCycleOption_ && sgResetOption_ == other.sgResetOption_ &&
            sgObjectUID_ == other.sgObjectUID_ && sgCache_ == other.sgCache_ && sgNextValue_ == other.sgNextValue_ &&
            sgEndValue_ == other.sgEndValue_ && sgRedefTime_ == other.sgRedefTime_ &&
            sgRetryNum_ == other.sgRetryNum_ && sgUseDlockImpl_ == other.sgUseDlockImpl_ &&
            sgOrder_ == other.sgOrder_ && sgUseDtmImpl_ == other.sgUseDtmImpl_ && sgReplType_ == other.sgReplType_ &&
            sgTimeout_ == other.sgTimeout_);
  }

 private:
  // Sequence generator

  long sgStartValue_;
  long sgIncrement_;
  long sgMaxValue_;
  long sgMinValue_;
  ComSequenceGeneratorType sgSGType_;
  ComSQLDataType sgSQLDataType_;
  ComFSDataType sgFSDataType_;
  NABoolean sgCycleOption_;
  NABoolean sgResetOption_;
  ComUID sgObjectUID_;
  long sgCache_;
  long sgNextValue_;
  long sgEndValue_;
  long sgRedefTime_;
  UInt32 sgRetryNum_;
  NABoolean sgUseDlockImpl_;
  NABoolean sgOrder_;
  NABoolean sgUseDtmImpl_;
  ComReplType sgReplType_;
  long sgTimeout_;
};  // class SequenceGeneratorAttributes

#endif /* SEQUENCEGENERATORATTRIBUTES_H */
