
#ifndef EXSM_SHORT_MESSAGE_H
#define EXSM_SHORT_MESSAGE_H

#include "executor/ExSMCommon.h"

class ExSMShortMessage;

class ExSMShortMessage {
 public:
  ExSMShortMessage();

  virtual ~ExSMShortMessage();

  const sm_target_t &getTarget() const { return target_; }
  void setTarget(const sm_target_t &t) { target_ = t; }

  size_t getNumValues() const { return numValues_; }
  void setNumValues(size_t n) { numValues_ = n; }

  int32_t getValue(size_t i) const { return (i < numValues_ ? values_[i] : 0); }

  void setValue(size_t i, int32_t val) {
    if (i < numValues_) values_[i] = val;
  }

  int32_t send() const;
  void receive(const sm_chunk_t &chunk);

  void writeToTrace(uint32_t trace_level, const char *prefix1 = "", const char *prefix2 = "") const;

  enum MsgType { UNKNOWN = 0, SHUTDOWN, SIZE, ACK, FIXUP_REPLY };

 protected:
  static const size_t MAX_VALUES = 8;

  sm_target_t target_;
  size_t numValues_;
  int32_t values_[MAX_VALUES];

};  // class ExSMShortMessage

#endif  // EXSM_SHORT_MESSAGE_H
