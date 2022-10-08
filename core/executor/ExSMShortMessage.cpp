
#include "executor/ExSMShortMessage.h"

#include "executor/ExSMGlobals.h"
#include "common/Platform.h"

ExSMShortMessage::ExSMShortMessage() {
  memset(&target_, 0, sizeof(target_));
  numValues_ = 0;
  memset(&values_, 0, sizeof(values_));
}

ExSMShortMessage::~ExSMShortMessage() {}

int32_t ExSMShortMessage::send() const {
  int32_t result = 0;

  result = ExSM_SendShortMessage(ExSMGlobals::GetExSMGlobals(), target_, values_, numValues_ * sizeof(int32_t));

  return result;
}

void ExSMShortMessage::receive(const sm_chunk_t &chunk) {
  target_ = chunk.tgt;

  size_t numValuesInChunk = chunk.size / sizeof(int32_t);
  if (numValuesInChunk > 8) numValuesInChunk = 8;

  numValues_ = numValuesInChunk;

  for (size_t i = 0; i < numValuesInChunk; i++)
    memcpy(&(values_[i]), chunk.buff + (i * sizeof(int32_t)), sizeof(int32_t));
}

void ExSMShortMessage::writeToTrace(uint32_t traceLevel, const char *prefix1, const char *prefix2) const {
  if (!EXSM_TRACE_ENABLED) return;

  EXSM_TRACE(traceLevel, "%s%sSHORT MSG %d:%d:%" PRId64 ":%d:0x%c", prefix1 ? prefix1 : "", prefix2 ? prefix2 : "",
             (int)target_.node, (int)target_.pid, target_.id, (int)ExSMTag_GetTagWithoutQualifier(target_.tag),
             (char)ExSMTag_GetQualifierDisplay(target_.tag));

  for (size_t i = 0; i < numValues_; i++) {
    EXSM_TRACE(traceLevel, "%s%s  [%d] %d (%08x)", prefix1 ? prefix1 : "", prefix2 ? prefix2 : "", (int)i,
               (int)values_[i], (int)values_[i]);
  }
}
