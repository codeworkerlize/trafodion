
#ifndef EXSM_TRACE_H
#define EXSM_TRACE_H

// 32 bits are used to store the current trace level. Each bit is an
// on/off switch for an individual trace level.

const int EXSM_TRACE_OFF = 0x00000000;
const int EXSM_TRACE_ALL = 0xffffffff;

const int EXSM_TRACE_SM_CALLS = 0x10000000;
const int EXSM_TRACE_THR_ALL = 0x0C000000;
const int EXSM_TRACE_RDR_THR = 0x08000000;
const int EXSM_TRACE_MAIN_THR = 0x04000000;
const int EXSM_TRACE_IO_ALL = 0x03FC0000;
const int EXSM_TRACE_SEND = 0x02000000;
const int EXSM_TRACE_WAIT = 0x01000000;
const int EXSM_TRACE_CANCEL = 0x00800000;
const int EXSM_TRACE_CONTINUE = 0x00400000;
const int EXSM_TRACE_TAG = 0x00200000;
const int EXSM_TRACE_EXIT = 0x00100000;
const int EXSM_TRACE_INIT = 0x00080000;
const int EXSM_TRACE_WORK = 0x00040000;
const int EXSM_TRACE_BUFFER = 0x00020000;
const int EXSM_TRACE_PROTOCOL = 0x00010000;

class ExSMGlobals;
void ExSM_SetTraceLevel(unsigned int lvl);
void ExSM_SetTraceInfo(unsigned int sessionTraceLevel, const char *sessionTraceFilePrefix,
                       unsigned int *effectiveTraceLevel = NULL,       // OUT
                       const char **effectiveTraceFilePrefix = NULL);  // OUT
void ExSM_SetTraceEnabled(bool b, ExSMGlobals *smGlobals);

extern __thread bool EXSM_TRACE_ENABLED;

#define EXSM_TRACE(trace_level, fmt, ...)        \
  if (EXSM_TRACE_ENABLED) {                      \
    ExSM_Trace(trace_level, fmt, ##__VA_ARGS__); \
  }

void ExSM_Trace(unsigned int trace_level, const char *formatString, ...);

#endif  // EXSM_TRACE_H
