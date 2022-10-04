
#define CALLABLE

#if defined(__cplusplus)
#undef assert
#define assert(ex)                                      \
  {                                                     \
    if (!(ex)) NAAssert("" #ex "", __FILE__, __LINE__); \
  }

#undef BriefAssertion
#define BriefAssertion(logic, errMsg)                     \
  {                                                       \
    if (!(logic)) NAAssert((errMsg), __FILE__, __LINE__); \
  }

#ifndef NAASSERT_H
#define NAASSERT_H

#include "common/Platform.h"
#include "common/NAMemory.h"

#include <setjmp.h>

#include <pthread.h>
// The main executor thread can terminate the process gracefully,
// and any executor thread can cause the process to abort,
// but synchronization is needed to assure multiple threads do not pursue
// process termination and/or abort processing in parallel"
int NAAssertMutexCreate();  // Function to create mutex to serialize termination
int NAAssertMutexLock();    // Function to lock mutext when a process terminates of a thread asserts

extern void NAAssert(const char *ex, const char *fil, Int32 lin);

extern void NABreakPoint();

extern void NAInlineBreakpointFunc(const char *fil, Int32 lin);

void assert_botch_no_abend(const char *f, Int32 l, const char *m);
// Add condition pointer (fourth argument) for certain SeaMonster asserts
void assert_botch_abend(const char *f, Int32 l, const char *m, const char *c = NULL);
#ifdef NDEBUG
#define NAInlineBreakpoint
#else
#define NAInlineBreakpoint \
  { NAInlineBreakpointFunc(__FILE__, __LINE__); }
#endif

class NAAssertGlobals {
  friend class CliGlobals;

 public:
  inline NABoolean getIsInitialized() const { return globalsAreInitialized_; }
  inline void setIsNotInitialized() { globalsAreInitialized_ = FALSE; }
  inline jmp_buf *getJmpBuf() { return &longJmpTgt_; }
  inline jmp_buf *getJmpBufPtr() { return longJmpTgtPtr_; }
  inline void setJmpBufPtr(jmp_buf *longJmpTgtPtr) { longJmpTgtPtr_ = longJmpTgtPtr; }

 protected:
  NABoolean globalsAreInitialized_;
  jmp_buf *longJmpTgtPtr_;
  jmp_buf longJmpTgt_;
  long numCliCalls_;
};

NAAssertGlobals *GetNAAssertGlobals(NABoolean *logEmsEvents = NULL);

NABoolean IsExecutor(NABoolean *logEmsEvents = NULL);

#endif  // NAASSERT_H
#endif  /* !__cplusplus */
#define NAASSERT_FIRST_PRIV_SEG_ID 2111
/* The removal of spoofing has made this #define history
#define NAASSERT_GLOBALS_OFFSET_IN_PRIV_SEG   20480
 */
#define NAASSERT_FAILURE 1
#define EXASSERT_FAILURE 2
#define MEMALLOC_FAILURE 3
