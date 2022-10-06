
#ifndef CLI_SEMAPHORE_H
#define CLI_SEMAPHORE_H 1

/* -*-C++-*-
******************************************************************************
*
* File:         CLISemaphore.h
* Description:  Header file containing CLI Semaphore implementation.
*
* Created:      7/21/97
* Language:     C++
*
*
*
******************************************************************************
*/

#ifndef NA_NO_GLOBAL_EXE_VARS
#ifndef CLI_GLOBALS_DEF_
#include "cli/Globals.h"
class CLISemaphore;

extern CliGlobals *cli_globals;
extern CLISemaphore *getCliSemaphore();
#endif
#endif

// define the semaphore functions and mechanism
class CLISemaphore {
 private:
  CRITICAL_SECTION cs;

 public:
  void get();
  void release();
  CLISemaphore();
  ~CLISemaphore();
};

inline void CLISemaphore::get() { EnterCriticalSection(&cs); }

inline void CLISemaphore::release() { LeaveCriticalSection(&cs); }

inline CLISemaphore::CLISemaphore() { InitializeCriticalSection(&cs); }

inline CLISemaphore::~CLISemaphore() {}

extern CLISemaphore globalSemaphore;

#endif
