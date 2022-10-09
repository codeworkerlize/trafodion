
#ifndef CLI_SEMAPHORE_H
#define CLI_SEMAPHORE_H

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

#include "cli/Globals.h"
#include "common/sqtypes.h"

class CLISemaphore;

extern CliGlobals *cli_globals;
extern CLISemaphore *getCliSemaphore();

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
