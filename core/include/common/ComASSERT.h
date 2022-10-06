
#ifndef COMASSERT_H
#define COMASSERT_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComASSERT.h
 * Description:  definition of macro ComASSERT
 *
 * Created:      12/04/95
 * Language:     C++
 *
 *****************************************************************************
 */

#include "common/BaseTypes.h"  // for some odd reason, NADebug() is defined there
#include "common/NAAssert.h"

// -----------------------------------------------------------------------
// For code under development, the macro ComASSERT aborts the program
// if assert truth condition  ex  fails.  For code to be released, the
// macro ComASSERT does nothing.
// -----------------------------------------------------------------------

#if defined(NDEBUG)
#define ComASSERT(ex)
#else
#define ComASSERT(ex)                                   \
  {                                                     \
    if (!(ex)) NAAssert("" #ex "", __FILE__, __LINE__); \
  }
#endif

#if defined(NDEBUG)
#define ComDEBUG(ex)
// An ABORT macro is defined in BaseTypes.h specifically for EID; must be used
//#define ComABORT(ex){ if (!(ex)) NAAbort(__FILE__, __LINE__, "" # ex ""); }
#define ComABORT(ex)             \
  {                              \
    if (!(ex)) ABORT("" #ex ""); \
  }
#else
#define ComDEBUG(ex)                                        \
  {                                                         \
    if (!(ex)) {                                            \
      cerr << "ComDEBUG: "                                  \
           << "" #ex ""                                     \
           << ", " << __FILE__ << ", " << __LINE__ << endl; \
      NADebug();                                            \
    }                                                       \
  }
#define ComABORT(ex) ComDEBUG(ex)
#endif

#endif  // COMASSERT_H
