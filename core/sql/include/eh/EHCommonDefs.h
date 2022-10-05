/* -*-C++-*-

 *****************************************************************************
 *
 * File:         EHCommonDefs.h
 * Description:  common definitions -- This file was derived from the file
 *               CommonDefs.h
 *
 * Created:      6/20/95
 * Language:     C++
 *
 *
 *
 *****************************************************************************
 */

#ifndef EHCOMMONDEFS_H
#define EHCOMMONDEFS_H

#include "common/Platform.h"  // 64-BIT
#include "EHBaseTypes.h"

// -----------------------------------------------------------------------
// Declare a Boolean type
// (compatible with standard C++ boolean expressions)
// -----------------------------------------------------------------------

#ifndef EHBOOLEAN_DEFINED
#define EHBOOLEAN_DEFINED
typedef int EHBoolean;
#endif

#ifndef TRUE
#ifndef TRUE_DEFINED
#define TRUE_DEFINED
const EHBoolean TRUE = (1 == 1);
#endif
#endif

#ifndef FALSE
#ifndef FALSE_DEFINED
#define FALSE_DEFINED
const EHBoolean FALSE = (0 == 1);
#endif
#endif

// -----------------------------------------------------------------------
// C++ operators in a more readable form
// -----------------------------------------------------------------------

#ifndef EQU
#define EQU ==
#endif
#ifndef NEQ
#define NEQ !=
#endif
#ifndef NOT
#define NOT !
#endif
#ifndef AND
#define AND &&
#endif
#ifndef OR
#define OR ||
#endif

// -----------------------------------------------------------------------
// Abnormal program termination (EHAbort defined in EHAbort.C)
// -----------------------------------------------------------------------

#ifndef EH_ABORT
#define EH_ABORT(msg) EHAbort(__FILE__, __LINE__, (msg))
#endif
void EHAbort(const char *filename, int lineno, const char *msg);

// -----------------------------------------------------------------------
// Abort program if AssertTruth condition fails
// -----------------------------------------------------------------------

#ifndef EH_ASSERT
#define EH_ASSERT(cond)                                                         \
  {                                                                             \
    if (NOT(cond)) EHAbort(__FILE__, __LINE__, "AssertTruth condition failed"); \
  }
#endif

#endif  // EHCOMMONDEFS_H
