#ifndef NABOOLEAN_H
#define NABOOLEAN_H

// -----------------------------------------------------------------------

#include "common/Platform.h"

// -----------------------------------------------------------------------
// Declare a Boolean type
// (compatible with standard C++ boolean expressions)
// -----------------------------------------------------------------------

#ifndef NABOOLEAN_DEFINED
#define NABOOLEAN_DEFINED
typedef int NABoolean;
#endif

#undef TRUE
#ifndef TRUE
#ifndef TRUE_DEFINED
#define TRUE_DEFINED
const NABoolean TRUE = (1 == 1);
#endif
#endif

#undef FALSE
#ifndef FALSE
#ifndef FALSE_DEFINED
#define FALSE_DEFINED
const NABoolean FALSE = (0 == 1);
#endif
#endif

#endif  // NABOOLEAN_H
