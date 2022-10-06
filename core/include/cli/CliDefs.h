
#ifndef CLI_STDH_H
#define CLI_STDH_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

enum RETCODE {
  SUCCESS = 0,
  SQL_EOF = 100,
  ERROR = -1,
  WARNING = 1,
  NOT_FINISHED = 2  // returned when a no-wait operation is incomplete
};

#include "common/ComSpace.h"
typedef Space Heap;

#endif
