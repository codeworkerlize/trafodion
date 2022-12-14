
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComSysUtil.C
 * Description:  Utility functions required in arkcode which do make
 *               system or runtime library calls.
 *
 * Created:      4/10/96, evening
 * Language:     C++
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComSysUtils.h"

#include "common/NAAssert.h"
#include "common/Platform.h"

//----------------------------------------------------------------
// This section of the code should be removed when the real
// gettimeofday call is available under OSS. Also remember
// to remove the function declaration from SqlciStats.h

// ****************************************************************************
// *                                                                          *
// * Function: gettimeofday                                                   *
// *                                                                          *
// *    This function partially duplicates the UNIX function by the same      *
// *    name.  In particular, nothing is returned in the timezone argument    *
// *    which is asserted to be zero.                                         *
// *                                                                          *
// * NOTE:     The resolution of the value returned in the timeval argument   *
// *           is 1 MILLIsecond!                                              *
// *              =============                                               *
// *                                                                          *
// ****************************************************************************
// *                                                                          *
// *  Parameters:                                                             *
// *                                                                          *
// *  <tp>                      struct timeval *                Out           *
// *    is used to return the seconds and microseconds since 12:00 am on      *
// *    January 1st, 1970 in the tv_sec and tv_usec fields, repectively, per  *
// *    the UNIX documentation.                                               *
// *                                                                          *
// *  <tzp>                     struct timezone *               In            *
// *    is not used and is asserted to be zero.                               *
// *                                                                          *
// *                                                                          *
// ****************************************************************************
// *                                                                          *
// *  Returns: 0, if all ok, -1, if error.                                    *
// *                                                                          *
// ****************************************************************************

extern "C" {
int NA_gettimeofday(struct NA_timeval *tp, struct NA_timezone *tzp) {
  return gettimeofday(tp, 0);

  return (0);
}

}  // extern "C"

//----------------------------------------------------------------

void copyInteger(void *destination, int targetLength, void *sourceAddress, int sourceLength) {
  switch (targetLength) {
    case SQL_TINY_SIZE: {
      Int8 *target = (Int8 *)destination;
      copyToInteger1(target, sourceAddress, sourceLength);
      break;
    }
    case SQL_SMALL_SIZE: {
      short *target = (short *)destination;
      copyToInteger2(target, sourceAddress, sourceLength);
      break;
    }
    case SQL_INT_SIZE: {
      int *target = (int *)destination;
      copyToInteger4(target, sourceAddress, sourceLength);
      break;
    }

    case SQL_LARGE_SIZE: {
      long *target = (long *)destination;
      copyToInteger8(target, sourceAddress, sourceLength);
      break;
    }

    default:
      break;
  }
}

void copyToInteger1(Int8 *destination, void *sourceAddress, int sourceSize) {
  switch (sourceSize) {
    case SQL_TINY_SIZE: {
      Int8 *source = (Int8 *)sourceAddress;
      *destination = *source;
      break;
    }

    case SQL_SMALL_SIZE: {
      short *source = (short *)sourceAddress;
      *destination = (Int8)*source;
      break;
    }

    case SQL_INT_SIZE: {
      int *source = (int *)sourceAddress;
      *destination = (Int8)*source;
      break;
    }

    case SQL_LARGE_SIZE: {
      long *source = (long *)sourceAddress;
      *destination = (Int8)*source;
      break;
    }

    default:
      break;
  }
}

void copyToInteger2(short *destination, void *sourceAddress, int sourceSize) {
  switch (sourceSize) {
    case SQL_SMALL_SIZE: {
      short *source = (short *)sourceAddress;
      *destination = *source;
      break;
    }

    case SQL_INT_SIZE: {
      int *source = (int *)sourceAddress;
      *destination = (short)*source;
      break;
    }

    case SQL_LARGE_SIZE: {
      long *source = (long *)sourceAddress;
      *destination = (short)*source;
      break;
    }

    default:
      break;
  }
}

void copyToInteger4(int *destination, void *sourceAddress, int sourceSize) {
  switch (sourceSize) {
    case SQL_SMALL_SIZE: {
      short *source = (short *)sourceAddress;
      *destination = (int)*source;
      break;
    }

    case SQL_INT_SIZE: {
      int *source = (int *)sourceAddress;
      *destination = *source;
      break;
    }

    case SQL_LARGE_SIZE: {
      long *source = (long *)sourceAddress;
      *destination = (int)*source;
      break;
    }

    default:
      break;
  }
}

void copyToInteger8(long *destination, void *sourceAddress, int sourceSize) {
  switch (sourceSize) {
    case SQL_SMALL_SIZE: {
      short *source = (short *)sourceAddress;
      *destination = (long)*source;
      break;
    }

    case SQL_INT_SIZE: {
      int *source = (int *)sourceAddress;
      *destination = (long)*source;
      break;
    }

    case SQL_LARGE_SIZE: {
      long *source = (long *)sourceAddress;
      *destination = *source;
      break;
    }

    default:
      break;
  }
}
