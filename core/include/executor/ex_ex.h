#ifndef EX_EX_H
#define EX_EX_H

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
#include "common/Platform.h"

// typedef	int		(*funcptr) (void *);
typedef int funcptr;  // for now

#define logInternalError(r) ((short)r)

#define ex_assert(p, msg)                        \
  if (!(p)) {                                    \
    assert_botch_abend(__FILE__, __LINE__, msg); \
  };

class ex_expr;  // to be defined

// other classes referenced

class ExConstants {
 public:
  enum { EX_TRUE, EX_FALSE };
};

#endif
