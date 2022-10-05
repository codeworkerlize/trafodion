#ifndef _LM_ASSERT_H_
#define _LM_ASSERT_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         LmAssert.h
 * Description:  assertion function of the language manager
 *
 *
 * Created:      5/4/02
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */
#include <iostream>
#include "common/Platform.h"

#define LM_ASSERT(p)                  \
  if (!(p)) {                         \
    lmAssert(__FILE__, __LINE__, ""); \
  }

#define LM_ASSERT1(p, m)               \
  if (!(p)) {                          \
    lmAssert(__FILE__, __LINE__, (m)); \
  }

void lmAssert(const char *file, int linenum, const char *msg = "");

#endif
