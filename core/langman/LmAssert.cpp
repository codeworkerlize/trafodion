/* -*-C++-*-
 *****************************************************************************
 *
 * File:         LmAssert.cpp
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
#include <stdio.h>
#include <stdlib.h>

#include <iostream>

#include "langman/LmExtFunc.h"
#include "common/Platform.h"

// Exclude this function from coverage as it is called only when there is an assertion in LM
// which results in UDR server abend, so no coverage info can be generated.
void lmAssert(const char *file, int linenum, const char *msg) {
  if (!file) file = "";
  if (!msg) msg = "";

  cout << "LM Assertion: " << msg << endl << "at FILE: " << file << " LINE: " << linenum << endl;

  char message[1060];  // 1024 for 'msg'

  snprintf(message, 1060, "Language Manager internal error : %s", msg);

  lmMakeTFDSCall(message, file, linenum);
  // should not reach here
}
