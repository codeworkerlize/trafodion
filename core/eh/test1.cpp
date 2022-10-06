/* -*-C++-*-
 *****************************************************************************
 *
 * File:         test1.C
 * Description:  driver to test my exception handling mechanism
 *
 *
 * Created:      5/20/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include <iostream>

#include "EHCommonDefs.h"
#include "eh/EHException.h"  // <---

// -----------------------------------------------------------------------
// global test routine forward declarations
// -----------------------------------------------------------------------

void DoSomething();
void DoMore();

// -----------------------------------------------------------------------
// main entry
// -----------------------------------------------------------------------
main() {
  cout << "in main - beginning of test program" << endl;

  EH_REGISTER(EH_ARITHMETIC_OVERFLOW);  // <---
  EH_REGISTER(EH_OUT_OF_RANGE);         // <---
  EH_TRY                                // <---
  {
    cout << "in main - in try block before calling DoSomething" << endl;
    DoSomething();
  }
  EH_END_TRY                        // <---
  EH_CATCH(EH_ARITHMETIC_OVERFLOW)  // <---
  {
    cout << "in main - in Arithmetic Overflow catch block" << endl;
  }
  EH_CATCH(EH_OUT_OF_RANGE)  // <---
  {
    cout << "in main - in Out of Range catch block" << endl;
  }

  cout << "in main - end of test program" << endl << "-----------------------------" << endl;
  return 0;
}

// -----------------------------------------------------------------------
// global test routines
// -----------------------------------------------------------------------

void DoSomething() {
  cout << "in DoSomething - before try block" << endl;

  DoMore();

  cout << "file " << __FILE__ << " - line " << __LINE__ << " : "
       << "This line is not supposed to be printed" << endl;
}

void DoMore() {
  cout << "in DoMore - before throw Out of Range statement" << endl;

  EH_THROW(EH_OUT_OF_RANGE);  // <---

  cout << "file " << __FILE__ << " - line " << __LINE__ << " : "
       << "This line is not supposed to be printed" << endl;
}
