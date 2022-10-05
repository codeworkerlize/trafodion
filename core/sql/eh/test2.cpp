/* -*-C++-*-
 *****************************************************************************
 *
 * File:         test2.C
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

#include "EHCommonDefs.h"

#include <iostream>

#include "eh/EHException.h"  // <--- contains EH macro defs

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

  //
  // Must place all EH_REGISTER lines right before EH_TRY line
  //
  EH_REGISTER(EH_ALL_EXCEPTIONS);       // <---
  EH_REGISTER(EH_ARITHMETIC_OVERFLOW);  // <---
  EH_REGISTER(EH_OUT_OF_RANGE);         // <---
  EH_TRY                                // <--- try block
  {
    cout << "in main - in try block before calling DoSomething" << endl;
    DoSomething();
  }
  EH_END_TRY                        // <---
  EH_CATCH(EH_ARITHMETIC_OVERFLOW)  // <--- catch block
  {
    cout << "in main - in Arithmetic Overflow catch block" << endl
         << "in main - line " << __LINE__ << " : "
         << " *** Error *** This line is not supposed to be printed" << endl;
  }
  EH_CATCH(EH_OUT_OF_RANGE)  // <--- catch block
  {
    cout << "in main - in Out of Range catch block" << endl;
  }
  EH_CATCH(EH_ALL_EXCEPTIONS)  // <--- catch block
  {
    cout << "in main - in All Exceptions (...) catch block" << endl
         << "in main - line " << __LINE__ << " : "
         << " *** Error *** This line is not supposed to be printed" << endl;
  }

  cout << "in main - end of test program" << endl << "-----------------------------" << endl;
  return 0;
}

// -----------------------------------------------------------------------
// global test routines
// -----------------------------------------------------------------------

void DoSomething() {
  cout << "in DoSomething - before try block" << endl;

  EH_REGISTER(EH_OUT_OF_RANGE);  // <---
  EH_TRY                         // <--- try block
  {
    cout << "in DoSomething - in try block before calling DoMore" << endl;
    DoMore();
  }
  EH_END_TRY                 // <---
  EH_CATCH(EH_OUT_OF_RANGE)  // <--- catch block
  {
    cout << "in DoSomething - in Out of Range catch block" << endl
         << "                 before throw Out of Range statement" << endl;
    EH_THROW(EH_OUT_OF_RANGE);  // <--- throw
  }

  cout << "file " << __FILE__ << " - line " << __LINE__ << " : "
       << " *** Error *** This line is not supposed to be printed" << endl;
}

void DoMore() {
  cout << "in DoMore - before throw Out of Range statement" << endl;

  EH_THROW(EH_OUT_OF_RANGE);  // <--- throw

  cout << "file " << __FILE__ << " - line " << __LINE__ << " : "
       << " *** Error *** This line is not supposed to be printed" << endl;
}

//
// End Of File
//
