/* -*-C++-*-
 *****************************************************************************
 *
 * File:         test3.C
 * Description:  driver to test my exception handling mechanism
 *
 *
 * Created:      2/9/96
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include <iostream>

#include "EHCommonDefs.h"
#include "eh/EHException.h"  // <--- contains EH macro defs

// -----------------------------------------------------------------------
// global test routine forward declarations
// -----------------------------------------------------------------------

void DoSomething();
void DoSomething2();
void DoMore();

// -----------------------------------------------------------------------
// main entry
// -----------------------------------------------------------------------
main() {
  cout << "in main - beginning of test program" << endl;

  //
  // Must place all EH_REGISTER lines right before EH_TRY line
  //
  EH_REGISTER(EH_ARITHMETIC_OVERFLOW);  // <--- xxxxx
  EH_REGISTER(EH_OUT_OF_RANGE);         // <--- xxxxx
  EH_TRY                                // <--- try block
  {
    cout << "in main - in try block before calling DoSomething" << endl;
    DoSomething();
    DoSomething2();
  }
  EH_END_TRY                        // <--- xxxxx
  EH_CATCH(EH_ARITHMETIC_OVERFLOW)  // <--- catch block
  {
    cout << "in main - in Arithmetic Overflow catch block" << endl;
  }
  EH_CATCH(EH_OUT_OF_RANGE)  // <--- catch block
  {
    cout << "in main - in Out of Range catch block" << endl
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

  EH_REGISTER(EH_ALL_EXCEPTIONS);  // <---
  EH_TRY                           // <--- try block
  {
    cout << "in DoSomething - in try block" << endl;
  }
  EH_END_TRY                   // <--- xxxxx
  EH_CATCH(EH_ALL_EXCEPTIONS)  // <--- catch block
  {
    cout << "in DoSomething - in All Exceptions (...) catch block" << endl
         << "in DoSomething - line " << __LINE__ << " : "
         << " *** Error *** This line is not supposed to be printed" << endl;
  }
  cout << "in DoSomething - end of routine" << endl;
}

void DoSomething2() {
  cout << "in DoSomething2 - before try block" << endl;

  EH_REGISTER(EH_ALL_EXCEPTIONS);  // <---
  EH_REGISTER(EH_OUT_OF_RANGE);    // <---
  EH_TRY                           // <--- try block
  {
    cout << "in DoSomething2 - in try block before calling DoMore" << endl;
    DoMore();
  }
  EH_END_TRY                 // <--- xxxxx
  EH_CATCH(EH_OUT_OF_RANGE)  // <--- catch block
  {
    cout << "in DoSomething2 - in Out of Range catch block" << endl
         << "                 before throw Out of Range statement" << endl
         << "in DoSomething2 - line " << __LINE__ << " : "
         << " *** Error *** This line is not supposed to be printed" << endl;
    EH_THROW(EH_OUT_OF_RANGE);  // <--- throw
  }
  EH_CATCH(EH_ALL_EXCEPTIONS)  // <--- catch block
  {
    cout << "in DoSomething2 - in All Exceptions (...) catch block" << endl
         << "                  EH_ARITHMETIC_OVERFLOW caught" << endl
         << "in DoSomething2 - before throw Arithmetic Overflow statement" << endl;

    EH_THROW(EH_ARITHMETIC_OVERFLOW);
  }

  cout << "in DoSomething2 - line " << __LINE__ << " : "
       << " *** Error *** This line is not supposed to be printed" << endl;
}

void DoMore() {
  cout << "in DoMore - before throw Arithmetic Overflow statement" << endl;

  EH_THROW(EH_ARITHMETIC_OVERFLOW);  // <--- throw

  cout << " in DoMore - line " << __LINE__ << " : "
       << " *** Error *** This line is not supposed to be printed" << endl;
}

//
// End Of File
//
