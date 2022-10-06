#ifndef NAEXIT_H
#define NAEXIT_H
/* -*-C++-*-
**************************************************************************
*
* File:         NAExit.h
* Description:  Simple error functions that should be (globally) common
* Created:      7/18/1998
* Language:     C++
*

*
**************************************************************************
*/

// DO NOT ADD ANY DEPENDENCIES TO THIS FILE!!!
//
// THIS FILE IS INTENDED TO BE GLOBALLY USEABLE/USEFUL, AND THE ONLY WAY
// THAT WILL WORK IS IF IT'S SAFE FOR ANYONE TO INCLUDE IT!

// NAExit() is just a simple wrapper around exit() which calls
// NAError_stub_for_breakpoints() (in NAError.h/BaseTypes.cpp) before
// quitting the process completely

void NAExit(int status);  // fn body is in BaseTypes.cpp

#endif /* NAEXIT_H */
