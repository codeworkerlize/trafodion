
/* -*-C++-*-
******************************************************************************
*
* File:         cmpmod.cpp
* Description:
*
* Created:      10/10/1995
* Language:     C++
*
*
******************************************************************************
*/

// runtime recompilation & rebinding of a statically compiled SQL module's:
//   SQL binary module,
//   SQL statement,
//   SQL cursor,
//   SQL string
// was some Tandem developer's pipe dream that was coded but
// was apparently never used and never worked!

// The design was to read from a module file and create another
// module file that contains the rebind object code.  After the
// rebind is successful, CATMAN will remove the original module
// file and rename the new module file to be the same name as
// the original file.
