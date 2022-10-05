
#ifndef ERRORCONDITION_H
#define ERRORCONDITION_H
/* -*-C++-*-
**************************************************************************
*
* File:         ErrCondition.h
* Description:  The Error Condition (an internal SQLSTATE)
* Created:      01/23/95
* Language:     C++
*
*
**************************************************************************
*/

// -----------------------------------------------------------------------

#include "export/NABasicObject.h"

// ***********************************************************************
// The SQLSTATE is a parameter that is maintained by the SQL executor for
// recording the status of execution. It records the status for a set of
// conditions specified by SQL2 (X3H2-93-004, Subclause 22.1, pp 523-527).
// The ErrCondition class is an internal version of the SQLSTATE.
//
// Each condition is implemented as a derived class of the class
// ErrCondition. It supports two virtual functions that return the 2
// character  code for the condition that caused the ErrCondition to be
// initialized and a 3 character code that provides further detail. SQL2
// calls them the SQLSTATE class and subclass respectively.
//
// Note that the ErrCondition allocates storage only for recording the
// subclass of the SQLSTATE. The SQLSTATE class is reflected in the
// different derived classes of the ErrCondition.
//
// ***********************************************************************

#endif /* ERRORCONDITION_H */
