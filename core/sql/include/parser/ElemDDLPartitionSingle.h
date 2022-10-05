#ifndef ELEMDDLPARTITIONSINGLE_H
#define ELEMDDLPARTITIONSINGLE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLPartitionSingle.h
 * Description:  class to contain information about a single partition
 *               element specified in a DDL statement.  As the name
 *               implies, only single partition is involved (the
 *               primary partition).
 *
 *
 * Created:      7/27/96
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "ElemDDLPartitionSystem.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLPartitionSingle;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of class ElemDDLPartitionSingle
// -----------------------------------------------------------------------
class ElemDDLPartitionSingle : public ElemDDLPartitionSystem {
 public:
  // constructors
  ElemDDLPartitionSingle();

  // virtual destructor
  virtual ~ElemDDLPartitionSingle();

  // cast
  virtual ElemDDLPartitionSingle *castToElemDDLPartitionSingle();

 private:
  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  ElemDDLPartitionSingle(const ElemDDLPartitionSingle &);

  // copy constructor not supported

  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------
  //
  // none
  //

};  // class ElemDDLPartitionSingle

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLPartitionSingle
// -----------------------------------------------------------------------
//
// none
//

#endif  // ELEMDDLPARTITIONSINGLE_H
