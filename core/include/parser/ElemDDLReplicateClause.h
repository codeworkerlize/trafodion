#ifndef ELEMDDLREPLICATECLAUSE_H
#define ELEMDDLREPLICATECLAUSE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLReplicateClause.h
 * Description:  Classes representing REPLICATE clause specified in
 *               create DDL for tables
 *
 *
 * Created:      8/21/2019
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"
#include "common/ComSmallDefs.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLReplicateClause;

// -----------------------------------------------------------------------
// class ElemDDLReplicateClause
// -----------------------------------------------------------------------

class ElemDDLReplicateClause : public ElemDDLNode {
 public:
  //
  // constructors
  //

  ElemDDLReplicateClause(Int16 numTrafReplicas);

  // virtual destructor
  virtual ~ElemDDLReplicateClause();

  // casting
  virtual ElemDDLReplicateClause *castToElemDDLReplicateClause();

  //
  // accessors
  //

  inline Int16 getNumTrafReplicas() const { return numTrafReplicas_; }

  // name we choose for the system column that contains the replica value
  static const char *getReplicaSysColName() { return "_REPLICA_"; }

  //
  // mutators
  //

  void setNumReplicas(Int16 numTrafReplicas) { numTrafReplicas_ = numTrafReplicas; }

  //
  // methods for tracing and/or building text
  //

  virtual const NAString getText() const;

  void unparseIt(NAString &result) const;

 private:
  //
  // Private methods
  //

  ElemDDLReplicateClause(const ElemDDLReplicateClause &rhs);             // not defined - DO NOT USE
  ElemDDLReplicateClause &operator=(const ElemDDLReplicateClause &rhs);  // not defined - DO NOT USE

  //
  // data members
  //

  Int16 numTrafReplicas_;

};  // class ElemDDLReplicateClause

#endif  // ELEMDDLREPLICATECLAUSE_H
