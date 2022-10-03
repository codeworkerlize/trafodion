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
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLReplicateClause;

// -----------------------------------------------------------------------
// class ElemDDLReplicateClause 
// -----------------------------------------------------------------------

class ElemDDLReplicateClause : public ElemDDLNode
{

public:

  //
  // constructors
  //

  ElemDDLReplicateClause(Int16 numTrafReplicas);

  // virtual destructor
  virtual ~ElemDDLReplicateClause();

  // casting
  virtual ElemDDLReplicateClause * castToElemDDLReplicateClause();

  //
  // accessors
  //

  inline Int16 getNumTrafReplicas() const
  {
    return numTrafReplicas_;
  }

  // name we choose for the system column that contains the replica value
  static const char * getReplicaSysColName() { return "_REPLICA_"; }

  //
  // mutators
  //

  void setNumReplicas(Int16 numTrafReplicas) {numTrafReplicas_ = numTrafReplicas;}

  //
  // methods for tracing and/or building text
  //

  virtual const NAString getText() const;

  void unparseIt(NAString & result) const;

private:

  //
  // Private methods
  //

  ElemDDLReplicateClause(const ElemDDLReplicateClause & rhs) ; // not defined - DO NOT USE
  ElemDDLReplicateClause & operator=(const ElemDDLReplicateClause & rhs) ; // not defined - DO NOT USE

  //
  // data members
  //

  Int16 numTrafReplicas_;
 

}; // class ElemDDLReplicateClause

#endif // ELEMDDLREPLICATECLAUSE_H
