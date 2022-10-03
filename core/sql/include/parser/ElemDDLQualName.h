/**********************************************************************
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
**********************************************************************/
#ifndef ELEMDDLQUALNAME_H
#define ELEMDDLQUALNAME_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLQualName.h
 * Description:  an element representing a qualified name
 *					used as a node in a list of qualified names
 *               
 * Created:      06/20/99
 * Language:     C++
 * Project:
 *
 *
 *
 *****************************************************************************
 */


#include "ElemDDLNode.h"
//#include "common/ComSmallDefs.h"
#include "common/NAString.h"
#include "optimizer/ObjectNames.h"

class ElemDDLQualName;

class ElemDDLQualName : public ElemDDLNode
{

public:

  ElemDDLQualName(const QualifiedName & qualName);

  virtual ~ElemDDLQualName();

  virtual ElemDDLQualName * castToElemDDLQualName();

  inline const NAString getName() const;
  inline const QualifiedName & getQualifiedName() const;
  inline       QualifiedName & getQualifiedName() ;



private:

  QualifiedName qualName_;

}; // class ElemDDLQualName 

//----------------------------------------------------------------------------
inline const NAString 
ElemDDLQualName::getName() const
{
  return qualName_.getQualifiedNameAsAnsiString();
}


inline QualifiedName &
ElemDDLQualName::getQualifiedName()
{
  return qualName_;
}

inline const QualifiedName & 
ElemDDLQualName::getQualifiedName() const
{
  return qualName_;
}


#endif // ELEMDDLQUALNAME_H

