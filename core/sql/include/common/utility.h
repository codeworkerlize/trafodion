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
#ifndef UTILITY_H
#define UTILITY_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         utility.h
 * Description:  utility functions. 
 * Created:      3/23/2017
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Platform.h"

// -----------------------------------------------------------------------

// find the smallest factor for x that is greater than 1.
Int32 smallestFactor(Int32 x);

// find the largest factor for x
Int32 largestFactor(Int32 x);

// find the closet factor of y that is closet to x and
// a multiple of z. One use is to adjust the maxDoP of
// a plan such that the adjusted mxDoP is a factor of
// the total number of CPUs and also a multiple of the
// number of Nodes.
//
//  closestFactor(maxDoP, numCPUs, numNodes);
//
//  where 
//    numCPUs is <n> * numNodes, where <n> is the # of 
//    usable cores in a node.
//
//
Int32 closestFactor(Int32 x, Int32 y, Int32 z);

// return a value that is a multple of y and closest to x
// The returned value can be either smaller or larger than x
Int32 adjustToMultiple(Int32 x, Int32 y);

#endif // UTILITY_H

