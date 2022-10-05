
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
int smallestFactor(int x);

// find the largest factor for x
int largestFactor(int x);

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
int closestFactor(int x, int y, int z);

// return a value that is a multple of y and closest to x
// The returned value can be either smaller or larger than x
int adjustToMultiple(int x, int y);

#endif  // UTILITY_H
