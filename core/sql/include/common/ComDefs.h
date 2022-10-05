#ifndef COMDEFS_H
#define COMDEFS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComDefs.h
 * Description:  Common defines used by both the optimizer and executor.
 *
 * Created:      7/06/99
 * Language:     C++
 *

 *
 *****************************************************************************
 */

///////////////////////////////////////////////////////////////////
// This file is included by executor, do not add any includes in
// here to make it executor-noncompliant.
///////////////////////////////////////////////////////////////////
#define ROUND2(size) Long(((size) + 1) & (~1))

#define ROUND4(size) Long(((size) + 3) & (~3))

#define ROUND8(size) Long(((size) + 7) & (~7))

#define ROUND16(size) Long(((size) + 15) & (~15))

#define ROUNDto1KinB(size) Long(((size - 1) / 1024 + 1) * 1024)

#define ROUNDto1KinK(size) Long(ROUNDto1KinB(size) / 1024)

#define ROUNDto4KinB(size) Long(((size - 1) / 4096 + 1) * 4096)

#define ROUNDto4KinK(size) Long(ROUNDto4KinB(size) / 1024)

// X/Y rounded up to the next integer, not rounded down as in C++
// Use integral data types and make sure there is no overflow
#define DIVIDE_AND_ROUND_UP(X, Y) (((X) + (Y)-1) / (Y))

#endif  // COMDEFS_H
