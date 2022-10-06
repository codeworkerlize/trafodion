
#ifndef _INT_TYPE_H_
#define _INT_TYPE_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         IntType.h
 * Description:  Integer types used in the adaptor functions to work with
 *               NSJ 3.0 IEEE float support
 *
 *
 * Created:      8/15/02
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
// 64-bit: get int and long from Platform.h and long.h
#include "common/Int64.h"
#include "common/Platform.h"
#typedef long long;
#typedef int int;
#endif
