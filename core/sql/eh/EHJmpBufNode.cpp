/* -*-C++-*-
 *****************************************************************************
 *
 * File:         EHJmpBufNode.C
 * Description:  method for class EHExceptionJmpBufNode
 *
 *
 * Created:      5/19/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "EHCommonDefs.h"

#include <string.h>
#include "EHBaseTypes.h"
#include "EHJmpBufNode.h"

// -----------------------------------------------------------------------
// methods for class EHExceptionJmpBufNode
// -----------------------------------------------------------------------

// mutators

void EHExceptionJmpBufNode::setEnv(const env &envStruct) {
  memcpy((void *)&environment, (void *)&envStruct, sizeof(envStruct));
}
