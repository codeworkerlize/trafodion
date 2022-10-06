/**********************************************************************

/**********************************************************************/
#ifndef ENCODEDKEYVALUE_H
#define ENCODEDKEYVALUE_H
/* -*-C++-*-
/**************************************************************************
*
* File:         EncodedKeyValue.h
* Description:  Functions to compute binary encoded keys that can be written
                to disk for a given set of TrafDescs.
* Origin:
* Created:      10/30/2013
* Language:     C++
*
*************************************************************************
*/

#include "common/NAString.h"
#include "common/Platform.h"
#include "generator/Generator.h"

NAString *getMinMaxValue(TrafDesc *column, TrafDesc *key, NABoolean highKey, CollHeap *h);

NAString **createInArrayForLowOrHighKeys(TrafDesc *column_descs, TrafDesc *key_descs, int numKeys, NABoolean highKey,
                                         NABoolean isIndex, CollHeap *h);

ItemExpr *buildEncodeTree(TrafDesc *column, TrafDesc *key,
                          NAString *dataBuffer,  // IN:contains original value
                          Generator *generator, ComDiagsArea *diagsArea);

short encodeKeyValues(TrafDesc *column_descs, TrafDesc *key_descs,
                      NAString *inValuesArray[],  // INPUT
                      NABoolean isIndex,
                      NABoolean isMaxKey,      // INPUT
                      char *encodedKeyBuffer,  // OUTPUT
                      CollHeap *h, ComDiagsArea *diagsArea);

#endif /* ENCODEDKEYVALUE_H */
