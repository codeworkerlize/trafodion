
/* -*-C++-*-
****************************************************************************
*
* File:         ComKeySingleSubset.cpp
* Description:
*
* Created:      5/6/98
* Language:     C++
*
*
*
*
****************************************************************************
*/

// -----------------------------------------------------------------------

#include "comexe/ComKeySingleSubset.h"

#include "comexe/ComKeyRange.h"
#include "comexe/ComPackDefs.h"
#include "exp/exp_expr.h"

keySingleSubsetGen::keySingleSubsetGen(int keyLen, ex_cri_desc *workCriDesc, unsigned short keyValuesAtpIndex,
                                       unsigned short excludeFlagAtpIndex, unsigned short dataConvErrorFlagIndex,
                                       ex_expr *bk_pred, ex_expr *ek_pred, ex_expr *bkey_excluded_expr,
                                       ex_expr *ekey_excluded_expr, short bkey_excluded, short ekey_excluded)
    : keyRangeGen(KEYSINGLESUBSET, keyLen, workCriDesc, keyValuesAtpIndex, excludeFlagAtpIndex, dataConvErrorFlagIndex),
      bkPred_(bk_pred),
      ekPred_(ek_pred),
      lowKeyExcludedExpr_(bkey_excluded_expr),
      highKeyExcludedExpr_(ekey_excluded_expr),
      lowKeyExcluded_(bkey_excluded),
      highKeyExcluded_(ekey_excluded){};

keySingleSubsetGen::~keySingleSubsetGen(){};

Long keySingleSubsetGen::pack(void *space) {
  bkPred_.pack(space);
  ekPred_.pack(space);
  lowKeyExcludedExpr_.pack(space);
  highKeyExcludedExpr_.pack(space);
  return keyRangeGen::pack(space);
};

int keySingleSubsetGen::unpack(void *base, void *reallocator) {
  if (bkPred_.unpack(base, reallocator)) return -1;
  if (ekPred_.unpack(base, reallocator)) return -1;
  if (lowKeyExcludedExpr_.unpack(base, reallocator)) return -1;
  if (highKeyExcludedExpr_.unpack(base, reallocator)) return -1;
  return keyRangeGen::unpack(base, reallocator);
};

ex_expr *keySingleSubsetGen::getExpressionNode(int pos) {
  if (pos == 0)
    return bkPred_;
  else if (pos == 1)
    return ekPred_;
  else
    return NULL;
}
