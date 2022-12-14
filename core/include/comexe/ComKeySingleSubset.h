
/* -*-C++-*-
****************************************************************************
*
* File:         ComKeySingleSubset.h
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

#ifndef COMKEYSINGLESUBSET_H
#define COMKEYSINGLESUBSET_H

#include "comexe/ComKeyRange.h"

class ex_expr;
class ex_cri_desc;

// -----------------------------------------------------------------------

/////////////////////////////////////////////////////////////////////////
//
// Class keySingleSubsetGen
//
// This class contains compiler-generated information used by scan
// operators that access a single key range.  It encapsulates methods
// needed to compute the begin and end key values and their exclusion
// flags.
//
//
/////////////////////////////////////////////////////////////////////////

class keySingleSubsetGen : public keyRangeGen {
 private:
  // expressions that compute begin and end keys respectively
  ExExprPtr bkPred_;  // 00-07
  ExExprPtr ekPred_;  // 08-15

  // boolean expression to compute whether low and high key should
  // be included in the range or subset
  ExExprPtr lowKeyExcludedExpr_;   // 16-23
  ExExprPtr highKeyExcludedExpr_;  // 24-31

  // hard-coded indicators whether to include low and/or high key
  // (used if the expressions above are NULL)
  Int16 lowKeyExcluded_;   // 32-33
  Int16 highKeyExcluded_;  // 34-35

  char fillersKeySingleSubsetGen_[20];  // 36-55

 public:
  keySingleSubsetGen(){};  // default constructor used by UNPACK

  keySingleSubsetGen(int keyLen, ex_cri_desc *workCriDesc, unsigned short keyValuesAtpIndex,
                     unsigned short excludeFlagAtpIndex, unsigned short dataConvErrorFlagAtpIndex, ex_expr *bk_pred,
                     ex_expr *ek_pred, ex_expr *bkey_excluded_expr, ex_expr *ekey_excluded_expr, short bkey_excluded,
                     short ekey_excluded);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    keyRangeGen::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(keySingleSubsetGen); }

  virtual ~keySingleSubsetGen();

  virtual Long pack(void *space);
  virtual int unpack(void *base, void *reallocator);

  virtual ex_expr *getExpressionNode(int pos);

  virtual keySingleSubsetGen *castToKeySingleSubsetGen() { return this; }

  // accessor functions
  inline ex_expr *bkPred() const { return bkPred_; };

  inline ex_expr *ekPred() const { return ekPred_; };

  inline ex_expr *bkExcludedExpr() const { return lowKeyExcludedExpr_; }

  inline ex_expr *ekExcludedExpr() const { return highKeyExcludedExpr_; }

  inline short isBkeyExcluded() const { return lowKeyExcluded_; }

  inline short isEkeyExcluded() const { return highKeyExcluded_; }
};

#endif
