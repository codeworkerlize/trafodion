

// Includes
//
#include "optimizer/ItemExpr.h"
#include "ItmBitMuxFunction.h"
#include "generator/Generator.h"
#include "GenExpGenerator.h"
#include "ExpBitMuxFunction.h"

ItemExpr *ItmBitMuxFunction::preCodeGen(Generator *generator) {
  if (nodeIsPreCodeGenned()) return this;

  int nc = (int)getArity();

  for (int index = 0; index < nc; index++) {
    // during key encode expr generation, no need to convert external
    // column types(like tandem floats) to their internal
    // equivalent(ieee floats). Avoid doing preCodeGen in these cases.
    //
    if (NOT child(index)->getValueId().getType().isExternalType()) {
      child(index) = child(index)->preCodeGen(generator);
      if (!child(index).getPtr()) return NULL;
    }
  }

  markAsPreCodeGenned();

  return this;
}

short ItmBitMuxFunction::codeGen(Generator *generator) {
  Attributes **attr;

  if (generator->getExpGenerator()->genItemExpr(this, &attr, (1 + getArity()), -1) == 1) return 0;

  ex_clause *function_clause =
      new (generator->getSpace()) ExpBitMuxFunction(getOperatorType(), 1 + getArity(), attr, generator->getSpace());

  generator->getExpGenerator()->linkClause(this, function_clause);

#ifdef _DEBUG
  int totalLength = 0;

  for (int i = 0; i < getArity(); i++) {
    totalLength += function_clause->getOperand((short)(i + 1))->getStorageLength();
  }

  GenAssert(totalLength == function_clause->getOperand(0)->getLength(), "Not enough storage allocated for bitmux");
#endif

  return 0;
}
