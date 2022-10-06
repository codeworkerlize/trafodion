

// This class is obsolete in the sense
// that it had been added long time ago during the data
// mining days (late 90s) but is not used anymore
#include "common/CharType.h"
#include "common/NumericType.h"
#include "optimizer/ItemExpr.h"
#include "ItmBitMuxFunction.h"

// Destructor
//
ItmBitMuxFunction::~ItmBitMuxFunction() { ; };

// synthesizeType
//
const NAType *ItmBitMuxFunction::synthesizeType() {
  int size = 0;
  for (int i = 0; i < getArity(); i++) {
    const NAType &type = child(i)->getValueId().getType();
    size += type.getTotalSize();
  }

  return new (CmpCommon::statementHeap()) SQLChar(CmpCommon::statementHeap(), size, FALSE);
};

// copyTopNode
//
ItemExpr *ItmBitMuxFunction::copyTopNode(ItemExpr *derivedNode, CollHeap *outHeap) {
  ItemExpr *result;

  if (derivedNode == NULL) {
    LIST(ItemExpr *) item(outHeap);
    result = new (outHeap) ItmBitMuxFunction(item);
  } else
    result = derivedNode;

  return BuiltinFunction::copyTopNode(result, outHeap);
};
