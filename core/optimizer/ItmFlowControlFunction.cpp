

#include "optimizer/ItmFlowControlFunction.h"

#include "common/CharType.h"
#include "common/NumericType.h"
#include "optimizer/ItemExpr.h"

//
// ItmDoWhile
//

// synthesizeType
//
const NAType *ItmDoWhileFunction::synthesizeType() { return &child(0)->castToItemExpr()->getValueId().getType(); };

// copyTopNode
//
ItemExpr *ItmDoWhileFunction::copyTopNode(ItemExpr *derivedNode, CollHeap *outHeap) {
  ItemExpr *result;

  if (derivedNode == NULL) {
    result = new (outHeap) ItmDoWhileFunction(NULL, NULL);
  } else
    result = derivedNode;

  return BuiltinFunction::copyTopNode(result, outHeap);
};

//
// ItmBlockFunction
//

// synthesizeType
//
// Returns the type of the second argument.
//
const NAType *ItmBlockFunction::synthesizeType() { return &child(1)->castToItemExpr()->getValueId().getType(); };

// copyTopNode
//
ItemExpr *ItmBlockFunction::copyTopNode(ItemExpr *derivedNode, CollHeap *outHeap) {
  ItemExpr *result;

  if (derivedNode == NULL) {
    result = new (outHeap) ItmBlockFunction(NULL, NULL);
  } else
    result = derivedNode;

  return BuiltinFunction::copyTopNode(result, outHeap);
};

//
// ItmWhile
//

// synthesizeType
//
const NAType *ItmWhileFunction::synthesizeType() { return &child(0)->castToItemExpr()->getValueId().getType(); };

// copyTopNode
//
ItemExpr *ItmWhileFunction::copyTopNode(ItemExpr *derivedNode, CollHeap *outHeap) {
  ItemExpr *result;

  if (derivedNode == NULL) {
    result = new (outHeap) ItmWhileFunction(NULL, NULL);
  } else
    result = derivedNode;

  return BuiltinFunction::copyTopNode(derivedNode, outHeap);
};
