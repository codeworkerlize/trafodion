
#ifndef BitMuxFunction_h
#define BitMuxFunction_h

// This class is obsolete in the sense
// that it had been added long time ago during the data
// mining days (late 90s) but is not used anymore

// Includes
//
#include "optimizer/ItemFunc.h"

// Forward External Declarations
//
class ItemExpr;
class BuiltinFunction;
class Generator;
class NAMemory;

// Forward Internal Declarations
//
class ItmBitMuxFunction;

class ItmBitMuxFunction : public BuiltinFunction {
 public:
  ItmBitMuxFunction(const LIST(ItemExpr *) & children)
      : BuiltinFunction(ITM_BITMUX, CmpCommon::statementHeap(), children){};

  virtual ~ItmBitMuxFunction();

  const NAType *synthesizeType();
  ItemExpr *preCodeGen(Generator *);
  short codeGen(Generator *);
  ItemExpr *copyTopNode(ItemExpr *derivedNode = NULL, NAMemory *outHeap = 0);
};

#endif
