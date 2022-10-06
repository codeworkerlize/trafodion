
#include "common/CharType.h"

NABoolean NAType::isComparable(const NAType &other, ItemExpr *parentOp, int emitErr) const { return FALSE; }

NABoolean CharType::isComparable(const NAType &other, ItemExpr *parentOp, int emitErr) const { return FALSE; }
