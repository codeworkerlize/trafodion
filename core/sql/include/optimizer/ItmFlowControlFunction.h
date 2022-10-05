
#ifndef ItmFlowControlFunction_h
#define ItmFlowControlFunction_h

// Includes
//
#include "optimizer/ItemFunc.h"
#include "optimizer/ItemColRef.h"

// Forward External Declarations
//
class ItemExpr;
class BuiltinFunction;
class Generator;
class NAMemory;

// Forward Internal Declarations
//
class ItmDoWhileFunction;
class ItmBlockFunction;
class ItmExpressionVariable;
class ItmPersistentExpressionVariable;
class ItmWhileFunction;

class ItmDoWhileFunction : public BuiltinFunction {
 public:
  ItmDoWhileFunction(ItemExpr *body, ItemExpr *condition)
      : BuiltinFunction(ITM_DO_WHILE, CmpCommon::statementHeap(), 2, body, condition){};

  virtual ~ItmDoWhileFunction(){};

  const NAType *synthesizeType();
  ItemExpr *preCodeGen(Generator *);
  short codeGen(Generator *);
  ItemExpr *copyTopNode(ItemExpr *derivedNode = NULL, NAMemory *outHeap = 0);
};

class ItmBlockFunction : public BuiltinFunction {
 public:
  ItmBlockFunction(ItemExpr *left, ItemExpr *right)
      : BuiltinFunction(ITM_BLOCK, CmpCommon::statementHeap(), 2, left, right){};

  virtual ~ItmBlockFunction(){};

  const NAType *synthesizeType();
  ItemExpr *preCodeGen(Generator *);
  short codeGen(Generator *);
  ItemExpr *copyTopNode(ItemExpr *derivedNode = NULL, NAMemory *outHeap = 0);
};

class ItmExpressionVar : public HostVar {
 public:
  ItmExpressionVar(const NAType *type) : HostVar("_sys_ItmExpressionVariable", type, TRUE){};

  virtual ~ItmExpressionVar(){};
};

class ItmPersistentExpressionVar : public ConstValue {
 public:
  ItmPersistentExpressionVar(int intVal) : ConstValue(intVal) { setConstFoldingDisabled(TRUE); };

  /*  ItmPersistentExpressionVar(NAType *type,
                             NABoolean wantMinValue =FALSE,
                             NABoolean allowNull =TRUE)
    : ConstValue(type, wantMinValue, allowNull) { };*/

  // Supply a type, a buffer containing the packed value,
  // the size of the buffer and , optionally, the string
  // for the literal (system-supplied version)
  ItmPersistentExpressionVar(const NAType *type, void *value, int value_len, NAString *literal = NULL)
      : ConstValue(type, value, value_len, literal) {
    setConstFoldingDisabled(TRUE);
  }

  virtual ~ItmPersistentExpressionVar(){};

  short codeGen(Generator *);
};

class ItmWhileFunction : public BuiltinFunction {
 public:
  ItmWhileFunction(ItemExpr *body, ItemExpr *condition)
      : BuiltinFunction(ITM_WHILE, CmpCommon::statementHeap(), 2, body, condition){};

  virtual ~ItmWhileFunction(){};

  const NAType *synthesizeType();
  ItemExpr *preCodeGen(Generator *);
  short codeGen(Generator *);
  ItemExpr *copyTopNode(ItemExpr *derivedNode = NULL, NAMemory *outHeap = 0);
};

#endif
