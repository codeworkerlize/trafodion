
#ifndef EXP_MATH_FUNC_H
#define EXP_MATH_FUNC_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "exp/exp_clause.h"
#include "exp/exp_clause_derived.h"

class ex_function_abs : public ex_function_clause {
 public:
  ex_function_abs(OperatorTypeEnum oper_type, Attributes **attr, Space *space)
      : ex_function_clause(oper_type, 2, attr, space){};
  ex_function_abs(){};

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);
  // Display
  //
  virtual void displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionBitOper : public ex_function_clause {
 public:
  ExFunctionBitOper(OperatorTypeEnum oper_type, short numOperands, Attributes **attr, Space *space)
      : ex_function_clause(oper_type, numOperands, attr, space) {
    setType(ex_clause::MATH_FUNCTION_TYPE);
  };

  ExFunctionBitOper(){};

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);
  // Display
  //
  virtual void displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

class ExFunctionMath : public ex_function_clause {
 public:
  ExFunctionMath(OperatorTypeEnum oper_type, short numOperands, short roundComptiableMode, Attributes **attr,
                 Space *space)
      : ex_function_clause(oper_type, numOperands, attr, space) {
    setType(ex_clause::MATH_FUNCTION_TYPE);
    roundComptiableMode_ = roundComptiableMode;
  };

  ExFunctionMath() : ex_function_clause(){};

  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea **diagsArea = 0);

  ex_expr::exp_return_type evalUnsupportedOperations(char *op_data[], CollHeap *heap, ComDiagsArea **diagsArea);

  // Display
  //
  virtual void displayContents(Space *space, const char *displayStr, int clauseNum, char *constsArea);

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(2, getClassVersionID());
    ex_function_clause::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------

 private:
  short roundComptiableMode_;  // 0 - OFF, 1 - ON
};

#endif
