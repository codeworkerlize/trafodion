#ifndef EXP_SEQUENCE_FUNCTION_H
#define EXP_SEQUENCE_FUNCTION_H
//
// Only include this file once

/* -*-C++-*-
******************************************************************************
*
* File:         ExSequenceFunction.h
* RCS:          $Id
* Description:  ExSequenceFunction class Declaration
* Created:      9/11/98
* Modified:     $Author
* Language:     C++
* Status:       $State
*
*

*
*
******************************************************************************
*/

// Includes
//
#include "exp/exp_clause.h"
#include "exp/exp_clause_derived.h"

// Internal forward declarations
//
class ExpSequenceFunction;
class ExpSequenceExpression;

// ExpSequenceFunction
//

// ExpSequenceFunction declaration
//
class ExpSequenceFunction : public ex_function_clause {
 public:
  enum { SF_NONE, SF_OFFSET };

  // Construction - this is the "real" constructor
  //
  ExpSequenceFunction(OperatorTypeEnum oper_type, int arity, int index, Attributes **attr, Space *space);

  // This constructor is used only to get at the virtual function table.
  //
  ExpSequenceFunction();

  // isNullInNullOut - Must redefine this virtual function to return 0
  // since a NULL input does not simply produce a NULL output.
  //
  int isNullInNullOut() const { return 0; };

  // isNullRelevant - Must redefine this virtual function to return 0
  // since all the work is done in eval and none in processNulls.
  //
  int isNullRelevant() const { return 0; };

  ex_expr::exp_return_type pCodeGenerate(Space *space, UInt32 f);

  // eval - Must redefine eval to do the effective NOP sequence function.
  //
  ex_expr::exp_return_type eval(char *op_data[], CollHeap *, ComDiagsArea ** = 0);

  inline NABoolean nullRowIsZero(void) { return ((flags_ & SF_NULL_ROW_IS_ZERO) != 0); };

  inline void setNullRowIsZero(NABoolean v) { (v) ? flags_ |= SF_NULL_ROW_IS_ZERO : flags_ &= ~SF_NULL_ROW_IS_ZERO; };

  inline NABoolean isOLAP(void) { return ((flags_ & SF_IS_OLAP) != 0); };

  inline void setIsOLAP(NABoolean v) { (v) ? flags_ |= SF_IS_OLAP : flags_ &= ~SF_IS_OLAP; };

  inline NABoolean isLeading(void) { return ((flags_ & SF_IS_LEADING) != 0); };

  inline void setIsLeading(NABoolean v) { (v) ? flags_ |= SF_IS_LEADING : flags_ &= ~SF_IS_LEADING; };

  inline NABoolean allowNegativeOffset(void) { return ((flags_ & SF_ALLOW_NEGATIVE_OFFSET) != 0); };

  inline void setAllowNegativeOffset(NABoolean v) {
    (v) ? flags_ |= SF_ALLOW_NEGATIVE_OFFSET : flags_ &= ~SF_ALLOW_NEGATIVE_OFFSET;
  };

  inline int winSize(void) { return winSize_; };

  inline void setWinSize(int winSize) { winSize_ = winSize; };

  // pack - Must redefine pack.
  Long pack(void *);

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

  enum {
    SF_NULL_ROW_IS_ZERO = 0x0001,
    SF_IS_OLAP = 0x0002,
    SF_IS_LEADING = 0x0004,
    SF_ALLOW_NEGATIVE_OFFSET = 0x0008  // 1: allow, 0: disallow
  };

  void *GetRowData_;
  char *(*GetRow_)(void *, int, NABoolean, int, int &);

  char *(*PutRow_)(void *, int, NABoolean, int, int &);

  int offsetIndex_;  // 00-03
  int flags_;        // 04-07
  int winSize_;      // 08-11
  char filler_[4];   // 12-15
};

// ExpSequenceExpression
//
class ExpSequenceExpression : public ex_expr {
 public:
  inline void seqFixup(void *data, char *(*getRow)(void *, int, NABoolean, int, int &),
                       char *(*getRowOLAP)(void *, int, NABoolean, int, int &)) {
    ex_clause *clause = getClauses();
    while (clause) {
      if (clause->getOperType() == ITM_OFFSET)
      // || clause->getOperType() == ITM_OLAP_LEAD)
      {
        ExpSequenceFunction *seqClause = (ExpSequenceFunction *)clause;
        seqClause->GetRowData_ = data;
        seqClause->GetRow_ = (seqClause->isOLAP() ? getRowOLAP : getRow);
      }
      clause = clause->getNextClause();
    }

    PCodeBinary *pCode = getPCodeBinary();
    if (!pCode) return;

    int length = *(pCode++);
    pCode += (2 * length);

    while (pCode[0] != PCIT::END) {
      if (pCode[0] == PCIT::OFFSET_IPTR_IPTR_IBIN32S_MBIN64S_MBIN64S ||
          pCode[0] == PCIT::OFFSET_IPTR_IPTR_MBIN32S_MBIN64S_MBIN64S) {
        int idx = 1;
        if (GetPCodeBinaryAsPtr(pCode, idx)) {  // if (pCode[1])
          idx += setPtrAsPCodeBinary(pCode, idx, (Long)getRowOLAP);
          // pCode[1] = (PCodeBinary) getRowOLAP;
        } else {
          idx += setPtrAsPCodeBinary(pCode, idx, (Long)getRow);
          // pCode[1] = (PCodeBinary) getRow;
        }
        idx = setPtrAsPCodeBinary(pCode, idx, (Long)data);
        // pCode[2] = (PCodeBinary) data;
      }

      pCode += PCode::getInstructionLength(pCode);
    }
  }

  // ---------------------------------------------------------------------
  // Redefinition of methods inherited from NAVersionedObject.
  // ---------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ex_expr::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }
  // ---------------------------------------------------------------------
};

#endif
