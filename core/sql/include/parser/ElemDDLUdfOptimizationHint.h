/* -*-C++-*- */
#ifndef ELEMDDLUDFOPTIMIZATIONHINT_H
#define ELEMDDLUDFOPTIMIZATIONHINT_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdfOptimizationHint.h
* Description:  class for function optimization hint (parser node)
*               elements in DDL statements
*
*
* Created:      1/28/2010
* Language:     C++
*
*

*
*
******************************************************************************
*/

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif
#include "parser/SqlParserGlobals.h"

#include "common/ComSmallDefs.h"
#include "ElemDDLNode.h"
#include "common/Collections.h"

class ElemDDLUdfOptimizationHint : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdfOptimizationHint(ComUdfOptimizationHintKind optimizationKind, CollHeap *h = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLUdfOptimizationHint();

  // cast
  virtual ElemDDLUdfOptimizationHint *castToElemDDLUdfOptimizationHint(void);

  //
  // accessors
  //

  inline ComUdfOptimizationHintKind getOptimizationKind(void) const { return udfOptimizationKind_; }

  inline ComSInt32 getCost(void) const { return cost_; }

  inline const NAList<ComSInt64> &getUniqueOutputValues(void) const { return uniqueOutputValues_; }

  //
  // mutators
  //

  inline void setOptimizationKind(ComUdfOptimizationHintKind newValue) { udfOptimizationKind_ = newValue; }

  inline void setCost(ComSInt32 newValue) { cost_ = newValue; }

  inline void setUniqueOutputValuesParseTree(ItemExprList *pList) { uniqueOutputValuesParseTree_ = pList; }

  inline NAList<ComSInt64> &getUniqueOutputValues(void) { return uniqueOutputValues_; }

  //
  // helpers
  //

  void synthesize(void);

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComUdfOptimizationHintKind udfOptimizationKind_;
  ComSInt32 cost_;
  ItemExprList *uniqueOutputValuesParseTree_;
  NAList<ComSInt64> uniqueOutputValues_;

};  // class ElemDDLUdfOptimizationHint

#endif /* ELEMDDLUDFOPTIMIZATIONHINT_H */
