/* -*-C++-*- */

#ifndef ELEMDDLUDFSPECIALATTRIBUTES_H
#define ELEMDDLUDFSPECIALATTRIBUTES_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdfSpecialAttributes.h
* Description:  class for UDF Special Attributes Text (parse node)
*               elements in DDL statements.
*
*
* Created:      2/2/2010
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
#include "parser/ElemDDLNode.h"
#include "common/ComSmallDefs.h"
#include "parser/SqlParserGlobals.h"

class ElemDDLUdfSpecialAttributes : public ElemDDLNode {
 public:
  // constructor
  ElemDDLUdfSpecialAttributes(const NAString &theSpecialAttributesText, CollHeap *h = PARSERHEAP());

  // virtual destructor
  virtual ~ElemDDLUdfSpecialAttributes(void);

  // cast
  virtual ElemDDLUdfSpecialAttributes *castToElemDDLUdfSpecialAttributes(void);

  // accessor
  inline const ComString &getSpecialAttributesText(void) const { return specialAttributesText_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComString specialAttributesText_;

};  // class ElemDDLUdfSpecialAttributes

#endif /* ELEMDDLUDFSPECIALATTRIBUTES_H */
