/* -*-C++-*- */
#ifndef ELEMDDLUUDFPARAMDEF_H
#define ELEMDDLUUDFPARAMDEF_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUudfParamDef.h
* Description:  class for universal function parameter kind (parse node)
*               elements in DDL statements
*
*
* Created:      1/20/2010
* Language:     C++
*
*

*
*
******************************************************************************
*/

#include "parser/ElemDDLNode.h"

class ElemDDLUudfParamDef : public ElemDDLNode {
 public:
  // constructor
  ElemDDLUudfParamDef(ComUudfParamKind uudfParamKind);

  // virtual destructor
  virtual ~ElemDDLUudfParamDef(void);

  // cast
  virtual ElemDDLUudfParamDef *castToElemDDLUudfParamDef(void);

  // accessor
  inline const ComUudfParamKind getUudfParamKind(void) const { return uudfParamKind_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComUudfParamKind uudfParamKind_;

};  // class ElemDDLUudfParamDef

#endif /* ELEMDDLUUDFPARAMDEF_H */
