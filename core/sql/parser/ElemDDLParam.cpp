/* -*-C++-*- */

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLParam.C
 * Description:  methods for classes relating to columns.
 *
 *
 * Created:      10/01/1999
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComASSERT.h"
#include "export/ComDiags.h"
#include "parser/ElemDDLParamDef.h"
#include "parser/ElemDDLParamName.h"

// -----------------------------------------------------------------------
// methods for class ElemDDLParamDef
// -----------------------------------------------------------------------

// constructor
ElemDDLParamDef::ElemDDLParamDef(NAType *pParamDataType, ElemDDLParamName *paramName, ComParamDirection paramDirection,
                                 CollHeap *heap)
    : ElemDDLNode(ELM_PARAM_DEF_ELEM),
      paramName_((paramName == NULL ? "" : paramName->getParamName()), heap),
      paramDataType_(pParamDataType),
      paramDirection_(paramDirection) {
  ComASSERT(pParamDataType NEQ NULL);
}  // ElemDDLParamDef()

// virtual destructor
ElemDDLParamDef::~ElemDDLParamDef() {
  // delete data structures owned by the node
  delete paramDataType_;
}

// cast
ElemDDLParamDef *ElemDDLParamDef::castToElemDDLParamDef() { return this; }

//
// accessors
//

// get the degree of this node
int ElemDDLParamDef::getArity() const { return MAX_ELEM_DDL_PARAM_DEF_ARITY; }

ExprNode *ElemDDLParamDef::getChild(int index) {
  ComASSERT(FALSE);  // No child node exists -- should not call this.
  return this;       // Just to keep the compiler happy.
}

//
// mutators
//

//
// methods for tracing
//

const NAString ElemDDLParamDef::displayLabel1() const { return NAString("Param name: ") + getParamName(); }

const NAString ElemDDLParamDef::displayLabel2() const {
  return NAString("Data type:   ") + getParamDataType()->getSimpleTypeName();
}

NATraceList ElemDDLParamDef::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  detailTextList.append(displayLabel1());  // column name
  detailTextList.append(displayLabel2());  // column data type

  return detailTextList;

}  // ElemDDLParamDef::getDetailInfo()

const NAString ElemDDLParamDef::getText() const { return "ElemDDLParamDef"; }

// -----------------------------------------------------------------------
// methods for class ElemDDLParamName
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLParamName::~ElemDDLParamName() {}

// cast virtual function
ElemDDLParamName *ElemDDLParamName::castToElemDDLParamName() { return this; }

// methods for tracing

const NAString ElemDDLParamName::getText() const { return "ElemDDLParamName"; }

const NAString ElemDDLParamName::displayLabel1() const { return NAString("Param name: ") + getParamName(); }

//
// End of File
//
