
/* -*-C++-*-
 ******************************************************************************
 *
 * File:         ItemComposite.C
 * Description:  Composite expressions
 * Created:
 * Language:     C++
 *
 ******************************************************************************
 */

#define SQLPARSERGLOBALS_NADEFAULTS
#include "parser/SqlParserGlobals.h"

#include "common/Platform.h"
#include "optimizer/Sqlcomp.h"
#include "optimizer/GroupAttr.h"
#include "optimizer/AllItemExpr.h"
#include "PartFunc.h"
#include "common/wstr.h"
#include "common/NLSConversion.h"
#include "Cost.h" /* for lookups in the defaults table */
#include "Stats.h"
#include "exp_function.h"  // for calling ExHDPHash::hash(data, len)
#include "ItemFuncUDF.h"
#include "arkcmp/CmpStatement.h"
#include "exp/exp_datetime.h"
#include "optimizer/Analyzer.h"
#include "ItemSample.h"
#include "common/ComUnits.h"

#include "OptRange.h"

#include <string.h>  // memcmp

// ----------------------------------------------------------------------
// class CompositeArrayLength
// ----------------------------------------------------------------------
NABoolean CompositeArrayLength::isCacheableExpr(CacheWA &cwa) { return ItemExpr::isCacheableExpr(cwa); }

ItemExpr *CompositeArrayLength::copyTopNode(ItemExpr *derivedNode, CollHeap *outHeap) {
  CompositeArrayLength *result = NULL;
  if (derivedNode == NULL)
    result = new (outHeap) CompositeArrayLength(child(0));
  else
    result = (CompositeArrayLength *)derivedNode;

  return ItemExpr::copyTopNode(result, outHeap);
}

const NAType *CompositeArrayLength::synthesizeType() {
  ValueId vid = child(0)->getValueId();
  vid.coerceType(NA_CHARACTER_TYPE);
  const NAType &operand = vid.getType();

  return new HEAP SQLInt(HEAP, FALSE,  // unsigned
                         operand.supportsSQLnullLogical());
}

ItemExpr *CompositeArrayLength::bindNode(BindWA *bindWA) {
  if (nodeIsBound()) return getValueId().getItemExpr();

  bindChildren(bindWA);
  if (bindWA->errStatus()) return this;

  const NAType *childType = &child(0)->getValueId().getType();
  if (NOT(childType->getFSDatatype() == REC_ARRAY)) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Operand of array_length must be of Array datatype.");
    bindWA->setErrStatus();
    return NULL;
  }

  ItemExpr *boundExpr = ItemExpr::bindNode(bindWA);
  if (bindWA->errStatus()) return this;

  return boundExpr;
}

const NAString CompositeArrayLength::getText() const {
  NAString text("array_length");

  return text;
}

// ----------------------------------------------------------------------
// class CompositeCreate
// ----------------------------------------------------------------------
ItemExpr *CompositeCreate::copyTopNode(ItemExpr *derivedNode, CollHeap *outHeap) {
  CompositeCreate *result = NULL;
  if (derivedNode == NULL)
    result = new (outHeap) CompositeCreate(child(0), type_);
  else
    result = (CompositeCreate *)derivedNode;

  result->elemVidList_ = elemVidList_;

  return ItemExpr::copyTopNode(result, outHeap);
}

const NAType *CompositeCreate::synthesizeType() {
  ItemExprList *iel = new HEAP ItemExprList(child(0), HEAP);

  int numEntries = iel->entries();
  CompositeType *type = NULL;
  ItemExpr *elem = NULL;
  if (type_ == ARRAY_TYPE) {
    elem = (*iel)[0];
    SQLArray *arrType = new HEAP SQLArray(HEAP, &elem->getValueId().getType(), numEntries);

    // make array elements to have the same nullable attribute as the
    // array datatype.
    if (elem->getOperatorType() != ITM_CONSTANT) {
      if (arrType->supportsSQLnull())
        ((NAType *)(arrType->getElementType()))->setNullable(TRUE);
      else
        ((NAType *)(arrType->getElementType()))->setNullable(FALSE);
    }
    type = arrType;
  } else if (type_ == ROW_TYPE) {
    NAArray<NAString> *fieldNames = NULL;
    NAArray<NAType *> *fieldTypes = new HEAP NAArray<NAType *>(HEAP);

    int totalSize = 0;
    for (int i = 0; i < numEntries; i++) {
      elem = (*iel)[i];
      const NAType *elemType = &elem->getValueId().getType();
      NAType *newElemType = elemType->newCopy(HEAP);
      fieldTypes->insertAt(i, newElemType);

      totalSize += elemType->getTotalSize();
    }

    SQLRow *rowType = new HEAP SQLRow(HEAP, NULL, fieldTypes);

    type = rowType;
  }

  if (NOT type->validate(CmpCommon::diags())) {
    return NULL;
  }

  return type;
}

ItemExpr *CompositeCreate::bindNode(BindWA *bindWA) {
  if (nodeIsBound()) return getValueId().getItemExpr();

  bindChildren(bindWA);
  if (bindWA->errStatus()) return this;

  if ((CmpCommon::getDefault(TRAF_COMPOSITE_DATATYPE_SUPPORT) == DF_OFF) &&
      (CmpCommon::getDefault(HIVE_COMPOSITE_DATATYPE_SUPPORT) == DF_OFF)) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Cannot specify composite array or row constructor.");
    bindWA->setErrStatus();
    return NULL;
  }

  ItemExprList *iel = new (bindWA->wHeap()) ItemExprList(child(0), bindWA->wHeap());

  ItemExprList *newIEL = new (bindWA->wHeap()) ItemExprList(iel->entries(), bindWA->wHeap());
  const NAType *elemType = NULL;
  if (type_ == ARRAY_TYPE) {
    const NAType *resultType = NULL;

    // -- validate that all array entries have compatible types.
    // -- find super type for all entries
    // -- convert each entry to the super type
    for (int i = 0; i < iel->entries(); i++) {
      ItemExpr *elem = (*iel)[i];

      NABoolean elemIsNullConst = FALSE;
      if ((elem->getOperatorType() == ITM_CONSTANT) && (((ConstValue *)elem)->isNull())) elemIsNullConst = TRUE;

      // type cast any params
      SQLVarChar c1(NULL, CmpCommon::getDefaultNumeric(VARCHAR_PARAM_DEFAULT_SIZE));
      elem->getValueId().coerceType(c1, NA_CHARACTER_TYPE);

      // make sure all array entries have compatible types
      elemType = &elem->getValueId().getType();
      if ((!resultType) && (NOT elemIsNullConst)) {
        resultType = elemType;
      } else if (elemIsNullConst) {
        // skip NULL constant
        continue;
      } else {
        const NAType &opR = *resultType;
        const NAType &opC = *elemType;
        resultType = resultType->synthesizeType(SYNTH_RULE_UNION, opR, opC, bindWA->wHeap());
        if (!resultType) {
          *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("ARRAY elements must have compatible types.");
          bindWA->setErrStatus();
          return NULL;
        }
      }
    }  // for

    // if no result, then it could be that all values were null constant.
    // Make a default result type.
    if (!resultType) {
      resultType = new (bindWA->wHeap()) SQLInt(NULL);
    }

    // make result nullable
    if (NOT resultType->supportsSQLnull()) {
      // convert elementType to resultType
      NAType *newType = resultType->newCopy(bindWA->wHeap());
      newType->setNullable(TRUE);
      resultType = newType;
    }

    // result type is the super type
    for (int i = 0; i < iel->entries(); i++) {
      ItemExpr *elem = (*iel)[i];

      NABoolean elemIsNullConst = FALSE;
      if ((elem->getOperatorType() == ITM_CONSTANT) && (((ConstValue *)elem)->isNull())) elemIsNullConst = TRUE;

      elemType = &elem->getValueId().getType();
      if ((elemIsNullConst) || (NOT(*elemType == *resultType))) {
        // convert elementType to resultType
        ItemExpr *ie = new (bindWA->wHeap()) Cast(elem, resultType);
        ie = ie->bindNode(bindWA);
        if (bindWA->errStatus()) return this;

        newIEL->addMember(ie);
      } else {
        newIEL->addMember(elem);
      }
    }   // for
  }     // ARRAY type
  else  // ROW type
  {
    for (int i = 0; i < iel->entries(); i++) {
      ItemExpr *elem = (*iel)[i];
      NABoolean elemIsNullConst = FALSE;
      if ((elem->getOperatorType() == ITM_CONSTANT) && (((ConstValue *)elem)->isNull())) elemIsNullConst = TRUE;

      // type cast any params
      SQLVarChar c1(NULL, CmpCommon::getDefaultNumeric(VARCHAR_PARAM_DEFAULT_SIZE));
      elem->getValueId().coerceType(c1, NA_CHARACTER_TYPE);

      elemType = &elem->getValueId().getType();
      if ((elemIsNullConst) || (NOT elemType->supportsSQLnull())) {
        NAType *newType = NULL;
        if (elemIsNullConst)
          newType = new (bindWA->wHeap()) SQLInt(NULL);
        else
          // convert elementType to resultType
          newType = elemType->newCopy(bindWA->wHeap());

        newType->setNullable(TRUE);

        ItemExpr *ie = new (bindWA->wHeap()) Cast(elem, newType);
        ie = ie->bindNode(bindWA);
        if (bindWA->errStatus()) return this;

        newIEL->addMember(ie);
      } else
        newIEL->addMember(elem);
    }
  }

  //  newIEL = iel;
  ItemExpr *newChild = newIEL->convertToItemExpr();
  setChild(0, newChild);

  bindWA->setHasCompositeExpr(TRUE);

  return ItemExpr::bindNode(bindWA);
}  // CompositeCreate::bindNode

// ----------------------------------------------------------------------
// class CompositeCast
// ----------------------------------------------------------------------
ItemExpr *CompositeCast::copyTopNode(ItemExpr *derivedNode, CollHeap *outHeap) {
  ItemExpr *result = NULL;
  if (derivedNode == NULL)
    result = new (outHeap) CompositeCast(child(0), type_, isExtendOrTruncate_);
  else
    result = derivedNode;

  return ItemExpr::copyTopNode(result, outHeap);
}

const NAType *CompositeCast::synthesizeType() { return type_; }

ItemExpr *CompositeCast::arrayExtendOrTruncate(BindWA *bindWA) {
  CompositeType &myCompType = (CompositeType &)(*type_);
  CompositeType &childCompType = (CompositeType &)child(0)->getValueId().getType();

  // if child array size is less then my array size, then
  // move nulls to "myarraysize - childarraysize" entries.
  // If child array size is greater than my size, then truncate.
  ItemExprList *ieNE = new (bindWA->wHeap()) ItemExprList(myCompType.getNumElements(), bindWA->wHeap());

  int i = 0;
  int childElems = MINOF(myCompType.getNumElements(), childCompType.getNumElements());
  int nullElems = (myCompType.getNumElements() > childCompType.getNumElements()
                         ? (myCompType.getNumElements() - childCompType.getNumElements())
                         : 0);
  ItemExpr *ie = NULL;
  for (i = 0; i < childElems; i++) {
    ie = new (bindWA->wHeap()) CompositeExtract(child(0), i + 1);
    ie = ie->bindNode(bindWA);
    if (bindWA->errStatus()) return this;

    ieNE->addMember(ie);
  }  // for

  for (i = 0; i < nullElems; i++) {
    ie = new (bindWA->wHeap()) ConstValue();
    ie = ie->bindNode(bindWA);
    if (bindWA->errStatus()) return this;

    ieNE->addMember(ie);
  }

  ItemExpr *cc = new (bindWA->wHeap()) CompositeCreate(ieNE->convertToItemExpr(), CompositeCreate::ARRAY_TYPE);
  cc = cc->bindNode(bindWA);
  if (bindWA->errStatus()) return this;

  setChild(0, cc);
  type_ = &child(0)->getValueId().getType();

  ItemExpr *boundExpr = ItemExpr::bindNode(bindWA);
  if (bindWA->errStatus()) return this;

  return boundExpr;
}  // array to be extended or truncated

ItemExpr *CompositeCast::bindNode(BindWA *bindWA) {
  if (nodeIsBound()) return getValueId().getItemExpr();

  // this will bind/type-propagate all children.
  bindChildren(bindWA);
  if (bindWA->errStatus()) return this;

  ItemExpr *child0 = child(0)->castToItemExpr();
  const NAType *childType = &child0->getValueId().getType();

  //
  // The NULL constant can be cast to any type.
  //
  NABoolean childIsNullConst = FALSE;
  if (child0->getOperatorType() == ITM_CONSTANT)
    if (((ConstValue *)child0)->isNull()) childIsNullConst = TRUE;

  if (childIsNullConst && (type_->isComposite())) {
    ItemExpr *boundExpr = ItemExpr::bindNode(bindWA);
    if (bindWA->errStatus()) return this;

    return boundExpr;
  }

  // if child is composite and target is char/varchar, return displayable
  // result.
  if (childType->isComposite() && type_->getTypeQualifier() == NA_CHARACTER_TYPE) {
    ItemExpr *ie = new (bindWA->wHeap()) CompositeDisplay(child0);
    setChild(0, ie);

    ie = ItemExpr::bindNode(bindWA);
    if (bindWA->errStatus()) return this;

    return ie;
  }

  if (NOT type_->isComposite()) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Target must be composite(ARRAY or ROW) datatype.");
    bindWA->setErrStatus();
    return NULL;
  }

  if (NOT childType->isComposite()) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Operand must be composite(ARRAY or ROW) datatype.");
    bindWA->setErrStatus();
    return NULL;
  }

  if (NOT(childType->isCompatible(*type_))) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Type of operand and target must be compatible.");
    bindWA->setErrStatus();
    return NULL;
  }

  CompositeType &myCompType = (CompositeType &)(*type_);
  CompositeType *childCompType = (CompositeType *)childType;

  // if child array size is less then my array size, then
  // move nulls to "myarraysize - childarraysize" entries.
  // If child array size is greater than my size, then truncate.
  if (isExtendOrTruncate_) {
    if ((myCompType.getFSDatatype() != REC_ARRAY) || (myCompType.getElementType(1) != NULL)) {
      *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Extend or Truncate can only be used on Array datatype.");
      bindWA->setErrStatus();
      return NULL;
    }

    return arrayExtendOrTruncate(bindWA);
  }

  if (type_->equalIgnoreCoercibility(*childType)) {
    ItemExpr *boundExpr = ItemExpr::bindNode(bindWA);
    if (bindWA->errStatus()) return this;

    return boundExpr;
  }

  ItemExprList *iel = new (bindWA->wHeap()) ItemExprList(myCompType.getNumElements(), bindWA->wHeap());

  for (int i = 0; i < childCompType->getNumElements(); i++) {
    ItemExpr *ce = new (bindWA->wHeap()) CompositeExtract(child(0), i + 1);
    ce = ce->bindNode(bindWA);
    if (bindWA->errStatus()) return this;

    const NAType &ceType = ce->getValueId().getType();

    ItemExpr *cast = ce;
    if (NOT(myCompType.getElementType(i + 1)->equalIgnoreCoercibility(ce->getValueId().getType()))) {
      cast = new (bindWA->wHeap()) Cast(ce, myCompType.getElementType(i + 1));  // getElementType is 1-based
    }
    cast = cast->bindNode(bindWA);
    if (bindWA->errStatus()) return this;

    iel->addMember(cast);
  }

  ItemExpr *cc = new (bindWA->wHeap()) CompositeCreate(
      iel->convertToItemExpr(),
      childCompType->getFSDatatype() == REC_ARRAY ? CompositeCreate::ARRAY_TYPE : CompositeCreate::ROW_TYPE);
  cc = cc->bindNode(bindWA);
  if (bindWA->errStatus()) return this;

  const NAType &ccType = cc->getValueId().getType();

  setChild(0, cc);

  ItemExpr *boundExpr = ItemExpr::bindNode(bindWA);
  if (bindWA->errStatus()) return this;

  return boundExpr;
}  // CompositeCast::bindNode

// get a printable string that identifies the operator
const NAString CompositeCast::getText() const {
  char buf[100];
  NAString text("composite_cast(");
  if (child(0)->getValueId().getType().getFSDatatype() == REC_ARRAY)
    text += "array";
  else if (child(0)->getValueId().getType().getFSDatatype() == REC_ROW)
    text += "row";
  if (type_->getFSDatatype() == REC_ARRAY)
    text += ", array";
  else if (type_->getFSDatatype() == REC_ROW)
    text += ", row";
  text += ")";

  return text;
}

// ----------------------------------------------------------------------
// class CompositeHiveCast
// ----------------------------------------------------------------------
ItemExpr *CompositeHiveCast::copyTopNode(ItemExpr *derivedNode, CollHeap *outHeap) {
  ItemExpr *result = NULL;
  if (derivedNode == NULL)
    result = new (outHeap) CompositeHiveCast(child(0), type_, fromHive_);
  else
    result = derivedNode;

  return ItemExpr::copyTopNode(result, outHeap);
}

// get a printable string that identifies the operator
const NAString CompositeHiveCast::getText() const {
  char buf[100];
  NAString text;
  if (fromHive_)
    text = "hive2traf_cast";
  else
    text = "traf2hive_cast";
  if (type_->getFSDatatype() == REC_ARRAY)
    text += "(array";
  else if (type_->getFSDatatype() == REC_ROW)
    text += "(row";
  text += ")";

  return text;
}

ItemExpr *CompositeHiveCast::bindNode(BindWA *bindWA) {
  if (nodeIsBound()) return getValueId().getItemExpr();

  ItemExpr *boundExpr = ItemExpr::bindNode(bindWA);
  if (bindWA->errStatus()) return this;

  return boundExpr;
}

// ----------------------------------------------------------------------
// class CompositeConcat
// ----------------------------------------------------------------------
NABoolean CompositeConcat::isCacheableExpr(CacheWA &cwa) { return ItemExpr::isCacheableExpr(cwa); }

ItemExpr *CompositeConcat::copyTopNode(ItemExpr *derivedNode, CollHeap *outHeap) {
  CompositeConcat *result = NULL;
  if (derivedNode == NULL)
    result = new (outHeap) CompositeConcat(child(0), child(1));
  else
    result = (CompositeConcat *)derivedNode;

  result->concatExpr_ = concatExpr_;

  return BuiltinFunction::copyTopNode(result, outHeap);
}

const NAType *CompositeConcat::synthesizeType() {
  ItemExpr *child1 = child(0)->castToItemExpr();
  const NAType &child1Type = child1->getValueId().getType();

  ItemExpr *child2 = child(1)->castToItemExpr();
  const NAType &child2Type = child2->getValueId().getType();

  const SQLArray &child1SA = (SQLArray &)child1Type;
  const SQLArray &child2SA = (SQLArray &)child2Type;

  SQLArray *sa = new HEAP SQLArray(HEAP, child1SA.getElementType(),
                                   (child1SA.getNumElements() + child2SA.getNumElements()), COM_SQLARK_EXPLODED_FORMAT);

  return sa;
}

ItemExpr *CompositeConcat::bindNode(BindWA *bindWA) {
  if (nodeIsBound()) return getValueId().getItemExpr();

  // this will bind/type-propagate all children.
  bindChildren(bindWA);
  if (bindWA->errStatus()) return this;

  ItemExpr *child1 = child(0)->castToItemExpr();
  const NAType &child1Type = child1->getValueId().getType();

  ItemExpr *child2 = child(1)->castToItemExpr();
  const NAType &child2Type = child2->getValueId().getType();

  if ((child1Type.getFSDatatype() != REC_ARRAY) || (child2Type.getFSDatatype() != REC_ARRAY)) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Only ARRAY operands can be concatenated.");
    bindWA->setErrStatus();
    return NULL;
  }

  const SQLArray &child1SA = (SQLArray &)child1Type;
  const SQLArray &child2SA = (SQLArray &)child2Type;

  const NAType *resultType = child1SA.getElementType()->synthesizeType(SYNTH_RULE_UNION, *child1SA.getElementType(),
                                                                       *child2SA.getElementType(), bindWA->wHeap());
  if (!resultType) {
    *CmpCommon::diags() << DgSqlCode(-3242)
                        << DgString0("ARRAY elements to be concatenated must have compatible types.");
    bindWA->setErrStatus();
    return NULL;
  }

  if (NOT(*child1SA.getElementType() == *resultType)) {
    // convert elementType to resultType
    SQLArray *sa = new (bindWA->wHeap()) SQLArray(bindWA->wHeap(), resultType, child1SA.getNumElements());
    ItemExpr *ie = new (bindWA->wHeap()) CompositeCast(child1, sa);
    ie = ie->bindNode(bindWA);
    if (bindWA->errStatus()) return this;

    setChild(0, ie);
  }

  if (NOT(*child2SA.getElementType() == *resultType)) {
    // convert elementType to resultType
    SQLArray *sa = new (bindWA->wHeap()) SQLArray(bindWA->wHeap(), resultType, child2SA.getNumElements());
    ItemExpr *ie = new (bindWA->wHeap()) CompositeCast(child2, sa);
    ie = ie->bindNode(bindWA);
    if (bindWA->errStatus()) return this;

    setChild(1, ie);
  }

  ItemExpr *boundExpr = ItemExpr::bindNode(bindWA);
  if (bindWA->errStatus()) return this;

  return boundExpr;
}

// ----------------------------------------------------------------------
// class CompositeDisplay
// ----------------------------------------------------------------------
ItemExpr *CompositeDisplay::copyTopNode(ItemExpr *derivedNode, CollHeap *outHeap) {
  CompositeDisplay *result = NULL;
  if (derivedNode == NULL)
    result = new (outHeap) CompositeDisplay(child(0), isInternal_);
  else
    result = (CompositeDisplay *)derivedNode;

  return ItemExpr::copyTopNode(result, outHeap);
}

const NAType *CompositeDisplay::synthesizeType() {
  const NAType &operand1 = child(0)->getValueId().getType();
  int displayLength = operand1.getDisplayLength();

  NAType *type = new HEAP SQLVarChar(HEAP, displayLength, operand1.supportsSQLnull());

  return type;
}

ItemExpr *CompositeDisplay::bindNode(BindWA *bindWA) {
  if (nodeIsBound()) return getValueId().getItemExpr();

  ItemExpr *boundExpr = ItemExpr::bindNode(bindWA);
  if (bindWA->errStatus()) return this;

  if (NOT child(0)->getValueId().getType().isComposite()) {
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Operand must be composite(ARRAY or ROW) datatype.");
    bindWA->setErrStatus();
    return NULL;
  }

  return boundExpr;
}  // CompositeDisplay::bindNode

// ----------------------------------------------------------------------
// class CompositeExtract
// ----------------------------------------------------------------------
NABoolean CompositeExtract::isCacheableExpr(CacheWA &cwa) {
  return FALSE;  // currently not cacheable
  // return ItemExpr::isCacheableExpr(cwa);
}

ItemExpr *CompositeExtract::copyTopNode(ItemExpr *derivedNode, CollHeap *outHeap) {
  CompositeExtract *result = NULL;
  if (derivedNode == NULL) {
    if (elemNum_ >= 0)
      result = new (outHeap) CompositeExtract(child(0), elemNum_);
    else
      result = new (outHeap) CompositeExtract(child(0), names_, indexes_);
  } else
    result = (CompositeExtract *)derivedNode;

  result->resultType_ = resultType_;
  result->attrIndexList_ = attrIndexList_;
  result->attrTypeList_ = attrTypeList_;

  return BuiltinFunction::copyTopNode(result, outHeap);
}

const NAType *CompositeExtract::synthesizeType() { return resultType_; }

ItemExpr *CompositeExtract::bindNode(BindWA *bindWA) {
  if (nodeIsBound()) return getValueId().getItemExpr();

  // this will bind/type-propagate all children.
  bindChildren(bindWA);
  if (bindWA->errStatus()) return this;

  ItemExpr *child0 = child(0)->castToItemExpr();
  ItemExpr *child1 = (getArity() == 2 ? child(1)->castToItemExpr() : NULL);
  NAString colName;
  if (child0->getOperatorType() == ITM_REFERENCE)
    colName = ((ColReference *)child0)->getColRefNameObj().getColName();
  else if (child0->getOperatorType() == ITM_BASECOLUMN)
    colName = ((BaseColumn *)child0)->getColName();

  NAString errStr;
  const NAType &child0Type = child0->getValueId().getType();
  if (NOT child0Type.isComposite()) {
    if (!colName.isNull()) {
      errStr = "Specified column " + colName + " must be composite(ARRAY or ROW) datatype.";
    } else {
      errStr = "Operand must be composite(ARRAY or ROW) datatype.";
    }
    *CmpCommon::diags() << DgSqlCode(-3242) << DgString0(errStr);
    bindWA->setErrStatus();
    return NULL;
  }

  const CompositeType &ct = (CompositeType &)child0Type;

  char buf[200];
  char buf2[200];
  resultType_ = NULL;
  if (elemNum_ > 0) {
    resultType_ = ct.getElementType(elemNum_);
  } else if (child1) {
    if (child1->getOperatorType() != ITM_NATYPE) {
      *CmpCommon::diags() << DgSqlCode(-3242)
                          << DgString0("Currently only NATypeToItem can be specified as elemNum expression.");
      bindWA->setErrStatus();
      return NULL;
    }

    const NumericType &child1Type = (NumericType &)child1->getValueId().getType();
    if (NOT((child1Type.getTypeQualifier() == NA_NUMERIC_TYPE) && (child1Type.isExact()) &&
            (child1Type.getScale() == 0) && (child1Type.getFSDatatype() == REC_BIN32_UNSIGNED))) {
      *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("ElemNum expression must be int with scale of zero.");
      bindWA->setErrStatus();
      return NULL;
    }

    if (ct.getFSDatatype() != REC_ARRAY) {
      *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Operand must be ARRAY datatype.");
      bindWA->setErrStatus();
      return NULL;
    }

    resultType_ = ct.getElementType(1);
  } else {
    if (ct.getFSDatatype() == REC_ARRAY) {
      if ((indexes_[0] == 0) && (indexes_.entries() > 1)) {
        if (colName.isNull())
          errStr = "Array index must be specified for ARRAY datatype.";
        else
          errStr = NAString("Array index must be specified for column ") + colName + ".";
        *CmpCommon::diags() << DgSqlCode(-3242) << DgString0(errStr);
        bindWA->setErrStatus();
        return NULL;
      }

      if (indexes_[0] > 0) {
        if (indexes_[0] > ((SQLArray &)ct).getArraySize()) {
          *CmpCommon::diags() << DgSqlCode(-3242)
                              << DgString0(NAString("Specified array index(") + str_itoa(indexes_[0], buf) +
                                           ") is greater than the max array size(" +
                                           str_itoa(((SQLArray &)ct).getArraySize(), buf2) + ").");
          bindWA->setErrStatus();
          return NULL;
        }
      }

      resultType_ = ct.getElementType(1);
    } else  // ROW
    {
      resultType_ = &ct;

      if ((indexes_.entries() > 0) && (indexes_[0] > 0)) {
        *CmpCommon::diags() << DgSqlCode(-3242)
                            << DgString0(NAString("Cannot specify array index for non-array field ") + names_[0] + ".");
        bindWA->setErrStatus();
        return NULL;
      }
    }  // row

    attrIndexList_.insert(indexes_.entries() > 0 ? indexes_[0] : 0);
    attrTypeList_.insert(resultType_->getFSDatatype());

    NAString currName = names_[0];
    for (int i = 1; i < names_.entries(); i++) {
      if (resultType_->getFSDatatype() != REC_ROW) {
        *CmpCommon::diags() << DgSqlCode(-3242) << DgString0("Specified field " + currName + " must be of ROW type.");
        bindWA->setErrStatus();
        return NULL;
      }

      currName = names_[i];

      SQLRow *sr = (SQLRow *)resultType_;
      const NAType *elemType = NULL;
      int elemNum = 0;
      NABoolean found = sr->getElementInfo(currName, elemType, elemNum);
      if (NOT found) {
        *CmpCommon::diags() << DgSqlCode(-3242)
                            << DgString0(NAString("Field ") + currName + " not found in column " + names_[i - 1] + ".");
        bindWA->setErrStatus();
        return NULL;
      }

      attrIndexList_.insert(elemNum);
      attrTypeList_.insert(elemType->getFSDatatype());

      if (elemType->getFSDatatype() == REC_ARRAY) {
        SQLArray *sa = (SQLArray *)elemType;

        if ((indexes_[i] == 0) && (indexes_.entries() > (i + 1))) {
          *CmpCommon::diags() << DgSqlCode(-3242)
                              << DgString0(NAString("Array index must be specified for an ARRAY datatype."));
          bindWA->setErrStatus();
          return NULL;
        }

        if ((indexes_.entries() > 0) && (indexes_[i] > 0)) {
          if (indexes_[i] > sa->getArraySize()) {
            *CmpCommon::diags() << DgSqlCode(-3242)
                                << DgString0(NAString("Specified array index ") + str_itoa(indexes_[i], buf) +
                                             " is greater than the array size " + str_itoa(sa->getArraySize(), buf2) +
                                             ".");
            bindWA->setErrStatus();
            return NULL;
          }

          resultType_ = sa->getElementType();
          attrIndexList_.insert(indexes_[i]);
          attrTypeList_.insert(resultType_->getFSDatatype());
        } else {
          resultType_ = elemType;
        }
      } else  // ROW
      {
        if ((indexes_.entries() > 0) && (indexes_[i] > 0)) {
          *CmpCommon::diags() << DgSqlCode(-3242)
                              << DgString0(NAString("Cannot specify array index for non-array field ") + names_[i] +
                                           ".");
          bindWA->setErrStatus();
          return NULL;
        }
        resultType_ = elemType;
      }
    }  // for
  }

  ItemExpr *boundExpr = BuiltinFunction::bindNode(bindWA);
  if (bindWA->errStatus()) return this;

  return boundExpr;
}  // CompositeExtract::bindNode

// get a printable string that identifies the operator
const NAString CompositeExtract::getText() const {
  if (elemNum_ <= 0) return "composite_extract";

  char buf[100];
  NAString text("composite_extract(");
  text += str_itoa(elemNum_, buf);
  if (child(0)->getValueId().getType().getFSDatatype() == REC_ARRAY)
    text += ", array";
  else if (child(0)->getValueId().getType().getFSDatatype() == REC_ROW)
    text += ", row";
  if (getValueId().getType().getFSDatatype() == REC_ARRAY)
    text += ", array";
  else if (getValueId().getType().getFSDatatype() == REC_ROW)
    text += ", row";
  text += ")";

  return text;
}
