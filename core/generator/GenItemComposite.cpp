
/* -*-C++-*-
******************************************************************************
*
* File:         GenItemComposite.cpp
* Description:  Composite expressions
*
*
* Created:
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ExpComposite.h"
#include "GenExpGenerator.h"
#include "ItemFuncUDF.h"
#include "common/CharType.h"
#include "common/CompositeType.h"
#include "common/NumericType.h"
#include "exp/exp_datetime.h"
#include "exp/exp_function.h"
#include "exp_math_func.h"
#include "generator/Generator.h"
#include "optimizer/RelMisc.h"

// create nested list of expression attributes corresponding to type.
// This list will be used during runtime to operate on the specified
// composite operand value.
static AttributesPtr createCompositeAttributes(Generator *generator, const NAType *type) {
  if (NOT type->isComposite()) return NULL;

  CompositeType *compType = (CompositeType *)type;
  MapTable *myMapTable = generator->appendAtEnd();

  Space *space = generator->getSpace();

  Attributes *attr = generator->getExpGenerator()->convertNATypeToAttributes(*type, generator->wHeap());
  CompositeAttributes *retAttr = (CompositeAttributes *)attr->newCopy(space);
  int numElements = retAttr->getNumElements();

  AttributesPtrPtr attrs = (AttributesPtr *)(space->allocateAlignedSpace(numElements * sizeof(AttributesPtr)));
  retAttr->setElements(attrs);

  for (int i = 0; i < numElements; i++) {
    const NAType *elementType = NULL;
    if (type->getFSDatatype() == REC_ARRAY) {
      const SQLArray *sa = (SQLArray *)type;
      elementType = (NAType *)sa->getElementType();
    } else if (type->getFSDatatype() == REC_ROW) {
      SQLRow *sa = (SQLRow *)type;
      elementType = sa->fieldTypes()[i];
    } else
      elementType = type;

    if (NOT elementType->isComposite()) {
      Attributes *elemAttr = generator->getExpGenerator()->convertNATypeToAttributes(*elementType, generator->wHeap());

      attrs[i] = elemAttr->newCopy(space);
    } else {
      attrs[i] = createCompositeAttributes(generator, elementType);
    }
  }  // for

  ExpTupleDesc::TupleDataFormat tdf = (ExpTupleDesc::TupleDataFormat)compType->getCompFormat();

  int tupleLength2 = 0;
  AttributesPtr *b = attrs;
  Attributes **bb = (Attributes **)b;
  generator->getExpGenerator()->processAttributes(numElements, bb, tdf, tupleLength2, 0, 1);

  generator->removeLast();

  return retAttr;
}

short CompositeArrayLength::codeGen(Generator *generator) {
  Attributes **attr;

  if (generator->getExpGenerator()->genItemExpr(this, &attr, (1 + getArity()), -1) == 1) return 0;

  ExpCompositeArrayLength *arrayLengthClause = new (generator->getSpace())
      ExpCompositeArrayLength(ITM_COMPOSITE_ARRAY_LENGTH, CompositeCreate::ARRAY_TYPE, attr, generator->getSpace());
  generator->getExpGenerator()->linkClause(this, arrayLengthClause);

  return 0;
}

ItemExpr *CompositeCreate::preCodeGen(Generator *generator) {
  if (nodeIsPreCodeGenned()) return this;

  if (!ItemExpr::preCodeGen(generator)) return NULL;

  ItemExprList *iel = new (generator->wHeap()) ItemExprList(child(0), generator->wHeap());

  for (int i = 0; i < iel->entries(); i++) {
    ItemExpr *elem = (*iel)[i];

    elementsVIDlist().insert(elem->getValueId());
  }

  //  generator->setHasCompositeExpr(TRUE);

  return this;
}

// CompositeCreate creates a composite row that is returned by this
// node. It evaluates an ex_expr expression at runtime to do that.
short CompositeCreate::codeGen(Generator *generator) {
  Attributes **attr;

  Space *space = generator->getSpace();

  MapInfo *map_info = generator->getMapInfoAsIs(getValueId());
  if ((map_info) && (map_info->isCodeGenerated())) map_info->resetCodeGenerated();

  if (generator->getExpGenerator()->genItemExpr(this, &attr, 1, -1) == 1) return 0;

  int compRowLen = 0;
  ex_expr *compExpr = NULL;

  int numEntries = elementsVIDlist().entries();

  // generate an expression in a new context. This expressions is a standalone
  // expr with all the needed info contained in it.
  // A new Generator/ExpGenerator is created to generate this expr.
  char *compExprStr = NULL;
  ex_cri_desc *compCriDesc = NULL;
  BindWA bindWA(ActiveSchemaDB(), CmpCommon::context());
  Generator myGenerator(CmpCommon::context(), NULL);
  myGenerator.setMapTable(generator->getMapTable());
  ExpGenerator expGen(&myGenerator);
  myGenerator.setExpGenerator(&expGen);

  myGenerator.setHasCompositeExpr(generator->hasCompositeExpr());

  myGenerator.setBindWA(&bindWA);
  FragmentDir *compFragDir = myGenerator.getFragmentDir();
  CollIndex myFragmentId = compFragDir->pushFragment(FragmentDir::MASTER);
  Space *mySpace = myGenerator.getSpace();

  expGen.setPCodeMode(ex_expr::PCODE_NONE);

  ExpTupleDesc *compTupleDesc = NULL;

  expGen.setForceLinkConstant(TRUE);

  if (generator->getExpGenerator()->getShowplan()) expGen.setShowplan(1);

  ExpTupleDesc::TupleDataFormat tdf;
  const CompositeType &ct = (CompositeType &)getValueId().getType();
  tdf = (ExpTupleDesc::TupleDataFormat)ct.getCompFormat();
  expGen.generateContiguousMoveExpr(elementsVIDlist(),
                                    TRUE,  // add convert modes
                                    2,     // expr row will be in atp 2
                                    2,     // at atpindex 2
                                    tdf, compRowLen, &compExpr);
  ExExprPtr(compExpr).pack(mySpace);
  int compExprLen = mySpace->getAllocatedSpaceSize();
  compExprStr = new (generator->getSpace()) char[compExprLen];
  mySpace->makeContiguous(compExprStr, compExprLen);
  myGenerator.removeLast();

  compCriDesc = new (generator->getSpace()) ex_cri_desc(3, generator->getSpace());

  AttributesPtr compAttrs = createCompositeAttributes(generator, &getValueId().getType());

  // int containing number of entries in the created row.
  // This field precedes the actual contents.
  if (type_ == ARRAY_TYPE) compRowLen += sizeof(int);

  ex_clause *comp_clause = new (generator->getSpace())
      ExpCompositeCreate(getOperatorType(), type_, numEntries, 1, attr, compRowLen, (ex_expr *)compExprStr, compCriDesc,
                         compAttrs, generator->getSpace());
  generator->getExpGenerator()->linkClause(this, comp_clause);

  return 0;
}

short CompositeConcat::codeGen(Generator *generator) {
  Attributes **attr;

  Space *space = generator->getSpace();

  if (generator->getExpGenerator()->genItemExpr(this, &attr, (1 + getArity()), -1) == 1) return 0;

  ItemExpr *child1 = child(0)->castToItemExpr();
  ItemExpr *child2 = child(1)->castToItemExpr();

  AttributesPtr compAttrs = createCompositeAttributes(generator, &getValueId().getType());
  AttributesPtr compAttrsChild1 = createCompositeAttributes(generator, &child1->getValueId().getType());
  AttributesPtr compAttrsChild2 = createCompositeAttributes(generator, &child2->getValueId().getType());

  ExpCompositeConcat *concatClause = new (space) ExpCompositeConcat(
      getOperatorType(), CompositeCreate::ARRAY_TYPE, attr, compAttrs, compAttrsChild1, compAttrsChild2, space);

  generator->getExpGenerator()->linkClause(this, concatClause);

  return 0;
}

short CompositeDisplay::codeGen(Generator *generator) {
  Attributes **attr;
  Space *space = generator->getSpace();

  if (generator->getExpGenerator()->genItemExpr(this, &attr, (1 + getArity()), -1) == 1) return 0;

  AttributesPtr compAttrs = createCompositeAttributes(generator, &child(0)->getValueId().getType());

  short type = 0;
  if (child(0)->getValueId().getType().getFSDatatype() == REC_ARRAY)
    type = CompositeCreate::ARRAY_TYPE;
  else
    type = CompositeCreate::ROW_TYPE;
  ex_clause *disp_clause = new (generator->getSpace())
      ExpCompositeDisplay(getOperatorType(), type, 1, (1 + getArity()), attr, NULL, compAttrs, generator->getSpace());
  generator->getExpGenerator()->linkClause(this, disp_clause);

  return 0;
}

short CompositeCast::codeGen(Generator *generator) {
  Attributes **attr;

  if (generator->getExpGenerator()->genItemExpr(this, &attr, (1 + getArity()), -1) == 1) return 0;

  if ((attr[0]->getDatatype() == REC_ARRAY) && (attr[1]->getDatatype() == REC_ARRAY)) {
    ItemExpr *child1 = child(0)->castToItemExpr();

    const NAType &childType = child1->getValueId().getType();
    const NAType &myType = getValueId().getType();
    const CompositeType &childCompType = (CompositeType &)childType;
    const CompositeType &myCompType = (CompositeType &)myType;

    AttributesPtr tgtAttrs = createCompositeAttributes(generator, &getValueId().getType());
    AttributesPtr srcAttrs = createCompositeAttributes(generator, &child1->getValueId().getType());
    ExpCompositeArrayCast *castClause = new (generator->getSpace()) ExpCompositeArrayCast(
        ITM_COMPOSITE_ARRAY_CAST, CompositeCreate::ARRAY_TYPE, attr, tgtAttrs, srcAttrs, generator->getSpace());
    generator->getExpGenerator()->linkClause(this, castClause);
  } else {
    ex_conv_clause *conv_clause =
        new (generator->getSpace()) ex_conv_clause(getOperatorType(), attr, generator->getSpace(), 1 + getArity());
    generator->getExpGenerator()->linkClause(this, conv_clause);
  }

  return 0;
}

short CompositeHiveCast::codeGen(Generator *generator) {
  Attributes **attr;

  if (generator->getExpGenerator()->genItemExpr(this, &attr, (1 + getArity()), -1) == 1) return 0;

  ItemExpr *child1 = child(0)->castToItemExpr();
  AttributesPtr srcAttrs = createCompositeAttributes(generator, &getValueId().getType());
  ExpCompositeHiveCast *castClause = new (generator->getSpace()) ExpCompositeHiveCast(
      ITM_COMPOSITE_HIVE_CAST, fromHive_, CompositeCreate::ARRAY_TYPE, attr, srcAttrs, srcAttrs, generator->getSpace());
  generator->getExpGenerator()->linkClause(this, castClause);

  return 0;
}

short CompositeExtract::codeGen(Generator *generator) {
  Attributes **attr;

  Space *space = generator->getSpace();

  const NAType &ctt = getValueId().getType();

  if (generator->getExpGenerator()->genItemExpr(this, &attr, (1 + getArity()), -1) == 1) return 0;

  AttributesPtr compAttrs = createCompositeAttributes(generator, &child(0)->getValueId().getType());

  short type = 0;
  if (child(0)->getValueId().getType().getFSDatatype() == REC_ARRAY)
    type = CompositeCreate::ARRAY_TYPE;
  else
    type = CompositeCreate::ROW_TYPE;

  int *searchAttrTypeList = (int *)(space->allocateAlignedSpace(attrTypeList_.entries() * sizeof(int)));
  int *searchAttrIndexList = (int *)(space->allocateAlignedSpace(attrIndexList_.entries() * sizeof(int)));

  for (int i = 0; i < attrTypeList_.entries(); i++) {
    searchAttrTypeList[i] = attrTypeList_[i];
    searchAttrIndexList[i] = attrIndexList_[i];
  }

  const CompositeType &ct = (CompositeType &)child(0)->getValueId().getType();
  ex_clause *extract_clause = new (generator->getSpace()) ExpCompositeExtract(
      getOperatorType(), type, ct.getNumElements(), elemNum_, (1 + getArity()), attr, compAttrs,
      attrTypeList_.entries(), (char *)searchAttrTypeList, (char *)searchAttrIndexList, generator->getSpace());
  generator->getExpGenerator()->linkClause(this, extract_clause);

  return 0;
}
