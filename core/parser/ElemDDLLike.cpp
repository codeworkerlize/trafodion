/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ElemDDLLike.C
 * Description:  methods for class ElemDDLLike and any classes
 *               derived from class ElemDDLLike.
 *
 *               Please note that classes with prefix ElemDDLLikeOpt
 *               are not derived from class ElemDDLLike.  Therefore
 *               they are defined in files ElemDDLLikeOpt.C and
 *               ElemDDLLikeOptions.h instead.
 *
 * Created:      6/5/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComASSERT.h"
#include "common/ComOperators.h"
#include "parser/AllElemDDLLike.h"

// -----------------------------------------------------------------------
// methods for class ElemDDLLike
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLike::~ElemDDLLike() {
  // delete all children
  for (int i = 0; i < MAX_ELEM_DDL_LIKE_ARITY; i++) {
    delete getChild(i);
  }
}

// cast
ElemDDLLike *ElemDDLLike::castToElemDDLLike() { return this; }

// accessors

// get the degree of this node
int ElemDDLLike::getArity() const { return MAX_ELEM_DDL_LIKE_ARITY; }

ExprNode *ElemDDLLike::getChild(int index) {
  ComASSERT(index >= 0 AND index < MAX_ELEM_DDL_LIKE_ARITY);
  return pLikeOptions_;
}

// mutators

void ElemDDLLike::setChild(int index, ExprNode *pChildNode) {
  ComASSERT(index EQU INDEX_LIKE_OPT_LIST);
  if (pChildNode NEQ NULL) {
    ComASSERT(pChildNode->castToElemDDLNode() NEQ NULL);
    pLikeOptions_ = pChildNode->castToElemDDLNode();
  } else
    pLikeOptions_ = NULL;
}

// methods for tracing

const NAString ElemDDLLike::getText() const { return "ElemDDLLike"; }

const NAString ElemDDLLike::displayLabel1() const {
  if (getSourceTableName().length() NEQ 0)
    return NAString("Source table name: ") + getSourceTableName();
  else
    return NAString();
}

// -----------------------------------------------------------------------
// methods for class ElemDDLLikeCreateTable
// -----------------------------------------------------------------------

// constructor
ElemDDLLikeCreateTable::ElemDDLLikeCreateTable(const CorrName &sourceTableName, ElemDDLNode *pLikeOptions,
                                               NABoolean forExtTable, CollHeap *h)
    : ElemDDLLike(ELM_LIKE_CREATE_TABLE_ELEM, sourceTableName, pLikeOptions, h), forExtTable_(forExtTable) {
  if (pLikeOptions NEQ NULL) {
    for (CollIndex index = 0; index < pLikeOptions->entries(); index++) {
      if ((*pLikeOptions)[index] != NULL) likeOptions_.setLikeOption((*pLikeOptions)[index]->castToElemDDLLikeOpt());
    }
  }
}

// virtual destructor
ElemDDLLikeCreateTable::~ElemDDLLikeCreateTable() {}

// cast
ElemDDLLikeCreateTable *ElemDDLLikeCreateTable::castToElemDDLLikeCreateTable() { return this; }

// methods for tracing

const NAString ElemDDLLikeCreateTable::getText() const { return "ElemDDLLikeCreateTable"; }

//
// End of File
//
