/* -*-C++-*-

 *****************************************************************************
 *
 * File:         StmtDDLNode.C
 * Description:  member functions for classes StmtDDLNode, StmtDDLGrant,
 *               StmtDDLGrantArray, StmtDDLRevoke,
 *		 StmtDDLSchRevoke
 *
 * Created:      3/9/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/StmtDDLNode.h"

#include "common/BaseTypes.h"
#include "common/ComOperators.h"
#include "parser/ElemDDLGrantee.h"
#include "parser/ElemDDLPrivileges.h"
#include "parser/StmtDDLGrant.h"
#include "parser/StmtDDLGrantArray.h"
#include "parser/StmtDDLGrantComponentPrivilege.h"
#include "parser/StmtDDLRegisterComponent.h"
#include "parser/StmtDDLRevoke.h"
#include "parser/StmtDDLRevokeComponentPrivilege.h"
#include "parser/StmtDDLSchGrant.h"
#include "parser/StmtDDLSchGrantArray.h"
#include "parser/StmtDDLSchRevoke.h"
// -----------------------------------------------------------------------
// member functions for class StmtDDLNode
// -----------------------------------------------------------------------

StmtDDLNode::StmtDDLNode(OperatorTypeEnum otype)
    : ElemDDLNode(otype),
      tableType_(COM_REGULAR_TABLE),
      objectClass_(COM_CLASS_USER_TABLE),
      isVolatile_(FALSE),
      exeUtil_(FALSE),
      isGhostObject_(FALSE),
      inMemoryObjectDefn_(FALSE),
      isExternal_(FALSE),
      isImplicitExternal_(FALSE),
      ddlXns_(FALSE),
      isPartition_(FALSE) {
  ddlXns_ = (CmpCommon::getDefault(DDL_TRANSACTIONS) == DF_ON);
}

// virtual destructor
// To improve performance, do not use inline with virtual destructor
StmtDDLNode::~StmtDDLNode() {}

//
// cast virtual functions
//

StmtDDLNode *StmtDDLNode::castToStmtDDLNode() { return this; }

const StmtDDLNode *StmtDDLNode::castToStmtDDLNode() const { return this; }

//
// methods for tracing
//

const NAString StmtDDLNode::getText() const {
  NAAbort("StmtDDLNode.C", __LINE__, "internal logic error");
  return "StmtDDLNode";
}

void StmtDDLNode::unparse(NAString &result, PhaseEnum /*phase*/, UnparseFormatEnum /*form*/, TableDesc *tabId) const {
  result += "a DDL statement";
}

NABoolean StmtDDLNode::performParallelOp(int numPartitions) {
  // PerformParallelOp
  NABoolean ppo = FALSE;

  // The CQD USE_PARALLEL_FOR_NUM_PARTITIONS specifies the number of
  // partitions that must exist before
  // performing the create in parallel.
  int numParts = CmpCommon::getDefaultLong(USE_PARALLEL_FOR_NUM_PARTITIONS);
  NAString pos;
  CmpCommon::getDefault(POS, pos, 0);

  // Parallel Create logic should be executed only if POS and
  // ATTEMPT_ESP_PARALLELISM are not turned OFF and table involved has
  // multiple partitions.
  if ((CmpCommon::getDefault(POS) != DF_OFF) && (!IsNAStringSpaceOrEmpty(pos)) &&
      (CmpCommon::getDefault(ATTEMPT_ESP_PARALLELISM) != DF_OFF) && (numPartitions >= numParts) &&
      (numPartitions > 1)) {
    // Check to see if parallel creation of labels is required
    // Parallel creations should not happen if POS_NUM_OF_PARTNS is set
    // to 1 ( single partition table).

    if (CmpCommon::getDefault(POS_NUM_OF_PARTNS, 0) == DF_SYSTEM)
      ppo = TRUE;
    else {  // Not DF_SYSTEM - get numeric POS_NUM_OF_PARTNS
      int posnumpartns = CmpCommon::getDefaultLong(POS_NUM_OF_PARTNS);
      if (posnumpartns > 1) ppo = TRUE;
    }
  }

  return ppo;
}

// -----------------------------------------------------------------------
// member functions for class StmtDDLGrant
// -----------------------------------------------------------------------

//
// constructor
//

StmtDDLGrant::StmtDDLGrant(ElemDDLNode *pPrivileges, const QualifiedName &objectName, ElemDDLNode *pGranteeList,
                           ElemDDLNode *pWithGrantOption, ElemDDLNode *pByGrantorOption, QualifiedName *actionName,
                           CollHeap *heap)
    : StmtDDLNode(DDL_GRANT),
      objectName_(heap),
      objectQualName_(objectName, heap),
      isAllPrivileges_(FALSE),
      isWithGrantOptionSpec_(FALSE),
      privActArray_(heap),
      actionQualName_(actionName),
      granteeArray_(heap),
      isByGrantorOptionSpec_(FALSE),
      byGrantor_(NULL) {
  setChild(INDEX_PRIVILEGES, pPrivileges);
  setChild(INDEX_GRANTEE_LIST, pGranteeList);
  setChild(INDEX_WITH_GRANT_OPTION, pWithGrantOption);
  setChild(INDEX_BY_GRANTOR_OPTION, pByGrantorOption);
  origObjectName_ = objectQualName_.getQualifiedNameAsAnsiString();
  objectName_ = objectQualName_.getQualifiedNameAsAnsiString();
  //
  // inserts pointers to parse nodes representing privilege
  // actions to privActArray_ so the user can access the
  // information about privilege actions easier.
  //

  ComASSERT(pPrivileges NEQ NULL);
  ElemDDLPrivileges *pPrivsNode = pPrivileges->castToElemDDLPrivileges();
  ComASSERT(pPrivsNode NEQ NULL);
  if (pPrivsNode->isAllPrivileges()) {
    isAllPrivileges_ = TRUE;
  } else {
    ElemDDLNode *pPrivActs = pPrivsNode->getPrivilegeActionList();
    for (CollIndex i = 0; i < pPrivActs->entries(); i++) {
      ElemDDLPrivAct *pPrivAct = (*pPrivActs)[i]->castToElemDDLPrivAct();
      privActArray_.insert(pPrivAct);
      if (pPrivAct->isDDLPriv()) privActArray_.setHasDDLPriv(TRUE);
    }
  }

  //
  // copies pointers to parse nodes representing grantee
  // to granteeArray_ so the user can access the information
  // easier.
  //

  ComASSERT(pGranteeList NEQ NULL);
  for (CollIndex i = 0; i < pGranteeList->entries(); i++) {
    granteeArray_.insert((*pGranteeList)[i]->castToElemDDLGrantee());
  }

  //
  // looks for With Grant option phrase
  //

  if (pWithGrantOption NEQ NULL) {
    isWithGrantOptionSpec_ = TRUE;
  }

  if (pByGrantorOption NEQ NULL) {
    isByGrantorOptionSpec_ = TRUE;
    byGrantor_ = pByGrantorOption->castToElemDDLGrantee();
  }

}  // StmtDDLGrant::StmtDDLGrant()

// virtual destructor
StmtDDLGrant::~StmtDDLGrant() {
  // delete all children
  for (int i = 0; i < getArity(); i++) {
    delete getChild(i);
  }
}

// cast
StmtDDLGrant *StmtDDLGrant::castToStmtDDLGrant() { return this; }

//
// accessors
//

int StmtDDLGrant::getArity() const { return MAX_STMT_DDL_GRANT_ARITY; }

ExprNode *StmtDDLGrant::getChild(int index) {
  ComASSERT(index >= 0 AND index < getArity());
  return children_[index];
}

//
// mutators
//

void StmtDDLGrant::setChild(int index, ExprNode *pChildNode) {
  ComASSERT(index >= 0 AND index < getArity());
  if (pChildNode NEQ NULL) {
    ComASSERT(pChildNode->castToElemDDLNode() NEQ NULL);
    children_[index] = pChildNode->castToElemDDLNode();
  } else {
    children_[index] = NULL;
  }
}

//
// methods for tracing
//

const NAString StmtDDLGrant::displayLabel1() const { return NAString("Object name: ") + getObjectName(); }

NATraceList StmtDDLGrant::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  //
  // object name
  //

  detailTextList.append(displayLabel1());  // object name

  //
  // privileges
  //

  StmtDDLGrant *localThis = (StmtDDLGrant *)this;

  detailTextList.append(
      localThis->getChild(INDEX_PRIVILEGES)->castToElemDDLNode()->castToElemDDLPrivileges()->getDetailInfo());

  //
  // grantee list
  //

  const ElemDDLGranteeArray &granteeArray = getGranteeArray();

  detailText = "Grantee list [";
  detailText += LongToNAString((int)granteeArray.entries());
  detailText += " element(s)]";
  detailTextList.append(detailText);

  for (CollIndex i = 0; i < granteeArray.entries(); i++) {
    detailText = "[grantee ";
    detailText += LongToNAString((int)i);
    detailText += "]";
    detailTextList.append(detailText);

    ComASSERT(granteeArray[i] NEQ NULL AND granteeArray[i]->castToElemDDLGrantee() NEQ NULL);
    detailTextList.append("    ", granteeArray[i]->castToElemDDLGrantee()->getDetailInfo());
  }

  //
  // with grant option
  //

  detailText = "is with grant option? ";
  detailText += YesNo(localThis->getChild(INDEX_WITH_GRANT_OPTION) NEQ NULL);
  detailTextList.append(detailText);

  return detailTextList;

}  // StmtDDLGrant::getDetailInfo

const NAString StmtDDLGrant::getText() const { return "StmtDDLGrant"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLGrantArray
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLGrantArray::~StmtDDLGrantArray() {}

// -----------------------------------------------------------------------
// member functions for class StmtDDLSchGrant
// -----------------------------------------------------------------------
StmtDDLSchGrant::StmtDDLSchGrant(ElemDDLNode *pPrivileges, const ElemDDLSchemaName &aSchemaNameParseNode,
                                 ElemDDLNode *pGranteeList, ElemDDLNode *pWithGrantOption,
                                 ElemDDLNode *pByGrantorOption, CollHeap *heap)
    : StmtDDLNode(DDL_GRANT_SCHEMA),
      schemaName_(heap),
      schemaQualName_(aSchemaNameParseNode.getSchemaName(), heap),
      isAllDMLPrivileges_(FALSE),
      isAllDDLPrivileges_(FALSE),
      isAllOtherPrivileges_(FALSE),
      isWithGrantOptionSpec_(FALSE),
      privActArray_(heap),
      granteeArray_(heap),
      isByGrantorOptionSpec_(FALSE),
      byGrantor_(NULL) {
  if (schemaQualName_.getSchemaName().isNull()) {
    schemaQualName_ = ActiveSchemaDB()->getDefaultSchema();
  }
  setChild(INDEX_PRIVILEGES, pPrivileges);
  setChild(INDEX_GRANTEE_LIST, pGranteeList);
  setChild(INDEX_WITH_GRANT_OPTION, pWithGrantOption);
  setChild(INDEX_BY_GRANTOR_OPTION, pByGrantorOption);
  // objectName_ = objectQualName_.getQualifiedNameAsAnsiString();
  //
  // inserts pointers to parse nodes representing privilege
  // actions to privActArray_ so the user can access the
  // information about privilege actions easier.
  //

  ComASSERT(pPrivileges NEQ NULL);
  ElemDDLPrivileges *pPrivsNode = pPrivileges->castToElemDDLPrivileges();
  ComASSERT(pPrivsNode NEQ NULL);

  // If columns level privileges specified, throw an error
  if (pPrivsNode->containsColumnPrivs()) {
    *SqlParser_Diags << DgSqlCode(-3028);
    return;
  }

  if (pPrivsNode->isAllPrivileges()) {
    isAllDMLPrivileges_ = TRUE;
    isAllDDLPrivileges_ = TRUE;
    isAllOtherPrivileges_ = TRUE;
  }
  if (pPrivsNode->isAllDMLPrivileges()) {
    isAllDMLPrivileges_ = TRUE;
  }
  if (pPrivsNode->isAllDDLPrivileges()) {
    isAllDDLPrivileges_ = TRUE;
  }
  if (pPrivsNode->isAllOtherPrivileges()) {
    isAllOtherPrivileges_ = TRUE;
  }

  ElemDDLNode *pPrivActs = pPrivsNode->getPrivilegeActionList();
  if (pPrivActs) {
    for (CollIndex i = 0; i < pPrivActs->entries(); i++) {
      privActArray_.insert((*pPrivActs)[i]->castToElemDDLPrivAct());
    }
  }

  //
  // copies pointers to parse nodes representing grantee
  // to granteeArray_ so the user can access the information
  // easier.
  //

  ComASSERT(pGranteeList NEQ NULL);
  for (CollIndex i = 0; i < pGranteeList->entries(); i++) {
    granteeArray_.insert((*pGranteeList)[i]->castToElemDDLGrantee());
  }

  //
  // looks for With Grant option phrase
  //

  if (pWithGrantOption NEQ NULL) {
    isWithGrantOptionSpec_ = TRUE;
  }

  if (pByGrantorOption NEQ NULL) {
    isByGrantorOptionSpec_ = TRUE;
    byGrantor_ = pByGrantorOption->castToElemDDLGrantee();
  }

}  // StmtDDLSchGrant::StmtDDLSchGrant()

// virtual destructor
StmtDDLSchGrant::~StmtDDLSchGrant() {
  // delete all children
  for (int i = 0; i < getArity(); i++) {
    delete getChild(i);
  }
}

// cast
StmtDDLSchGrant *StmtDDLSchGrant::castToStmtDDLSchGrant() { return this; }

//
// accessors
//

int StmtDDLSchGrant::getArity() const { return MAX_STMT_DDL_GRANT_ARITY; }

ExprNode *StmtDDLSchGrant::getChild(int index) {
  ComASSERT(index >= 0 AND index < getArity());
  return children_[index];
}

//
// mutators
//

void StmtDDLSchGrant::setChild(int index, ExprNode *pChildNode) {
  ComASSERT(index >= 0 AND index < getArity());
  if (pChildNode NEQ NULL) {
    ComASSERT(pChildNode->castToElemDDLNode() NEQ NULL);
    children_[index] = pChildNode->castToElemDDLNode();
  } else {
    children_[index] = NULL;
  }
}

//
// methods for tracing
//

const NAString StmtDDLSchGrant::displayLabel1() const { return NAString("Schema Name ") + getSchemaName(); }

NATraceList StmtDDLSchGrant::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  //
  // object name
  //

  detailTextList.append(displayLabel1());  // object name

  //
  // privileges
  //

  StmtDDLSchGrant *localThis = (StmtDDLSchGrant *)this;

  detailTextList.append(
      localThis->getChild(INDEX_PRIVILEGES)->castToElemDDLNode()->castToElemDDLPrivileges()->getDetailInfo());

  //
  // grantee list
  //

  const ElemDDLGranteeArray &granteeArray = getGranteeArray();

  detailText = "Grantee list [";
  detailText += LongToNAString((int)granteeArray.entries());
  detailText += " element(s)]";
  detailTextList.append(detailText);

  for (CollIndex i = 0; i < granteeArray.entries(); i++) {
    detailText = "[grantee ";
    detailText += LongToNAString((int)i);
    detailText += "]";
    detailTextList.append(detailText);

    ComASSERT(granteeArray[i] NEQ NULL AND granteeArray[i]->castToElemDDLGrantee() NEQ NULL);
    detailTextList.append("    ", granteeArray[i]->castToElemDDLGrantee()->getDetailInfo());
  }

  //
  // with grant option
  //

  detailText = "is with grant option? ";
  detailText += YesNo(localThis->getChild(INDEX_WITH_GRANT_OPTION) NEQ NULL);
  detailTextList.append(detailText);

  return detailTextList;

}  // StmtDDLGrant::getDetailInfo

const NAString StmtDDLSchGrant::getText() const { return "StmtDDLSchGrant"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLSchGrantArray
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLSchGrantArray::~StmtDDLSchGrantArray() {}

// -----------------------------------------------------------------------
// methods for class StmtDDLGrantComponentPrivilege
// -----------------------------------------------------------------------

//
// constructor
//

StmtDDLGrantComponentPrivilege::StmtDDLGrantComponentPrivilege(const ConstStringList *pComponentPrivilegeNameList,
                                                               const NAString &componentName,
                                                               const NAString &userRoleName,
                                                               const NABoolean isWithGrantOptionClauseSpecified,
                                                               ElemDDLNode *pOptionalGrantedBy,
                                                               CollHeap *heap)  // default is PARSERHEAP()
    : StmtDDLNode(DDL_GRANT_COMPONENT_PRIVILEGE),
      pComponentPrivilegeNameList_(pComponentPrivilegeNameList),
      componentName_(componentName, heap),
      userRoleName_(userRoleName, heap),
      isWithGrantOptionSpec_(isWithGrantOptionClauseSpecified),
      grantedBy_(NULL) {
  if (pOptionalGrantedBy NEQ NULL) {
    grantedBy_ = pOptionalGrantedBy->castToElemDDLGrantee();
  }
}

//
// virtual destructor
//

StmtDDLGrantComponentPrivilege::~StmtDDLGrantComponentPrivilege() {
  if (pComponentPrivilegeNameList_ NEQ NULL) delete pComponentPrivilegeNameList_;
}

// virtual safe cast-down function
StmtDDLGrantComponentPrivilege *StmtDDLGrantComponentPrivilege::castToStmtDDLGrantComponentPrivilege() { return this; }

//
// methods for tracing
//

const NAString StmtDDLGrantComponentPrivilege::displayLabel1() const {
  NAString aLabel("Component name: ");
  aLabel += getComponentName();
  return aLabel;
}

const NAString StmtDDLGrantComponentPrivilege::displayLabel2() const {
  NAString aLabel("User role name: ");
  aLabel += getUserRoleName();
  return aLabel;
}

NATraceList StmtDDLGrantComponentPrivilege::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  //
  // component name
  //

  detailTextList.append(displayLabel1());  // component name

  //
  // user role name
  //

  detailTextList.append(displayLabel2());  // user role name

  //
  // component privilege name list
  //

  const ConstStringList &privs = getComponentPrivilegeNameList();

  detailText = "Component Privilege Name List [";
  detailText += LongToNAString((int)privs.entries());
  detailText += " element(s)]";
  detailTextList.append(detailText);

  for (CollIndex i = 0; i < privs.entries(); i++) {
    detailText = "[";
    detailText += LongToNAString((int)i);
    detailText += "] ";
    detailText += *privs[i];
    detailTextList.append(detailText);
  }

  //
  // with grant option
  //

  detailText = "is with grant option? ";
  detailText += YesNo(isWithGrantOptionSpecified());
  detailTextList.append(detailText);

  return detailTextList;

}  // StmtDDLGrantComponentPrivilege::getDetailInfo

const NAString StmtDDLGrantComponentPrivilege::getText() { return "StmtDDLGrantComponentPrivilege"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLRevokeComponentPrivilege
// -----------------------------------------------------------------------

//
// constructor
//

StmtDDLRevokeComponentPrivilege::StmtDDLRevokeComponentPrivilege(const ConstStringList *pComponentPrivilegeNameList,
                                                                 const NAString &componentName,
                                                                 const NAString &userRoleName,
                                                                 const NABoolean isGrantOptionForClauseSpecified,
                                                                 ElemDDLNode *pOptionalGrantedBy,
                                                                 CollHeap *heap)  // default is PARSERHEAP()
    : StmtDDLNode(DDL_REVOKE_COMPONENT_PRIVILEGE),
      pComponentPrivilegeNameList_(pComponentPrivilegeNameList),  // shallow copy
      componentName_(componentName, heap),                        // deep copy
      userRoleName_(userRoleName, heap),                          // deep copy
      isGrantOptionForSpec_(isGrantOptionForClauseSpecified),
      grantedBy_(NULL) {
  if (pOptionalGrantedBy NEQ NULL) {
    grantedBy_ = pOptionalGrantedBy->castToElemDDLGrantee();
  }
}

//
// virtual destructor
//

StmtDDLRevokeComponentPrivilege::~StmtDDLRevokeComponentPrivilege() {
  if (pComponentPrivilegeNameList_ NEQ NULL) delete pComponentPrivilegeNameList_;
}

// virtual safe cast-down function
StmtDDLRevokeComponentPrivilege *StmtDDLRevokeComponentPrivilege::castToStmtDDLRevokeComponentPrivilege() {
  return this;
}

//
// methods for tracing
//

const NAString StmtDDLRevokeComponentPrivilege::displayLabel1() const {
  NAString aLabel("Component name: ");
  aLabel += getComponentName();
  return aLabel;
}

const NAString StmtDDLRevokeComponentPrivilege::displayLabel2() const {
  NAString aLabel("User rol name: ");
  aLabel += getUserRoleName();
  return aLabel;
}

NATraceList StmtDDLRevokeComponentPrivilege::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  //
  // component name
  //

  detailTextList.append(displayLabel1());  // component name

  //
  // user role name
  //

  detailTextList.append(displayLabel2());  // user role name

  //
  // component privilege name list
  //

  const ConstStringList &privs = getComponentPrivilegeNameList();

  detailText = "Component Privilege Name List [";
  detailText += LongToNAString((int)privs.entries());
  detailText += " element(s)]";
  detailTextList.append(detailText);

  for (CollIndex i = 0; i < privs.entries(); i++) {
    detailText = "[";
    detailText += LongToNAString((int)i);
    detailText += "] ";
    detailText += *privs[i];
    detailTextList.append(detailText);
  }

  //
  // with revoke option
  //

  detailText = "is Grant Option For clause specified? ";
  detailText += YesNo(isGrantOptionForSpecified());
  detailTextList.append(detailText);

  return detailTextList;

}  // StmtDDLRevokeComponentPrivilege::getDetailInfo

const NAString StmtDDLRevokeComponentPrivilege::getText() { return "StmtDDLRevokeComponentPrivilege"; }

// -----------------------------------------------------------------------
// member functions for class StmtDDLRevoke
// -----------------------------------------------------------------------

//
// constructor
//

StmtDDLRevoke::StmtDDLRevoke(NABoolean isGrantOptionFor, ElemDDLNode *pPrivileges, const QualifiedName &objectName,
                             ElemDDLNode *pGranteeList, ComDropBehavior dropBehavior, ElemDDLNode *pByGrantorOption,
                             QualifiedName *actionName, CollHeap *heap)
    : StmtDDLNode(DDL_REVOKE),
      objectName_(heap),
      objectQualName_(objectName, heap),
      isAllPrivileges_(FALSE),
      isGrantOptionForSpec_(isGrantOptionFor),
      dropBehavior_(dropBehavior),
      privActArray_(heap),
      actionQualName_(actionName),
      granteeArray_(heap),
      isByGrantorOptionSpec_(FALSE),
      byGrantor_(NULL) {
  setChild(INDEX_PRIVILEGES, pPrivileges);
  setChild(INDEX_GRANTEE_LIST, pGranteeList);
  setChild(INDEX_BY_GRANTOR_OPTION, pByGrantorOption);

  //
  // Fully expand the name.
  //
  origObjectName_ = objectQualName_.getQualifiedNameAsAnsiString();
  objectName_ = objectQualName_.getQualifiedNameAsAnsiString();

  //
  // inserts pointers to parse nodes representing privilege
  // actions to privActArray_ so the user can access the
  // information about privilege actions easier.
  //

  ComASSERT(pPrivileges NEQ NULL);
  ElemDDLPrivileges *pPrivsNode = pPrivileges->castToElemDDLPrivileges();
  ComASSERT(pPrivsNode NEQ NULL);
  if (pPrivsNode->isAllPrivileges()) {
    isAllPrivileges_ = TRUE;
  } else {
    ElemDDLNode *pPrivActs = pPrivsNode->getPrivilegeActionList();
    for (CollIndex i = 0; i < pPrivActs->entries(); i++) {
      ElemDDLPrivAct *pPrivAct = (*pPrivActs)[i]->castToElemDDLPrivAct();
      privActArray_.insert(pPrivAct);
      if (pPrivAct->isDDLPriv()) privActArray_.setHasDDLPriv(TRUE);
    }
  }

  //
  // copies pointers to parse nodes representing grantee
  // to granteeArray_ so the user can access the information
  // easier.
  //

  ComASSERT(pGranteeList NEQ NULL);
  for (CollIndex i = 0; i < pGranteeList->entries(); i++) {
    granteeArray_.insert((*pGranteeList)[i]->castToElemDDLGrantee());
  }

  if (pByGrantorOption NEQ NULL) {
    isByGrantorOptionSpec_ = TRUE;
    byGrantor_ = pByGrantorOption->castToElemDDLGrantee();
  }

}  // StmtDDLRevoke::StmtDDLRevoke()

// virtual destructor
StmtDDLRevoke::~StmtDDLRevoke() {
  // delete all children
  for (int i = 0; i < getArity(); i++) {
    delete getChild(i);
  }
}

// cast
StmtDDLRevoke *StmtDDLRevoke::castToStmtDDLRevoke() { return this; }

//
// accessors
//

int StmtDDLRevoke::getArity() const { return MAX_STMT_DDL_REVOKE_ARITY; }

ExprNode *StmtDDLRevoke::getChild(int index) {
  ComASSERT(index >= 0 AND index < getArity());
  return children_[index];
}

//
// mutators
//

void StmtDDLRevoke::setChild(int index, ExprNode *pChildNode) {
  ComASSERT(index >= 0 AND index < getArity());
  if (pChildNode NEQ NULL) {
    ComASSERT(pChildNode->castToElemDDLNode() NEQ NULL);
    children_[index] = pChildNode->castToElemDDLNode();
  } else {
    children_[index] = NULL;
  }
}

//
// methods for tracing
//

const NAString StmtDDLRevoke::displayLabel1() const { return NAString("Object name: ") + getObjectName(); }

const NAString StmtDDLRevoke::displayLabel2() const {
  NAString label2("Drop behavior: ");
  switch (getDropBehavior()) {
    case COM_CASCADE_DROP_BEHAVIOR:
      return label2 + "Cascade";

    case COM_RESTRICT_DROP_BEHAVIOR:
      return label2 + "Restrict";

    default:
      ABORT("internal logic error");
      return NAString();
  }
}

NATraceList StmtDDLRevoke::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  //
  // object name
  //

  detailTextList.append(displayLabel1());  // object name

  //
  // privileges
  //

  StmtDDLRevoke *localThis = (StmtDDLRevoke *)this;

  detailTextList.append(
      localThis->getChild(INDEX_PRIVILEGES)->castToElemDDLNode()->castToElemDDLPrivileges()->getDetailInfo());

  //
  // grantee list
  //

  const ElemDDLGranteeArray &granteeArray = getGranteeArray();

  detailText = "Grantee list [";
  detailText += LongToNAString((int)granteeArray.entries());
  detailText += " element(s)]";
  detailTextList.append(detailText);

  for (CollIndex i = 0; i < granteeArray.entries(); i++) {
    detailText = "[grantee ";
    detailText += LongToNAString((int)i);
    detailText += "]";
    detailTextList.append(detailText);

    ComASSERT(granteeArray[i] NEQ NULL AND granteeArray[i]->castToElemDDLGrantee() NEQ NULL);
    detailTextList.append("    ", granteeArray[i]->castToElemDDLGrantee()->getDetailInfo());
  }

  //
  // grant option for
  //

  detailText = "is grant option for? ";
  detailText += YesNo(isGrantOptionForSpecified());
  detailTextList.append(detailText);

  //
  // drop behavior
  //

  detailTextList.append(displayLabel2());  // drop behavior

  return detailTextList;

}  // StmtDDLRevoke::getDetailInfo

const NAString StmtDDLRevoke::getText() const { return "StmtDDLRevoke"; }

// -----------------------------------------------------------------------
// member functions for class StmtDDLSchRevoke
// -----------------------------------------------------------------------

//
// constructor
//

StmtDDLSchRevoke::StmtDDLSchRevoke(NABoolean isGrantOptionFor, ElemDDLNode *pPrivileges,
                                   const ElemDDLSchemaName &aSchemaNameParseNode, ElemDDLNode *pGranteeList,
                                   ComDropBehavior dropBehavior, ElemDDLNode *pByGrantorOption, CollHeap *heap)
    : StmtDDLNode(DDL_REVOKE_SCHEMA),
      schemaName_(heap),
      schemaQualName_(aSchemaNameParseNode.getSchemaName(), heap),
      isAllDDLPrivileges_(FALSE),
      isAllDMLPrivileges_(FALSE),
      isAllOtherPrivileges_(FALSE),
      isGrantOptionForSpec_(isGrantOptionFor),
      dropBehavior_(dropBehavior),
      privActArray_(heap),
      granteeArray_(heap),
      isByGrantorOptionSpec_(FALSE),
      byGrantor_(NULL) {
  setChild(INDEX_PRIVILEGES, pPrivileges);
  setChild(INDEX_GRANTEE_LIST, pGranteeList);
  setChild(INDEX_BY_GRANTOR_OPTION, pByGrantorOption);

  //
  // inserts pointers to parse nodes representing privilege
  // actions to privActArray_ so the user can access the
  // information about privilege actions easier.
  //

  ComASSERT(pPrivileges NEQ NULL);
  ElemDDLPrivileges *pPrivsNode = pPrivileges->castToElemDDLPrivileges();
  ComASSERT(pPrivsNode NEQ NULL);

  // If columns level privileges specified, throw an error
  if (pPrivsNode->containsColumnPrivs()) {
    *SqlParser_Diags << DgSqlCode(-3028);
    return;
  }

  if (pPrivsNode->isAllPrivileges()) {
    isAllDDLPrivileges_ = TRUE;
    isAllDMLPrivileges_ = TRUE;
    isAllOtherPrivileges_ = TRUE;
  }
  if (pPrivsNode->isAllDDLPrivileges()) {
    isAllDDLPrivileges_ = TRUE;
  }
  if (pPrivsNode->isAllDMLPrivileges()) {
    isAllDMLPrivileges_ = TRUE;
  }
  if (pPrivsNode->isAllOtherPrivileges()) {
    isAllOtherPrivileges_ = TRUE;
  }

  ElemDDLNode *pPrivActs = pPrivsNode->getPrivilegeActionList();
  if (pPrivActs) {
    for (CollIndex i = 0; i < pPrivActs->entries(); i++) {
      privActArray_.insert((*pPrivActs)[i]->castToElemDDLPrivAct());
    }
  }

  //
  // copies pointers to parse nodes representing grantee
  // to granteeArray_ so the user can access the information
  // easier.
  //

  ComASSERT(pGranteeList NEQ NULL);
  for (CollIndex i = 0; i < pGranteeList->entries(); i++) {
    granteeArray_.insert((*pGranteeList)[i]->castToElemDDLGrantee());
  }

  if (pByGrantorOption NEQ NULL) {
    isByGrantorOptionSpec_ = TRUE;
    byGrantor_ = pByGrantorOption->castToElemDDLGrantee();
  }

}  // StmtDDLSchRevoke::StmtDDLSchRevoke()

// virtual destructor
StmtDDLSchRevoke::~StmtDDLSchRevoke() {
  // delete all children
  for (int i = 0; i < getArity(); i++) {
    delete getChild(i);
  }
}

// cast
StmtDDLSchRevoke *StmtDDLSchRevoke::castToStmtDDLSchRevoke() { return this; }

//
// accessors
//

int StmtDDLSchRevoke::getArity() const { return MAX_STMT_DDL_REVOKE_ARITY; }

ExprNode *StmtDDLSchRevoke::getChild(int index) {
  ComASSERT(index >= 0 AND index < getArity());
  return children_[index];
}

//
// mutators
//

void StmtDDLSchRevoke::setChild(int index, ExprNode *pChildNode) {
  ComASSERT(index >= 0 AND index < getArity());
  if (pChildNode NEQ NULL) {
    ComASSERT(pChildNode->castToElemDDLNode() NEQ NULL);
    children_[index] = pChildNode->castToElemDDLNode();
  } else {
    children_[index] = NULL;
  }
}

//
// methods for tracing
//

const NAString StmtDDLSchRevoke::displayLabel1() const { return NAString("Schema name: ") + getSchemaName(); }

const NAString StmtDDLSchRevoke::displayLabel2() const {
  NAString label2("Drop behavior: ");
  switch (getDropBehavior()) {
    case COM_CASCADE_DROP_BEHAVIOR:
      return label2 + "Cascade";

    case COM_RESTRICT_DROP_BEHAVIOR:
      return label2 + "Restrict";

    default:
      ABORT("internal logic error");
      return NAString();
  }
}

NATraceList StmtDDLSchRevoke::getDetailInfo() const {
  NAString detailText;
  NATraceList detailTextList;

  //
  // object name
  //

  detailTextList.append(displayLabel1());  // object name

  //
  // privileges
  //

  StmtDDLSchRevoke *localThis = (StmtDDLSchRevoke *)this;

  detailTextList.append(
      localThis->getChild(INDEX_PRIVILEGES)->castToElemDDLNode()->castToElemDDLPrivileges()->getDetailInfo());

  //
  // grantee list
  //

  const ElemDDLGranteeArray &granteeArray = getGranteeArray();

  detailText = "Grantee list [";
  detailText += LongToNAString((int)granteeArray.entries());
  detailText += " element(s)]";
  detailTextList.append(detailText);

  for (CollIndex i = 0; i < granteeArray.entries(); i++) {
    detailText = "[grantee ";
    detailText += LongToNAString((int)i);
    detailText += "]";
    detailTextList.append(detailText);

    ComASSERT(granteeArray[i] NEQ NULL AND granteeArray[i]->castToElemDDLGrantee() NEQ NULL);
    detailTextList.append("    ", granteeArray[i]->castToElemDDLGrantee()->getDetailInfo());
  }

  //
  // grant option for
  //

  detailText = "is grant option for? ";
  detailText += YesNo(isGrantOptionForSpecified());
  detailTextList.append(detailText);

  //
  // drop behavior
  //

  detailTextList.append(displayLabel2());  // drop behavior

  return detailTextList;

}  // StmtDDLSchRevoke::getDetailInfo

const NAString StmtDDLSchRevoke::getText() const { return "StmtDDLSchRevoke"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLRegisterComponent
// -----------------------------------------------------------------------

//
// constructors used for (UN)REGISTER COMPONENT
//
StmtDDLRegisterComponent::StmtDDLRegisterComponent(
    StmtDDLRegisterComponent::RegisterComponentType eRegComponentParseNodeType, const NAString &sComponentName,
    const NABoolean isSystem, const NAString &sDetailInfo, CollHeap *heap)
    : StmtDDLNode(DDL_REGISTER_COMPONENT),
      registerComponentType_(eRegComponentParseNodeType),
      componentName_(sComponentName, heap),
      isSystem_(isSystem),
      componentDetailInfo_(sDetailInfo, heap) {}
StmtDDLRegisterComponent::StmtDDLRegisterComponent(
    StmtDDLRegisterComponent::RegisterComponentType eRegComponentParseNodeType, const NAString &sComponentName,
    ComDropBehavior dropBehavior, CollHeap *heap)
    : StmtDDLNode(DDL_REGISTER_COMPONENT),
      registerComponentType_(eRegComponentParseNodeType),
      componentName_(sComponentName, heap),
      dropBehavior_(dropBehavior),
      componentDetailInfo_(heap) {}

//
// virtual destructor
//
StmtDDLRegisterComponent::~StmtDDLRegisterComponent() {}

//
// cast
//
StmtDDLRegisterComponent *StmtDDLRegisterComponent::castToStmtDDLRegisterComponent() { return this; }

//
// methods for tracing
//

const NAString StmtDDLRegisterComponent::displayLabel1() const {
  NAString aLabel("UNREGISTER COMPONENT");
  if (getRegisterComponentType() == StmtDDLRegisterComponent::REGISTER_COMPONENT) {
    aLabel = "REGISTER COMPONENT";
    if (isSystem()) aLabel += " (SYSTEM)";
  }
  aLabel += " - External Component name: ";
  aLabel += getExternalComponentName();
  if (getRegisterComponentType() == StmtDDLRegisterComponent::UNREGISTER_COMPONENT) {
    aLabel += " Drop behavior: ";
    if (dropBehavior_ == COM_CASCADE_DROP_BEHAVIOR)
      aLabel += "CASCADE";
    else
      aLabel += "RESTRICT";
  }
  return aLabel;
}

const NAString StmtDDLRegisterComponent::displayLabel2() const {
  if (NOT getRegisterComponentDetailInfo().isNull()) {
    return NAString("Detail Information: ") + getRegisterComponentDetailInfo();
  } else {
    return NAString("No Detail Information (i.e., an empty string).");
  }
}

const NAString StmtDDLRegisterComponent::getText() const { return "StmtDDLRegisterComponent"; }

//
// End of File
//
