/* -*-C++-*-

 ****************************************************************************
 *
 * File:         StmtDDLCreateRole.cpp
 * Description:  Methods for classes representing Grant/Revoke Role Statements.
 *               Patterned after StmtDDLRegisterUser.cpp.
 *
 * Created:      June 2011
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#define SQLPARSERGLOBALS_FLAGS  // must precede all #include's

#include <stdlib.h>
#ifndef NDEBUG
#include <iostream>
#endif
#include "AllElemDDLPartition.h"
#include "AllElemDDLParam.h"
#include "AllElemDDLUdr.h"
#include "parser/StmtDDLRoleGrant.h"
#include "common/BaseTypes.h"
#include "export/ComDiags.h"
#include "common/ComOperators.h"

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif

#include "parser/SqlParserGlobals.h"  // must be last #include

// -----------------------------------------------------------------------
// member functions for class StmtDDLRoleGrant
// -----------------------------------------------------------------------
// constructor for GRANT /REVOKE ROLE
StmtDDLRoleGrant::StmtDDLRoleGrant(ElemDDLNode *pRolesList, ElemDDLNode *pGranteeList, NABoolean withAdmin,
                                   ElemDDLNode *pOptionalGrantedBy, ComDropBehavior dropBehavior, NABoolean isGrantRole,
                                   CollHeap *heap)

    : StmtDDLNode(DDL_GRANT_ROLE),
      withAdmin_(withAdmin),
      isGrantRole_(isGrantRole),
      dropBehavior_(dropBehavior),
      grantedBy_(NULL) {
  setChild(INDEX_ROLES_LIST, pRolesList);
  setChild(INDEX_GRANTEE_LIST, pGranteeList);
  setChild(INDEX_GRANTED_BY_OPTION, pOptionalGrantedBy);

  //
  // copies pointers to parse nodes representing grantee
  // to granteeArray_ so the user can access the information
  // easier.
  //

  ComASSERT(pGranteeList NEQ NULL);
  for (CollIndex i = 0; i < pGranteeList->entries(); i++) {
    granteeArray_.insert((*pGranteeList)[i]->castToElemDDLGrantee());
  }

  ComASSERT(pRolesList NEQ NULL);
  for (CollIndex i = 0; i < pRolesList->entries(); i++) {
    rolesArray_.insert((*pRolesList)[i]->castToElemDDLGrantee());
  }

  if (pOptionalGrantedBy NEQ NULL) {
    grantedBy_ = pOptionalGrantedBy->castToElemDDLGrantee();
  }

}  // StmtDDLRoleGrant::StmtDDLRoleGrant()

// virtual destructor
StmtDDLRoleGrant::~StmtDDLRoleGrant() {
  // delete all children
  for (int i = 0; i < getArity(); i++) {
    delete getChild(i);
  }
}
// cast
StmtDDLRoleGrant *StmtDDLRoleGrant::castToStmtDDLRoleGrant() { return this; }

//
// accessors
//

int StmtDDLRoleGrant::getArity() const { return MAX_STMT_DDL_GRANT_ARITY; }

ExprNode *StmtDDLRoleGrant::getChild(int index) {
  ComASSERT(index >= 0 AND index < getArity());
  return children_[index];
}

//
// mutators
//

void StmtDDLRoleGrant::setChild(int index, ExprNode *pChildNode) {
  ComASSERT(index >= 0 AND index < getArity());
  if (pChildNode NEQ NULL) {
    ComASSERT(pChildNode->castToElemDDLNode() NEQ NULL);
    children_[index] = pChildNode->castToElemDDLNode();
  } else {
    children_[index] = NULL;
  }
}

// -----------------------------------------------------------------------
// methods for class StmtDDLRoleGrantArray
// -----------------------------------------------------------------------

// virtual destructor
StmtDDLRoleGrantArray::~StmtDDLRoleGrantArray() {}
