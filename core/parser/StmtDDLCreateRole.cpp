/* -*-C++-*-

****************************************************************************
 *
 * File:         StmtDDLCreateRole.cpp
 * Description:  Methods for classes representing Create/Drop Role Statements.
 *               Patterned after StmtDDLRegisterUser.cpp.
 *
 * Created:      April 2011
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
#include "parser/AllElemDDLParam.h"
#include "parser/AllElemDDLPartition.h"
#include "parser/AllElemDDLUdr.h"
#include "common/BaseTypes.h"
#include "common/ComOperators.h"
#include "export/ComDiags.h"
#include "parser/StmtDDLCreateRole.h"

#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif

#include "parser/SqlParserGlobals.h"  // must be last #include

// -----------------------------------------------------------------------
// methods for class StmtDDLCreateRole
// -----------------------------------------------------------------------

//
// constructor
//
// constructor used for CREATE ROLE
StmtDDLCreateRole::StmtDDLCreateRole(const NAString &roleName, ElemDDLNode *pOwner, NABoolean adminRole,
                                     NAString *tenantName, CollHeap *heap)
    : StmtDDLNode(DDL_CREATE_ROLE), roleName_(roleName, heap), isCreateRole_(TRUE), adminRole_(adminRole) {
  if (tenantName) tenantName_ = *tenantName;

  if (pOwner) {
    pOwner_ = pOwner->castToElemDDLGrantee();
    ComASSERT(pOwner_ NEQ NULL);
    isCurrentUserSpecified_ = FALSE;
  } else {
    pOwner_ = NULL;
    isCurrentUserSpecified_ = TRUE;
  }
}

// constructor used for DROP ROLE
StmtDDLCreateRole::StmtDDLCreateRole(const NAString &roleName, CollHeap *heap)
    : StmtDDLNode(DDL_CREATE_ROLE),
      roleName_(roleName, heap),
      isCreateRole_(FALSE),
      pOwner_(NULL),                   // does not apply to drop but don't leave it uninitialized
      isCurrentUserSpecified_(FALSE),  // does not apply to drop but don't leave it uninitialized
      adminRole_(FALSE)                // does not apply but initialize it anyway
{}                                     // StmtDDLCreateRole::StmtDDLCreateRole()

//
// virtual destructor
//
StmtDDLCreateRole::~StmtDDLCreateRole() {
  // delete all children
  if (pOwner_) delete pOwner_;
}

//
// cast
//
StmtDDLCreateRole *StmtDDLCreateRole::castToStmtDDLCreateRole() { return this; }

//
// methods for tracing
//

const NAString StmtDDLCreateRole::displayLabel1() const { return NAString("Role name: ") + getRoleName(); }

const NAString StmtDDLCreateRole::getText() const { return "StmtDDLCreateRole"; }

// -----------------------------------------------------------------------
// methods for class StmtDDLCreateRoleArray
// -----------------------------------------------------------------------

// virtual destructor
// Do the list of user commands need to be removed?
StmtDDLCreateRoleArray::~StmtDDLCreateRoleArray() {}

//
// End of File
//
