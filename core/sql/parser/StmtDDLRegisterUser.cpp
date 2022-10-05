/* -*-C++-*-

 ****************************************************************************
 *
 * File:         StmtDDLRegisterUser.cpp
 * Description:  Methods for classes representing DDL (Un)Register User Statements
 *
 *               Also contains definitions of non-inline methods of
 *               classes relating to view usages.
 *
 * Language:     C++
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
#include "parser/StmtDDLRegisterUser.h"
#include "ElemDDLAuthSchema.h"
#include "common/BaseTypes.h"
#include "export/ComDiags.h"
#include "common/ComOperators.h"
#ifndef SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#endif

#include "parser/SqlParserGlobals.h"  // must be last #include

// -----------------------------------------------------------------------
// methods for class StmtDDLRegisterUser
// -----------------------------------------------------------------------

//
// constructor
//
// constructor used for REGISTER USER
StmtDDLRegisterUser::StmtDDLRegisterUser(const NAString &externalUserName, const NAString *pDbUserName,
                                         const NAString *pConfig, ElemDDLNode *authSchema, NABoolean isUser,
                                         CollHeap *heap, const NAString *authPassword)
    : StmtDDLNode(DDL_REGISTER_USER),
      externalUserName_(externalUserName, heap),
      dropBehavior_(COM_UNKNOWN_DROP_BEHAVIOR),
      isSetupWithDefaultPassword_(TRUE) {
  if (pDbUserName == NULL) {
    NAString userName(externalUserName_, heap);
    dbUserName_ = userName;
  } else {
    NAString userName(*pDbUserName, heap);
    dbUserName_ = userName;
  }
  if (pConfig == NULL)
    config_ = ComString("", heap);
  else {
    NAString config(*pConfig, heap);
    config_ = config;
  }
  if (authSchema) {
    authSchema_ = authSchema->castToElemDDLAuthSchema();
    ComASSERT(authSchema_ NEQ NULL);
  } else
    authSchema_ = NULL;

  if (authPassword == NULL) {
    // set a defalut password
    NAString password(AUTH_DEFAULT_WORD, heap);
    authPassword_ = password;
  } else {
    NAString password(*authPassword, heap);
    authPassword_ = password;
    isSetupWithDefaultPassword_ = FALSE;
  }

  registerUserType_ = (isUser) ? REGISTER_USER : REGISTER_TENANT;
}

// constructor used for UNREGISTER USER
StmtDDLRegisterUser::StmtDDLRegisterUser(const NAString &dbUserName, const ComDropBehavior dropBehavior,
                                         NABoolean isUser, CollHeap *heap)
    : StmtDDLNode(DDL_REGISTER_USER),
      dbUserName_(dbUserName, heap),
      externalUserName_("", heap),
      dropBehavior_(dropBehavior),
      authSchema_(NULL),
      isSetupWithDefaultPassword_(TRUE) {
  registerUserType_ = (isUser) ? UNREGISTER_USER : UNREGISTER_TENANT;
}  // StmtDDLRegisterUser::StmtDDLRegisterUser()

//
// virtual destructor
//
StmtDDLRegisterUser::~StmtDDLRegisterUser() {
  // delete all children
  if (authSchema_) delete authSchema_;
}

//
// cast
//
StmtDDLRegisterUser *StmtDDLRegisterUser::castToStmtDDLRegisterUser() { return this; }

// -----------------------------------------------------------------------
// methods for class StmtDDLRegisterUserArray
// -----------------------------------------------------------------------

// virtual destructor
// Do the list of user commands need to be removed?
StmtDDLRegisterUserArray::~StmtDDLRegisterUserArray() {}

//
// End of File
//
