#ifndef STMTDDLALTERUSER_H
#define STMTDDLALTERUSER_H
//******************************************************************************

//******************************************************************************
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLAlterUser.h
 * Description:  class for parse nodes representing alter user statements
 *
 *
 * Created:      May 27, 2011
 * Language:     C++
 *
 *****************************************************************************
 */

#include "common/ComLocationNames.h"
#include "ElemDDLLocation.h"
#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLRegisterUser;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Alter user statement
// -----------------------------------------------------------------------
class StmtDDLAlterUser : public StmtDDLNode {
 public:
  enum AlterUserCmdSubType {
    SET_EXTERNAL_NAME = 2,
    SET_IS_VALID_USER,
    SET_USER_PASSWORD,
    SET_USER_JOIN_GROUP,
    SET_USER_EXIT_GROUP,
    SET_USER_UNLOCK
  };

  StmtDDLAlterUser(const NAString &databaseUsername, AlterUserCmdSubType cmdSubType, const NAString *pExternalName,
                   NAString *pConfig, NABoolean isValidUser, CollHeap *heap, const NAString *authPassword);

  StmtDDLAlterUser(const NAString &databaseUsername, AlterUserCmdSubType cmdSubType, ElemDDLNode *groupList,
                   CollHeap *heap)
      : StmtDDLNode(DDL_ALTER_USER),
        databaseUserName_(databaseUsername, heap),
        alterUserCmdSubType_(cmdSubType),
        groupList_(groupList),
        isSetupWithDefaultPassword_(TRUE) {
    // not need pConfig ExternalName,the 'alter user add/remove group' statement is only use on local user auth
  }

  StmtDDLAlterUser(const NAString &databaseUsername, AlterUserCmdSubType cmdSubType, CollHeap *heap)
      : StmtDDLNode(DDL_ALTER_USER),
        databaseUserName_(databaseUsername, heap),
        alterUserCmdSubType_(cmdSubType),
        groupList_(NULL),
        isSetupWithDefaultPassword_(TRUE) {
    // not need pConfig ExternalName,the 'alter user unlock' statement is only use on local user auth
  }

  virtual ~StmtDDLAlterUser();

  virtual StmtDDLAlterUser *castToStmtDDLAlterUser();

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // accessors

  inline const NAString &getExternalUsername() const;
  inline const NAString &getDatabaseUsername() const;
  inline const NAString &getConfig() const;
  inline const NAString &getAuthPassword() const;
  inline void setAuthPassword(char *newPassword);
  inline NABoolean isValidUser() const;
  inline AlterUserCmdSubType getAlterUserCmdSubType() const;
  inline ElemDDLList *getUserGroupList() const;
  inline const NABoolean isSetupWithDefaultPassword() { return this->isSetupWithDefaultPassword_; }

  // for tracing

  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  // ---------------------------------------------------------------------
  // private data members
  // ---------------------------------------------------------------------

  NAString externalUserName_;
  NAString databaseUserName_;
  NAString config_;
  NAString authPassword_;  // encryption password
  AlterUserCmdSubType alterUserCmdSubType_;
  NABoolean isValidUser_;
  ElemDDLNode *groupList_;
  NABoolean isSetupWithDefaultPassword_;
};  // class StmtDDLAlterUser

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterUser
// -----------------------------------------------------------------------

//
// accessors
//

inline const NAString &StmtDDLAlterUser::getExternalUsername() const { return externalUserName_; }

inline const NAString &StmtDDLAlterUser::getDatabaseUsername() const { return databaseUserName_; }

inline const NAString &StmtDDLAlterUser::getConfig() const { return config_; }

inline const NAString &StmtDDLAlterUser::getAuthPassword() const { return authPassword_; }

inline NABoolean StmtDDLAlterUser::isValidUser() const { return isValidUser_; }

inline StmtDDLAlterUser::AlterUserCmdSubType StmtDDLAlterUser::getAlterUserCmdSubType() const {
  return alterUserCmdSubType_;
}

inline void StmtDDLAlterUser::setAuthPassword(char *newPassword) {
  NAString tmp(newPassword);
  this->authPassword_ = tmp;
}

inline ElemDDLList *StmtDDLAlterUser::getUserGroupList() const { return (ElemDDLList *)groupList_; }

#endif  // STMTDDLALTERUSER_H
