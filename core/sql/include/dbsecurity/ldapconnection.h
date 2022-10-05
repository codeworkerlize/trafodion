#if !defined(LDAPCONNECTION_H)
#define LDAPCONNECTION_H

//*********************************************************************
//*                                                                   *

//*                                                                   *
//*********************************************************************
#include "ldapconfignode.h"
#include "ldapconfigparam.h"

#pragma page "Class LdapConnection"
// *****************************************************************************
// *                                                                           *
// *  Class       LdapConnection                                               *
// *                                                                           *
// *              This class represents server connections to LDAP serves.     *
// *              It contains a list of server nodes, LdapConfigNode.          *
// *              This class loads the server configuration info from CONFIG   *
// *              table, then open a LDAP connection for each servers          *
// *                                                                           *
// *  Qualities                                                                *
// *              Abstract:    No                                              *
// *              Assignable:  Yes                                             *
// *              Copyable:    Yes                                             *
// *              Derivable:   Yes                                             *
// *                                                                           *
// *****************************************************************************
class LdapConnection {
 public:
  LdapConnection();
  LdapConnection(const LdapConnection &other);
  LdapConnection &operator=(const LdapConnection &other);
  virtual ~LdapConnection();

  int openConnection();
  int closeConnection();
  int updateConnection();
  int reopenConnection();
  void checkLDAPConfig(void);

  int addDefaultConfig();
  int prepareConfigTxt();
  int initConnection();

  void addNode(LdapConfigNode *configNode) { ldapConfigNodes_.push_back(configNode); };

  vector<LdapConfigNode *> *getLdapConfigNodes() { return &ldapConfigNodes_; }

  LdapConfigNode *getDefaultConfigNode() { return defaultConfigNode_; };

  LdapConfigNode *getSearchUserIdConfigNode() { return searchUserIdConfigNode_; };

  vector<LdapConfigNode *> *getSearchUniqueIdConfigNode() { return &searchUniqueIdConfigNode_; };

  LdapConfigNode *getAuthConfigNode() { return authConfigNode_; };

  LdapConfigNode *getUserFoundNode() { return userFoundNode_; };

  short getTotalActiveNodes() { return totalActiveNodes_; };

  int setDefaultConfigNode();
  int setSearchConfigNode();
  int setAuthConfigNode();

  void setUserFoundNode(LdapConfigNode *node) { userFoundNode_ = node; }

  bool isValid() { return isValid_; }

 private:
  bool isValid_;  // true iff this object represents a valid connection
  vector<LdapConfigNode *> ldapConfigNodes_;
  LdapConfigNode *defaultConfigNode_;
  LdapConfigNode *searchUserIdConfigNode_;
  vector<LdapConfigNode *> searchUniqueIdConfigNode_;
  LdapConfigNode *authConfigNode_;
  LdapConfigNode *userFoundNode_;

  short totalActiveNodes_;
  long long lastUpdateTimestamp_;

  int loadConfigParams();
  int loadConfigNodes();
  int checkConfigNodes();
  int searchAuthGroup(LdapConfigNode *node);
};

#endif
