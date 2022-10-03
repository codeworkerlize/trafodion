#ifndef DBCONN_H
#define DBCONN_H
#include <list>
#include <vector>
#include <map>
#include <string>
#include <sql.h>
#include <sqlext.h>
#include <unordered_map>
#include "namefilter.h"
#include "ldapconfignode.h"

enum QueryType
{
    USER = 1,
    GROUP = 2,
    MEMBER = 4,
    ALL = 7
};

class dbConnection
{
public:
    virtual bool initialize(void) = 0;
    virtual bool connect(void) = 0;
    virtual void disconnect(void) = 0;

protected:
    virtual void printfInfo(void) = 0;
};

class LDAPCollector : public dbConnection
{
public:
    LDAPCollector(std::string, std::string);
    bool initialize(void) override;
    bool connect(void) override;
    void disconnect(void) override;

    bool getValidNameInLDAP(list<char *> *, bool);
    bool getValidGroupRelationshipInLDAP(list<char *> *, std::map<size_t, char *> *, map<char *, list<char *> *> *);

    std::string userFilter;
    std::string groupFilter;
    bool searchGroup;

private:
    void printfInfo(void) override;

    std::string configName_;
    std::vector<AuthEvent> authEvents_;
    LDAPConfigNode *ldapConn_;

    std::string userBase_;
    std::string groupBase_;
    std::string memberAttr_;
};

class SQLCollector : public dbConnection
{
public:
    SQLCollector(std::string, std::string, std::string);
    bool initialize(void) override;
    bool connect(void) override;
    void disconnect(void) override;
    NameFilter *createDatabaseNameFilter(int);
    bool executeSQL(list<char *> *, list<char *> *);
    bool isLDAPauth(void);

private:
    void printfInfo(void) override{};

    char hostUri_[1024 * 4];
    SQLHANDLE hEnv_;
    SQLHANDLE hDbc_;
};
#endif