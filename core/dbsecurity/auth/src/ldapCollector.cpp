#include <vector>
#include <errno.h>
#include <stdlib.h>
#include <iostream>
#include "dbconn.h"
#include "ldapconfigfile.h"
#include "ldapconfignode.h"

using namespace std;

extern char *formatDN(char *dn);

#define LOG_AUTH_EVENTS(authEvents)                       \
    {                                                     \
        for (size_t i = 0; i < authEvents.size(); i++)    \
            cout << authEvents[i].getEventText() << endl; \
    }

#define FORMAT_FILTER(filter)                              \
    {                                                      \
        if ('(' != filter.at(0))                           \
        {                                                  \
            char *str = new char[filter.length() + 2 + 1]; \
            sprintf(str, "(%s)", filter.c_str());          \
            filter.clear();                                \
            filter.append(str);                            \
            delete[] str;                                  \
        }                                                  \
    }

#define FORMAT_DN(dn)                             \
    {                                             \
        char *str = formatDN((char *)dn.c_str()); \
        dn.clear();                               \
        dn.append(str);                           \
        delete[] str;                             \
    }

bool LDAPCollector::initialize(void)
{
    LDAPHostConfig *config = NULL;
    authInitEventLog();
    this->ldapConn_ = LDAPConfigNode::GetLDAPConnection(this->authEvents_, this->configName_.c_str(), -2, SearchConnection);
    if (NULL == this->ldapConn_)
        goto ERROR;

    config = (LDAPHostConfig *)this->ldapConn_->GetConfiguration();
    //create filter for user
    {
        char *userBase = NULL;
        char *userRdn = NULL;
        if (!this->ldapConn_->getLDAPUserDNSuffix(&userRdn, &userBase))
            goto ERROR;
        this->userBase_.append(userBase);
        if (this->userFilter.empty())
        {
            char str[128] = {0};
            sprintf(str, "(%s=*)", userRdn);
            this->userFilter.append(str);
        }
        else
        {
            FORMAT_FILTER(this->userFilter);
        }
        delete[] userBase;
        delete[] userRdn;
    }

    //create filter for group
    if (this->searchGroup)
    {
        this->groupBase_ = config->searchGroupBase;
        if (this->groupBase_.empty())
            goto ERROR;
        FORMAT_DN(this->groupBase_);

        this->memberAttr_ = config->searchGroupNameAttr;

        if (!this->groupFilter.empty())
        {
            FORMAT_FILTER(this->groupFilter);
        }
        else
        {
            char str[1024] = {0};
            sprintf(str, "(%s=*)", config->searchGroupNameAttr.c_str());
            this->groupFilter.append(str);
        }
        char *str = NULL;
        if (!config->searchGroupCustomFilter.empty())
        {
            FORMAT_FILTER(config->searchGroupCustomFilter);
            {
                str = new char[config->searchGroupCustomFilter.length() +
                               this->groupFilter.length() + 3 + 1];
                sprintf(str, "(&%s%s)", this->groupFilter.c_str(), config->searchGroupCustomFilter.c_str());
                config->searchGroupCustomFilter.clear();
                config->searchGroupCustomFilter.append(str);
                delete[] str;
            }
        }
        else
            config->searchGroupCustomFilter.append(this->groupFilter);

        str = new char[config->searchGroupObjectClass.length() +
                       config->searchGroupCustomFilter.length() + strlen("(& (objectClass=)") + 1 + 1];
        sprintf(str, "(& (objectClass=%s)%s)", config->searchGroupObjectClass.c_str(), config->searchGroupCustomFilter.c_str());
        this->groupFilter.clear();
        this->groupFilter.append(str);
        delete[] str;
    }
    this->printfInfo();
    return true;
ERROR:
    LOG_AUTH_EVENTS(this->authEvents_);
    return false;
}

void LDAPCollector::disconnect(void)
{
    if (!this->ldapConn_)
        return;
    this->ldapConn_->CloseConnection();
    this->ldapConn_->FreeInstance(-2, SearchConnection);
}

LDAPCollector::LDAPCollector(string configName, string configFileName)
{
    if (!configFileName.empty())
        setenv("TRAFAUTH_CONFIGFILE", configFileName.c_str(), 1);

    this->configName_ = configName.empty() ? "local" : configName;
}

bool LDAPCollector::connect()
{
    return true;
}

bool LDAPCollector::getValidNameInLDAP(list<char *> *result, bool isGroup)
{
    string *base;
    string *filter;
    string tmpFilter; //for group
    if (!this->ldapConn_)
    {
        cout << "No connection for LDAP." << endl;
        return false;
    }

    if (!isGroup)
    {
        base = &(this->userBase_);
        filter = &(this->userFilter);
    }
    else
    {
        base = &(this->groupBase_);
        filter = &(this->groupFilter);
    }
    if (LDSearchFound == this->ldapConn_->getLDAPEntryNames(
                             this->authEvents_, base, filter, result))
        return true;
    LOG_AUTH_EVENTS(this->authEvents_);
    return false;
}

#define MAX_ITEM_IN_FILTER 50

bool LDAPCollector::getValidGroupRelationshipInLDAP(list<char *> *groupNames, std::map<size_t, char *> *validLDAPUsers, map<char *, list<char *> *> *result)
{
    char *userBase = NULL;
    char *userRdn = NULL;
    if (!this->ldapConn_)
    {
        cout << "No connection for LDAP." << endl;
        return false;
    }

    if (groupNames->empty())
        return true;

    if (!this->ldapConn_->getLDAPUserDNSuffix(&userRdn, &userBase))
    {
        cout << "Failed on getLDAPUserDNSuffix." << endl;
        return false;
    }

    if (LDSearchFound == this->ldapConn_->searchGroupRelationship(this->authEvents_, groupNames, result))
    {
        for (map<char *, list<char *> *>::iterator itor = result->begin(); itor != result->end();)
        {
            char *groupName = itor->first;
            list<char *> *members = itor->second;
            for (list<char *>::iterator itor2 = members->begin(); itor2 != members->end();)
            {
                size_t hash = NameFilter::getNameHash(*itor2);
                if (0 == validLDAPUsers->count(hash))
                {
                    delete[] * itor2;
                    itor2 = members->erase(itor2);
                }
                else
                    itor2++;
            }
            if (members->empty())
            {
                delete[] groupName;
                delete members;
                itor = result->erase(itor);
            }
            else
                itor++;
        }
        return true;
    }
ERROR:
    LOG_AUTH_EVENTS(this->authEvents_);
    for (map<char *, list<char *> *>::iterator itor = result->begin(); itor != result->end();)
    {
        delete[] itor->first;
        for (list<char *>::iterator itor2 = itor->second->begin(); itor2 != itor->second->end(); itor2++)
            delete[] * itor2;
        delete itor->second;
        itor = result->erase(itor);
    }
    result->clear();
    return false;
}

void LDAPCollector::printfInfo(void)
{
    stringstream ss;
    LDAPHostConfig *config = (LDAPHostConfig *)this->ldapConn_->GetConfiguration();
    ss << "The configuration of LDAP" << endl;
    ss << "Hostname: " << endl;
    if (!config->hostName.empty())
    {
        for (vector<string>::iterator itor = config->hostName.begin(); itor != config->hostName.end(); itor++)
            ss << *itor << endl;
    }
    ss << "PortNumber: " << config->portNumber << endl;
    ss << "Base for user: " << this->userBase_ << endl;
    ss << "Filter for user: " << this->userFilter << endl;
    if (this->searchGroup)
    {
        ss << "Base for group: " << this->groupBase_ << endl;
        ss << "Filter for group: " << this->groupFilter << endl;
        ss << "Attribute for members in group: " << this->memberAttr_ << endl;
    }
    cout << ss.str() << endl;
}