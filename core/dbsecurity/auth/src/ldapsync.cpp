#include <stdlib.h>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <getopt.h>
#include <sys/stat.h>
#include <errno.h>
#include <signal.h>
#include <algorithm>
#include <unordered_map>

#include "inc/namefilter.h"
#include "inc/dbconn.h"

extern char *optarg;
extern int optind, opterr, optopt;

#define SQL_STATEMENTS_FILE_PREFIX "ldapsync.result"

#define MAX_ITEMS_IN_ALTER_GROUP_STATEMENT 100

typedef struct sqlInfo_
{
    string dsn;
    string username;
    string password;
} sqlInfo;

typedef struct ldapInfo_
{
    string configFile;
    string sectionName;
    string userFilter;
    string groupFilter;
} ldapInfo;

static NameFilter *nameFilter;

static map<size_t, char *> *ldapUsers = NULL;
static map<size_t, char *> *ldapGroups = NULL;

static int type = (int)QueryType::ALL;

//database Connection
static SQLCollector *sqlConn = NULL;
static LDAPCollector *ldapConn = NULL;

static pthread_barrier_t barrier;

static sqlInfo sql_info;
static ldapInfo ldap_info;

//counter
static int autoRegisterUserNumber = 0;
static int autoRegisterGroupNumber = 0;
static int alterGroupNumber = 0;
static int repeatedObjectNumber = 0;

#define GET_LDAP_OBJECTS_AND_CONVER_TO_MAP(isGroup)                      \
    {                                                                    \
        map<size_t, char *> **pMap = isGroup ? &ldapGroups : &ldapUsers; \
        list<char *> *ldapObjects = new list<char *>();                  \
        if (!ldapConn->getValidNameInLDAP(ldapObjects, isGroup))         \
            goto ERROR;                                                  \
        *pMap = NameFilter::covertToHashMap(ldapObjects);                \
        delete ldapObjects;                                              \
    }

#define FREE_LDAP_OBJECTS_MAP(isGroup)                                   \
    {                                                                    \
        map<size_t, char *> **pMap = isGroup ? &ldapGroups : &ldapUsers; \
        if (NULL != *pMap)                                               \
        {                                                                \
            for (map<size_t, char *>::iterator itor = (*pMap)->begin();  \
                 itor != (*pMap)->end(); itor++)                         \
                delete[] itor->second;                                   \
            delete *pMap;                                                \
            *pMap = NULL;                                                \
        }                                                                \
    }

static void convertToRegisterObjectStatement(map<size_t, char *> *names, list<char *> *sqlStatements, bool isGroup)
{
    char sql[1024 * 4] = {0};
    if (names->empty())
        return;
    for (map<size_t, char *>::iterator itor = names->begin(); itor != names->end(); itor++)
    {
        sprintf(sql, "create %s %s", isGroup ? "group" : "user", itor->second);
        sqlStatements->push_back(strdup(sql));
    }
}

static void convertToAlterGroupStatement(map<char *, list<char *> *> *groupRelationship, list<char *> *sqlStatements)
{
    char sqlBuf[1024 * 16] = {0};
    if (groupRelationship->empty())
        return;
    for (map<char *, list<char *> *>::iterator itor = groupRelationship->begin(); itor != groupRelationship->end();)
    {
        const char *groupName = nameFilter->getDatabaseName(itor->first, NameType::GROUPNAME);
        list<char *> *members = itor->second;
        if (NULL == groupName || members->empty())
        {
            if (!members->empty())
            {
                for (list<char *>::iterator itor2 = members->begin(); itor2 != members->end(); itor2 = members->erase(itor2))
                    delete[] * itor2;
            }
            delete members;
            //delete NULL is ok
            delete[] itor->first;
            itor = groupRelationship->erase(itor);
            continue;
        }
        bzero((void *)sqlBuf, sizeof(sqlBuf));
        sprintf(sqlBuf, "alter group %s add members(", groupName);
        volatile size_t counter = MAX_ITEMS_IN_ALTER_GROUP_STATEMENT;
        volatile size_t bufLen = strlen(sqlBuf);
        char *ptr = sqlBuf + bufLen;
        for (list<char *>::iterator itor2 = members->begin(); itor2 != members->end(); itor2++)
        {
            const char *dbName = nameFilter->getDatabaseName(*itor2, NameType::USERNAME);
            if (NULL == dbName)
                continue;
            if (0 == counter)
            {
                *(ptr - 1) = ')';
                sqlStatements->push_back(strdup(sqlBuf));
                bzero((void *)sqlBuf, sizeof(sqlBuf));
                sprintf(sqlBuf, "alter group %s add members(", groupName);
                bufLen = strlen(sqlBuf);
                ptr = sqlBuf + bufLen;
                counter = MAX_ITEMS_IN_ALTER_GROUP_STATEMENT;
            }
            int len = sprintf(ptr, "%s,", dbName);
            bufLen += len;
            ptr += len;
            counter--;
        }
        if (0 != counter && MAX_ITEMS_IN_ALTER_GROUP_STATEMENT != counter)
        {
            *(ptr - 1) = ')';
            sqlStatements->push_back(strdup(sqlBuf));
        }
        itor++;
    }
}

static void useage()
{
    stringstream ss;
    ss << "Usage:ldapsync [option]..." << endl;
    ss << "option ::=   -d <ODBC DSN name>          [MUST]     specifies the ODBC data source name for trafodion." << endl;
    ss << "             -u <username>               [MUST]     specifies the username for trafodion." << endl;
    ss << "             -p <password>               [MUST]     specifies the password for trafodion." << endl;
    ss << "             -f <filename>               [OPTIONAL] specifies the configuration file for ldap." << endl;
    ss << "                                                    Default is ${TRAF_CONF}/.traf_authentication_config when no item set." << endl;
    ss << "             -s <section name>           [OPTIONAL] specifies the configuration section name for file in '--ldapconfig'." << endl;
    ss << "                                                    Default is 'local' when no item set." << endl;
    ss << "             -t <user|group|member|all>  [OPTIONAL] specifies the type of the synchronization. Default is ALL." <<endl;
    ss << "                                                    Can combo with '+',like 'USER+GROUP'." <<endl;
    ss << "             -1 <LDAP filter>            [OPTIONAL] specifies the 'LDAP filter' statement for filter user on LDAP." << endl;
    ss << "             -2 <LDAP filter>            [OPTIONAL] specifies the 'LDAP filter' statement for filter group on LDAP." << endl;
    ss << "             -o <filename>               [OPTIONAL] specifies the prefix for filename for saving sql statement. Default is 'ldapsync.result'" << endl;
    ss << "             -a                          [OPTIONAL] automatically add non-registered users or groups in trafodion. Default is no" << endl;
    ss << endl;
    ss << "* The ODBC DSN name is set in the ODBC config file. Using 'odbcinst -j' to find out the ODBC config file." << endl;
    ss << "* The LDAP filter will like '(|(cn=abc*))'" << endl;
    ss << "* The -t with user or group need to specify -a togother" << endl;
    cout << ss.str() << endl;
    cout.flush();
}

static void saveStatements(list<char *> *names, string fileName)
{
    ofstream fs(fileName, ios::out | ios::trunc);
    if (fs.good() && fs.is_open())
    {
        for (list<char *>::iterator itor = names->begin(); itor != names->end(); itor++)
            fs << *itor << ';' << endl;
        fs.flush();
    }
    fs.close();
}

static void wakeupMainThread(void *param)
{
    pthread_kill((*(pthread_t *)param), SIGUSR1);
}

static void sighand(int signo)
{
    //do nothing
    return;
}

static void *getAllOjectNameFromTrafodion(void *param)
{
    pthread_cleanup_push(wakeupMainThread, param);
    pthread_barrier_wait(&barrier);

    nameFilter = NULL;
    if (sqlConn->connect())
    {
        if (sqlConn->isLDAPauth())
            cout << "Cannot execute in LDAP authentication for trafodion." << endl;
        else
            nameFilter = sqlConn->createDatabaseNameFilter(type);
    }

    pthread_exit((void *)(NULL != nameFilter));
    pthread_cleanup_pop(1);
}

static void *getAllOjectNameFormLDAP(void *param)
{
    pthread_cleanup_push(wakeupMainThread, param);
    pthread_barrier_wait(&barrier);

    if (!ldapConn->connect())
        goto ERROR;

    if (type & (QueryType::USER | QueryType::MEMBER))
        GET_LDAP_OBJECTS_AND_CONVER_TO_MAP(false);
    if (type & QueryType::GROUP)
        GET_LDAP_OBJECTS_AND_CONVER_TO_MAP(true);

    pthread_exit((void *)true);
ERROR:
    FREE_LDAP_OBJECTS_MAP(ldapUsers);
    FREE_LDAP_OBJECTS_MAP(ldapGroups);
    pthread_exit((void *)false);
    pthread_cleanup_pop(1);
}

static void findNonRegisterObjects(std::map<size_t, char *> *objects, NameType type,
                                   list<char *> *statements, list<char *> *reserved_statements)
{
    map<size_t, char *> needRegisterObjects;
    map<size_t, char *> registeredObjects;
    nameFilter->filterNames(objects, &needRegisterObjects, &registeredObjects, type);
    convertToRegisterObjectStatement(&needRegisterObjects, statements, (NameType::GROUPNAME == type));
    convertToRegisterObjectStatement(&registeredObjects, reserved_statements, (NameType::GROUPNAME == type));
    if (NameType::USERNAME == type)
        autoRegisterUserNumber += needRegisterObjects.size();
    else if (NameType::GROUPNAME == type)
        autoRegisterGroupNumber += needRegisterObjects.size();
    repeatedObjectNumber += registeredObjects.size();
    //update nameFilter
    for (map<size_t, char *>::iterator itor = needRegisterObjects.begin();
         itor != needRegisterObjects.end(); itor++)
        nameFilter->addNamesToFilter(itor->second, NameFilter::getNameHash(itor->second), type);
}

int main(int argc, char *argv[])
{
    //common
    int autoRegister = 0;
    char statementFilename[256];
    sprintf(statementFilename, "%s.%s", SQL_STATEMENTS_FILE_PREFIX, "succeed");
    char failedStatementFilename[256] = SQL_STATEMENTS_FILE_PREFIX;
    sprintf(failedStatementFilename, "%s.%s", SQL_STATEMENTS_FILE_PREFIX, "failed");

    //parameter
    int c;
    int option_index = 0;
    opterr = 1;
    static struct option long_options[] =
        {
            {"dsn", required_argument, NULL, 'd'},
            {"usename", required_argument, NULL, 'u'},
            {"password", required_argument, NULL, 'p'},
            {"ldap", required_argument, NULL, 'f'},
            {"sectionname", required_argument, NULL, 's'},
            {"type", required_argument, NULL, 't'},
            {"userfilter", required_argument, NULL, '1'},
            {"groupfilter", required_argument, NULL, '2'},
            {"autoregister", no_argument, &autoRegister, 'a'},
            {"outputfile", required_argument, NULL, 'o'},
            {0, 0, 0, 0}};

    if (argc <= 3)
    {
        useage();
        exit(1);
    }

    while (true)
    {
        c = getopt_long(argc, argv, "d:u:p:f:s:t:1:2:o:a", long_options, &option_index);

        if (c == -1)
            break;

        switch (c)
        {
        case 0:
            break;
        case 'd':
        {
            sql_info.dsn.clear();
            sql_info.dsn.append(optarg);
        }
        break;
        case 'u':
        {
            sql_info.username.clear();
            sql_info.username.append(optarg);
        }
        break;
        case 'p':
        {
            sql_info.password.clear();
            sql_info.password.append(optarg);
        }
        break;
        case 'f':
        {
            ldap_info.configFile.clear();
            ldap_info.configFile.append(optarg);
        }
        break;
        case 's':
        {
            ldap_info.sectionName.clear();
            ldap_info.sectionName.append(optarg);
        }
        break;
        case 't':
        {
            type = 0;
            char *ptr = strtok(optarg, "+");
            while (NULL != ptr)
            {
                if (0 == strcasecmp(ptr, "ALL"))
                    type = QueryType::ALL;
                else if (0 == strcasecmp(ptr, "USER"))
                    type |= QueryType::USER;
                else if (0 == strcasecmp(ptr, "GROUP"))
                    type |= QueryType::GROUP;
                else if (0 == strcasecmp(ptr, "MEMBER"))
                    type |= QueryType::MEMBER;
                else
                {
                    cout << "ldapsync: the argument --type is only support [USER|GROUP|MEMBER|ALL]." << endl;
                    exit(1);
                }
                ptr = strtok(NULL, "+");
            }
            if (0 == type)
            {
                cout << "ldapsync: the argument --type is only support [USER|GROUP|MEMBER|ALL]." << endl;
                exit(1);
            }
        }
        break;
        case '1':
        {
            ldap_info.userFilter.clear();
            ldap_info.userFilter.append(optarg);
        }
        break;
        case '2':
        {
            ldap_info.groupFilter.clear();
            ldap_info.groupFilter.append(optarg);
        }
        break;
        case 'a':
        {
            autoRegister = 1;
        }
        break;
        case 'o':
        {
            if ((255 - 8) < strlen(optarg))
            {
                cout << "ldapsync: the argument --outputfile is too long" << endl;
                exit(1);
            }
            snprintf(statementFilename, sizeof(statementFilename), "%s.%s", optarg, "succeed");
            snprintf(failedStatementFilename, sizeof(statementFilename), "%s.%s", optarg, "failed");
        }
        break;
        default:
        {
            useage();
            exit(1);
        }
        }
    }

    if (sql_info.dsn.empty())
    {
        cout << "ldapsync: need argument --dsn" << endl;
        useage();
        exit(1);
    }

    if (sql_info.username.empty())
    {
        cout << "ldapsync: need argument --usename" << endl;
        useage();
        exit(1);
    }

    if (sql_info.password.empty())
    {
        cout << "ldapsync: need argument --password" << endl;
        useage();
        exit(1);
    }

    if (QueryType::MEMBER > (type & QueryType::ALL) && 0 == autoRegister)
    {
        cout << "ldapsync: -t is only with USER or GROUP and without -a,nothing to do" << endl;
        exit(1);
    }

    //check ldap config
    {
        string cmd = "ldapconfigcheck -file ";
        FILE *pp = NULL;
        if (!ldap_info.configFile.empty())
            cmd.append(ldap_info.configFile);
        else
        {
            const char *tmp = getenv("TRAFAUTH_CONFIGFILE");
            if (NULL != tmp && 0 != tmp[0])
                cmd.append(tmp);
            else
            {
                tmp = getenv("TRAF_CONF");
                if (NULL == tmp || 0 == tmp[0])
                    cmd.append("/etc/trafodion/conf");
                else
                    cmd.append(tmp);
                cmd.append("/.traf_authentication_config");
            }
        }
        cout << cmd << endl;
        if (NULL != (pp = popen(cmd.c_str(), "r")))
        {
            bool isOk = false;
            char buf[4 * 1024] = {0};
            char *ptr;
            while (fgets(buf, sizeof(buf), pp))
            {
                cout << buf << endl;
                ptr = buf + (strlen(buf) - 9 - 1);
                isOk = (0 == strncmp(ptr, "is valid.", 9));
            }
            if (!isOk)
                exit(1);
        }
        else
        {
            cout << strerror(errno) << endl;
            exit(1);
        }
        pclose(pp);
    }

    //start connection
    sqlConn = new SQLCollector(sql_info.dsn, sql_info.username, sql_info.password);
    sqlConn->initialize();

    ldapConn = new LDAPCollector(ldap_info.sectionName, ldap_info.configFile);
    ldapConn->userFilter = ldap_info.userFilter;
    ldapConn->groupFilter = ldap_info.groupFilter;
    ldapConn->searchGroup = ((QueryType::GROUP | QueryType::MEMBER) & type);
    ldapConn->initialize();

    {
        int rc = 0;
        void *retValue;
        bool isContinue = true;
        pthread_t threads[2] = {0};
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_JOINABLE);
        //2 child threads + 1 main thread
        pthread_barrier_init(&barrier, NULL, 2 + 1);
        //wait for SIGUSR1
        struct sigaction actions;
        siginfo_t singalinfo;
        sigset_t waitSingalSet;
        bzero((void *)&actions, sizeof(struct sigaction));
        sigemptyset(&actions.sa_mask);
        actions.sa_flags = 0;
        actions.sa_handler = sighand;
        sigaction(SIGUSR1, &actions, NULL);

        sigemptyset(&waitSingalSet);
        sigaddset(&waitSingalSet, SIGUSR1);

        pthread_sigmask(SIG_BLOCK, &waitSingalSet, NULL);

        pthread_t mainThread = pthread_self();

        cout << "Start fetching data from trafodion and LDAP" << endl;

        rc = pthread_create(&threads[0], &attr, getAllOjectNameFormLDAP, (void *)&mainThread);
        if (rc)
        {
            cout << "Create thread for LDAP search fail, return code: " + rc << endl;
            exit(1);
        }

        rc = pthread_create(&threads[1], &attr, getAllOjectNameFromTrafodion, (void *)&mainThread);
        if (rc)
        {
            cout << "Create thread for database search fail, return code: " + rc << endl;
            exit(1);
        }

        pthread_barrier_wait(&barrier);
        while (isContinue)
        {
            struct timespec timeout;
            //300ms
            timeout.tv_sec = 0;
            timeout.tv_nsec = 300 * 1000 * 1000;

            bzero(&singalinfo, sizeof(siginfo_t));
            sigtimedwait(&waitSingalSet, &singalinfo, &timeout);

            for (int i = 0; i < 2; i++)
            {
                unsigned char otherThread = i ^ 1;
                rc = pthread_kill(threads[i], 0);
                if (ESRCH == rc)
                {
                    //thread has exited
                    pthread_join(threads[i], &retValue);
                    if (!retValue)
                    {
                        cout << "The thread for fetching " << ((0 == i) ? "LDAP" : "trafodion") << " is failed." << endl;
                        //kill other thread
                        pthread_cancel(threads[otherThread]);
                        pthread_join(threads[otherThread], &retValue);
                        exit(1);
                    }
                    else
                    {
                        cout << "Fetching from " << ((0 == i) ? "LDAP" : "trafodion") << " is complete." << endl;
                        pthread_join(threads[otherThread], &retValue);
                        if (!retValue)
                        {
                            cout << "The thread for fetching " << ((0 == otherThread) ? "LDAP" : "trafodion") << " is failed." << endl;
                            exit(1);
                        }
                        else
                        {
                            cout << "Fetching from " << ((0 == otherThread) ? "LDAP" : "trafodion") << " is complete." << endl;
                            isContinue = false;
                            break;
                        }
                    }
                }
            }
        }
        pthread_barrier_destroy(&barrier);
        pthread_attr_destroy(&attr);
    }
    //newline
    cout << endl;

    //save sql statements
    list<char *> *statements = new list<char *>();
    list<char *> *reserved_statements = new list<char *>();

    if ((type & QueryType::USER) && autoRegister)
        findNonRegisterObjects(ldapUsers, NameType::USERNAME, statements, reserved_statements);

    if ((type & QueryType::GROUP) && autoRegister)
        findNonRegisterObjects(ldapGroups, NameType::GROUPNAME, statements, reserved_statements);

    if (type & QueryType::MEMBER)
    {
        map<char *, list<char *> *> *ldapMembers = new map<char *, list<char *> *>();
        std::list<char *> *tmp = nameFilter->getGroupNames();
        if (tmp->empty())
        {
            //noting to do
            delete tmp;
            goto FINISH;
        }
        if (!ldapConn->getValidGroupRelationshipInLDAP(nameFilter->getGroupNames(), ldapUsers, ldapMembers))
            exit(1);
        delete tmp;
        for (map<char *, list<char *> *>::iterator itor = ldapMembers->begin(); itor != ldapMembers->end();)
        {
            list<char *> *members = itor->second;
            map<size_t, char *> *hash_members = NameFilter::covertToHashMap(itor->second);
            if (!hash_members->empty())
            {
                if (autoRegister)
                    findNonRegisterObjects(hash_members, NameType::USERNAME, statements, reserved_statements);
                else
                {
                    map<size_t, char *> nonRegisteredMembers;
                    nameFilter->filterNames(hash_members, &nonRegisteredMembers, NameType::USERNAME);
                    if (!nonRegisteredMembers.empty())
                    {
                        for (list<char *>::iterator itor2 = members->begin(); itor2 != members->end();)
                        {
                            size_t hash = NameFilter::getNameHash(*itor2);
                            if (0 != nonRegisteredMembers.count(hash))
                            {
                                delete[] * itor2;
                                itor2 = members->erase(itor2);
                            }
                            else
                                itor2++;
                        }
                    }
                }
            }
            if (members->empty())
            {
                delete[] itor->first;
                delete itor->second;
                itor = ldapMembers->erase(itor);
            }
            else
                itor++;
            delete hash_members;
        }
        convertToAlterGroupStatement(ldapMembers, statements);
        alterGroupNumber = ldapMembers->size();

        for (map<char *, list<char *> *>::iterator itor = ldapMembers->begin();
             itor != ldapMembers->end(); itor++)
            delete itor->second;
        delete ldapMembers;
    }

    ldapConn->disconnect();

    saveStatements(statements, statementFilename);
    saveStatements(reserved_statements, failedStatementFilename);

FINISH:
    //execute sql
    cout << "Total of " << autoRegisterUserNumber << " users need to register for trafodion." << endl;
    cout << "Total of " << autoRegisterGroupNumber << " groups need to register for trafodion." << endl;
    cout << "Total of " << alterGroupNumber << " groups need to update membership for trafodion." << endl;
    cout << "Found " << repeatedObjectNumber << " duplicate users or groups in trafodion AUTHS table." << endl;

    if (statements->empty())
        goto END;
    cout << "The statement for executing file is \33[1;32m" << statementFilename << "\033[0m" << endl;
    cout << endl;
    while (true)
    {
        char a;
        cout << "continue?(Y/N)" << endl;
        cin >> a;
        if ('Y' == toupper(a))
            break;
        else if ('N' == toupper(a))
            goto END;
    }
    sqlConn->executeSQL(statements, reserved_statements);
    //update statement file
    saveStatements(statements, statementFilename);
END:
    sqlConn->disconnect();
    if (!reserved_statements->empty())
    {
        cout << "Some statements cannot be executed. Please check the file \33[1;31m" << failedStatementFilename << "\033[0m" << endl;
        //update statement file
        saveStatements(reserved_statements, failedStatementFilename);
    }

    return 0;
}