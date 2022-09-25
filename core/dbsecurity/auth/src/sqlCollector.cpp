#include <vector>
#include <errno.h>
#include <stdlib.h>
#include <iostream>
#include "dbconn.h"

using namespace std;

#define FETCH_ROW_ONE_TIME_ARRAY 1000

typedef struct auth_info_
{
    SQLCHAR AUTH_DB_NAME[256 * 2];
    SQLLEN idn_AUTH_DB_NAME;
    SQLCHAR AUTH_EXT_NAME[256 * 2];
    SQLLEN idn_AUTH_EXT_NAME;
    SQLCHAR AUTH_TYPE[2 * 2];
    SQLLEN idn_AUTH_TYPE;
} auth_info;

static void extract_error(
    const char *fn,
    SQLHANDLE handle,
    SQLSMALLINT type)
{
    SQLINTEGER i = 0;
    SQLINTEGER native;
    SQLCHAR state[8];
    SQLCHAR text[2 * 1024];
    SQLSMALLINT len;
    SQLRETURN ret;
    do
    {
        ret = SQLGetDiagRec(type, handle, ++i, state, &native, text,
                            sizeof(text), &len);
        if (SQL_SUCCEEDED(ret))
            cout << "\033[0;31m" << text << "\033[0m" << endl;
    } while (ret == SQL_SUCCESS);
}

SQLCollector::SQLCollector(std::string DSN, std::string username, std::string password)
{
    bzero((void *)this->hostUri_, sizeof(this->hostUri_));
    sprintf(this->hostUri_, "DSN=%s;UID=%s;PWD=%s", DSN.c_str(), username.c_str(), password.c_str());
    this->hDbc_ = NULL;
    this->hEnv_ = NULL;
}

bool SQLCollector::initialize(void)
{
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &(this->hEnv_));
    SQLSetEnvAttr(hEnv_, SQL_ATTR_ODBC_VERSION, (SQLPOINTER)SQL_OV_ODBC3, SQL_IS_INTEGER);
    SQLSetEnvAttr(hEnv_, SQL_ATTR_CONNECTION_POOLING, (SQLPOINTER)SQL_CP_OFF, SQL_IS_INTEGER);
    SQLAllocHandle(SQL_HANDLE_DBC, hEnv_, &(this->hDbc_));
    this->printfInfo();
    return true;
}

bool SQLCollector::connect(void)
{
    if (SQLDriverConnect(hDbc_, NULL, (SQLCHAR *)this->hostUri_, SQL_NTS, NULL, 0, NULL, SQL_DRIVER_NOPROMPT))
        goto ERROR;
    return true;
ERROR:
    extract_error("connect", hDbc_, SQL_HANDLE_DBC);
    return false;
}

void SQLCollector::disconnect(void)
{
    if (NULL != hDbc_)
    {
        SQLDisconnect(hDbc_);
        SQLFreeHandle(SQL_HANDLE_DBC, hDbc_);
    }
    if (NULL != hEnv_)
        SQLFreeHandle(SQL_HANDLE_ENV, hEnv_);
}

bool SQLCollector::executeSQL(list<char *> *statements, list<char *> *reserved_statements)
{
    SQLHANDLE hStmt;
    SQLRETURN ret;
    bool isOk = true;
    if (NULL == this->hDbc_)
        return false;
    SQLAllocHandle(SQL_HANDLE_STMT, this->hDbc_, &hStmt);
    //begin work
    SQLSetStmtAttr(hStmt, SQL_ATTR_AUTOCOMMIT, (SQLPOINTER)SQL_AUTOCOMMIT_OFF, 0);
    for (list<char *>::iterator itor = statements->begin(); itor != statements->end();)
    {
        cout << *itor << ';' << endl;
        ret = SQLExecDirect(hStmt, (SQLCHAR *)*itor, SQL_NTS);
        if (!SQL_SUCCEEDED(ret))
        {
            reserved_statements->push_back(strdup(*itor));
            itor = statements->erase(itor);
            extract_error("executeSQL", hStmt, SQL_HANDLE_STMT);
            isOk = false;
            //TODO
            //goto ERROR;
            continue;
        }
        else if (SQL_SUCCESS_WITH_INFO == ret)
            extract_error("executeSQL", hStmt, SQL_HANDLE_STMT);
        itor++;
    }
    SQLEndTran(SQL_HANDLE_DBC, this->hDbc_, SQL_COMMIT);
    goto END;
ERROR:
    isOk = false;
    if (NULL != hStmt)
    {
        extract_error("executeSQL", hStmt, SQL_HANDLE_STMT);
        SQLEndTran(SQL_HANDLE_DBC, this->hDbc_, SQL_ROLLBACK);
    }
END:
    if (NULL != hStmt)
        SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    return isOk;
}

NameFilter *SQLCollector::createDatabaseNameFilter(int type)
{
    SQLHANDLE hStmt;
    if (NULL == this->hDbc_)
        return NULL;
    NameFilter *nameFilter = new NameFilter();
    bool isOk = false;
    char sql[1024 * 4] = {0};
    auth_info *users = new auth_info[FETCH_ROW_ONE_TIME_ARRAY];
    SQLUSMALLINT *RowStatusArray = new SQLUSMALLINT[FETCH_ROW_ONE_TIME_ARRAY];
    SQLULEN NumRowsFetched;
    SQLRETURN ret;
    //bind row
    SQLAllocHandle(SQL_HANDLE_STMT, this->hDbc_, &hStmt);
    SQLSetStmtAttr(hStmt, SQL_ATTR_ROW_BIND_TYPE, (SQLPOINTER)sizeof(auth_info), 0);
    SQLSetStmtAttr(hStmt, SQL_ATTR_ROW_ARRAY_SIZE, (SQLPOINTER)FETCH_ROW_ONE_TIME_ARRAY, 0);
    SQLSetStmtAttr(hStmt, SQL_ATTR_ROW_STATUS_PTR, (SQLPOINTER)RowStatusArray, 0);
    SQLSetStmtAttr(hStmt, SQL_ATTR_ROWS_FETCHED_PTR, &NumRowsFetched, 0);
    SQLBindCol(hStmt, 1, SQL_C_CHAR, &users[0].AUTH_DB_NAME, (256 * 2), (SQLLEN *)&users[0].idn_AUTH_DB_NAME);
    SQLBindCol(hStmt, 2, SQL_C_CHAR, &users[0].AUTH_EXT_NAME, (256 * 2), (SQLLEN *)&users[0].idn_AUTH_EXT_NAME);
    SQLBindCol(hStmt, 3, SQL_C_CHAR, &users[0].AUTH_TYPE, (2 * 2), (SQLLEN *)&users[0].idn_AUTH_TYPE);
    //get all of valid Name form trafodion
    sprintf(sql, "select distinct AUTH_DB_NAME,AUTH_EXT_NAME,AUTH_TYPE from \"_MD_\".AUTHS");
    if (SQLExecDirect(hStmt, (SQLCHAR *)sql, SQL_NTS))
        goto ERROR;
    while ((ret = SQLFetchScroll(hStmt, SQL_FETCH_NEXT, 0)) != SQL_NO_DATA)
    {
        if (!SQL_SUCCEEDED(ret))
            goto ERROR;
        else if (SQL_SUCCESS_WITH_INFO == ret)
            extract_error("createDatabaseNameFilter", hStmt, SQL_HANDLE_STMT);
        for (int i = 0; i < NumRowsFetched; i++)
        {
            if (RowStatusArray[i] == SQL_ROW_SUCCESS || RowStatusArray[i] == SQL_ROW_SUCCESS_WITH_INFO)
            {
                if (users[i].idn_AUTH_DB_NAME == SQL_NULL_DATA)
                    users[i].AUTH_DB_NAME[0] = 0;
                if (users[i].idn_AUTH_EXT_NAME == SQL_NULL_DATA)
                    users[i].AUTH_EXT_NAME[0] = 0;
                if (users[i].idn_AUTH_TYPE == SQL_NULL_DATA)
                    users[i].AUTH_TYPE[0] = '?';
                const char *p1 = (const char *)(users[i].AUTH_DB_NAME);
                const char *p2 = (const char *)(users[i].AUTH_EXT_NAME);
                switch (users[i].AUTH_TYPE[0])
                {
                case 'U':
                {
                    //key: hash(AUTH_EXT_NAME) value: AUTH_DB_NAME
                    if (type & (QueryType::USER | QueryType::MEMBER))
                        nameFilter->addNamesToFilter(strdup(p1), NameFilter::getNameHash(p2), NameType::USERNAME);
                    else
                        nameFilter->addNamesToFilter(strdup(p1), 0, NameType::OTHER);
                }
                break;
                case 'G':
                {
                    if (type & (QueryType::GROUP | QueryType::MEMBER))
                        nameFilter->addNamesToFilter(strdup(p1), NameFilter::getNameHash(p1), NameType::GROUPNAME);
                    else
                        nameFilter->addNamesToFilter(strdup(p1), 0, NameType::OTHER);
                }
                break;
                default:
                {
                    nameFilter->addNamesToFilter(strdup(p1), 0, NameType::OTHER);
                }
                break;
                }
                //save AUTH_EXT_NAME to map
                if (0 != strcmp(p1, p2))
                    nameFilter->addNamesToFilter(strdup(p2), 0, NameType::OTHER);
                bzero((void *)&users[i], sizeof(auth_info));
            }
        }
    }
    isOk = true;
ERROR:
    if (NULL != hStmt)
    {
        if (!isOk)
        {
            extract_error("createDatabaseNameFilter", hStmt, SQL_HANDLE_STMT);
            delete nameFilter;
            nameFilter = NULL;
        }
        SQLCloseCursor(hStmt);
        SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    }
    delete[] users;
    delete[] RowStatusArray;
    return nameFilter;
}

bool SQLCollector::isLDAPauth(void)
{
    SQLHANDLE hStmt;
    SQLRETURN rc;
    char buffer[1024 * 4] = {0};
    SQLLEN len;
    SQLSMALLINT rowNum = 0;
    bool isLDAPAuth = false;
    if (NULL == this->hDbc_)
        return false;
    SQLAllocHandle(SQL_HANDLE_STMT, this->hDbc_, &hStmt);
    rc = SQLExecDirect(hStmt, (SQLCHAR *)"GET AUTHENTICATION", SQL_NTS);
    if (!SQL_SUCCEEDED(rc))
        goto ERROR;
    do
    {
        rc = SQLFetch(hStmt);
        if (!SQL_SUCCEEDED(rc))
            goto ERROR;
        rc = SQLGetData(hStmt, 1, SQL_C_TCHAR, (SQLPOINTER)buffer, sizeof(buffer), &len);
        if (!SQL_SUCCEEDED(rc))
            goto ERROR;
        if (strstr(buffer, "LDAP"))
        {
            isLDAPAuth = true;
            break;
        }
        bzero((void *)&buffer, sizeof(buffer));
    } while (rc != SQL_NO_DATA);
ERROR:
    if (SQL_NO_DATA != rc && !SQL_SUCCEEDED(rc))
        extract_error("isLDAPauth", hStmt, SQL_HANDLE_STMT);
    SQLFreeHandle(SQL_HANDLE_STMT, hStmt);
    return isLDAPAuth;
}