
/* -*-C++-*- ***************************************************************
 *
 * File:         ComSqlId.h
 * Description:
 *
 * Created:      10/31/2006
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#ifndef COM_SQL_ID_H
#define COM_SQL_ID_H

#include <stddef.h>
#include "common/CollHeap.h"
#include "common/Int64.h"

/////////////////////////////////////////////////////
//
// class ComSqlId
//
/////////////////////////////////////////////////////
class ComSqlId {
 public:
  // this enum *must* be in sync with SQLQUERYID_ATTR_TYPE in cli/sqlcli.h
  enum SQLQUERYID_ATTR_TYPE {
    /* segment number                                       */
    SQLQUERYID_SEGMENTNUM = 0,

    /* segment name: max 10 bytes                           */
    SQLQUERYID_SEGMENTNAME = 1,

    /* cpu number                                           */
    SQLQUERYID_CPUNUM = 2,

    /* pin of master exe process                            */
    SQLQUERYID_PIN = 3,

    /* starttime of master exe process                      */
    SQLQUERYID_EXESTARTTIME = 4,

    /* unique session number                                */
    SQLQUERYID_SESSIONNUM = 5,

    /* null terminated user name: max 32 bytes              */
    SQLQUERYID_USERNAME = 6,

    /* null terminated session name: max 24 bytes           */
    SQLQUERYID_SESSIONNAME = 7,

    /* unique query number within a process                    */
    SQLQUERYID_QUERYNUM = 8,

    /* null terminated user statement name: max 110 bytes         */
    SQLQUERYID_STMTNAME = 9,

    /* null terminated session id: max 104 bytes         */
    SQLQUERYID_SESSIONID = 10,

    SQLQUERYID_VERSION = 11,
    SQLQUERYID_TENANTID = 12
  };

  /*
    Session ID:
    ===========
    MXID<version><segment><cpu><pin><processStartTS><sessNum><unLen><userName><snLen><sessionName>
    <version>:         version number of ID                   : 2 digits
    <segment>:         segment number                         : 3 digits
    <cpu>:             cpu number                             : 2 digits
    <pin>:             pin                                    : 4 digits
    <processStartTS>:  time when master exe process started   : 18 digits
    <sessNum>:         sequentially increasing session number : 10 digits
    <uidLen>:           length of user id                      : 2 digits
    <userID>:           user id(ascii )                       : unLen bytes(max 15)
    <tidLen>:           tenant id len                         : 2 digits
    <tenant id>:        tenant id (ascii)                     : tenant length (max 15)
    <snLen>:           length of user specified session name  : 2 digits
    <sessionName>:     actual session name                    : snLen bytes(max 24)

    Query ID:
    =========
    <Session ID>_<queryNum>_<userStmtName>
    <queryNum>:       unique query number                    : max 18 digits
    <userStmtName>:   odbc generated stmt name               : max 32 bytes

    Max Query ID Len:  160 bytes (currently using 149 bytes)
   */

#define COM_SESSION_ID_PREFIX "MXID"

  enum { NEO_SQL_ID_VERSION = 1, SQ_SQL_ID_VERSION = 11 };

  // In case of Linux, segment and cpu will be same
  // Hence, we will treat one of them with 0 length
  // However, when we extract segment and cpu, it will return
  // the same value
  enum {
    PREFIX_LEN = 4,
    VERSION_LEN = 2,
    SEGMENT_LEN = 3,
    CPU_LEN = 3,
    PIN_LEN = 6,
    NEO_SEGMENT_LEN = 3,
    NEO_CPU_LEN = 2,
    NEO_PIN_LEN = 4,
    STARTTS_LEN = 18,
    SESSIONNUM_LEN = 10,
    USERNAMELEN_LEN = 2,
    TENANTIDLEN_LEN = 2,
    SESSIONNAMELEN_LEN = 2,
    QUERYNUM_LEN = 18
  };

  enum {
    MIN_SESSION_ID_LEN = PREFIX_LEN + VERSION_LEN + SEGMENT_LEN +
                         // in case Cpu and Segment number are at the same location
                         PIN_LEN + STARTTS_LEN + SESSIONNUM_LEN + USERNAMELEN_LEN + SESSIONNAMELEN_LEN
  };

  enum {
    MIN_QUERY_ID_LEN = MIN_SESSION_ID_LEN + 1 +  // underscore separator
                       1 +                       // min queryNum
                       1 +                       // underscore separator
                       1                         // query name (atleast one byte)
  };

  enum {
    MAX_SESSION_ID_LEN = 104,
    MAX_SESSION_NAME_LEN = 24,
    MAX_LDAP_USER_NAME_LEN = 128,
    MAX_GUARDIAN_USER_ALIAS_LEN = 32,
    MAX_PASSWORD_LEN = 64
  };

  enum { MAX_QUERY_ID_LEN = 160 };
#define MAX_QUERY_ID_LEN_STR "160"

  enum { MAX_DP2_QUERY_ID_LEN = 16 };

  enum {
    VERSION_OFFSET = PREFIX_LEN,
    SEGMENT_OFFSET = VERSION_OFFSET + VERSION_LEN,
    CPU_OFFSET = SEGMENT_OFFSET,
    PIN_OFFSET = CPU_OFFSET + SEGMENT_LEN,
    NEO_CPU_OFFSET = SEGMENT_OFFSET + SEGMENT_LEN,
    NEO_PIN_OFFSET = NEO_CPU_OFFSET + NEO_CPU_LEN,
    STARTTS_OFFSET = PIN_OFFSET + PIN_LEN,
    SESSIONNUM_OFFSET = STARTTS_OFFSET + STARTTS_LEN,
    USERNAMELEN_OFFSET = SESSIONNUM_OFFSET + SESSIONNUM_LEN,
    USERNAME_OFFSET = USERNAMELEN_OFFSET + USERNAMELEN_LEN,
  };

  ComSqlId(CollHeap *heap);

  static int getSqlQueryIdAttr(int attr,          // which attr (SqlQueryIDAttr)
                                 char *queryId,       // query ID
                                 int queryIdLen,    // query ID len.
                                 long &value,        // If returned attr is of string type, this value is the
                                                      // max length of the buffer pointed to by stringValue.
                                                      // If returned attr is numeric, this field contains
                                                      // the returned value.
                                 char *stringValue);  // null terminated returned value for string attrs.

  static int getSqlSessionIdAttr(int attr,           // which attr (SqlQueryIDAttr)
                                   const char *queryId,  // query ID
                                   int queryIdLen,     // query ID len.
                                   long &value,         // If returned attr is of string type, this value is the
                                                         // max length of the buffer pointed to by stringValue.
                                                         // If returned attr is numeric, this field contains
                                                         // the returned value.
                                   char *stringValue);   // null terminated returned value for string attrs.

  static short packNumIntoStr(long num, char *outStr, int outStrLen, NABoolean backwardCompatability = TRUE);

  static long unpackNumFromStr(const char *str, int numChars, NABoolean backwardCompatability = TRUE);

  static int createSqlSessionId(char *sessionId,             // INOUT
                                  int maxSessionIdLen,       // IN
                                  int &actualSessionIdLen,   // OUT
                                  int nodeNumber,            // IN
                                  int cpu,                   // IN
                                  int pin,                   // IN
                                  long startTime,             // IN
                                  long sessionUniqueNum,      // IN
                                  int userNameLen,           // IN
                                  const char *userName,        // IN
                                  int tenantIdLen,           // IN
                                  const char *tenantId,        // IN
                                  int userSessionNameLen,    // IN
                                  const char *userSessionName  // IN
  );

  static int createSqlQueryId(char *queryId,            // INOUT
                                int maxQueryIdLen,      // IN
                                int &actualQueryIdLen,  // OUT
                                int sessionIdLen,       // IN
                                char *sessionId,          // IN
                                long queryUniqueNum,     // IN
                                int queryNameLen,       // IN
                                char *queryName           // IN
  );

  static int extractSqlSessionIdAttrs(const char *sessionId,      // IN
                                        int sessionIdLen,         // IN
                                        long &segmentNumber,       // OUT
                                        long &cpu,                 // OUT
                                        long &pin,                 // OUT
                                        long &processStartTS,      // OUT
                                        long &sessionUniqueNum,    // OUT
                                        int &userNameLen,         // OUT
                                        char *userName,             // OUT
                                        int &tenantIdLen,         // OUT
                                        char *tenantId,             // OUT
                                        int &userSessionNameLen,  // OUT
                                        char *userSessionName,      // OUT
                                        int *version = NULL);

  // returns a compact form of query id which is shipped to dp2.

  static int getDp2QueryIdString(char *queryId, int queryIdLen, char *dp2QueryId, int &dp2QueryIdLen);

  static int decomposeDp2QueryIdString(char *queryId,     // input: buffer containing dp2 query id
                                         int queryIdLen,  // input: length of query id
                                         int *queryNum,   // output: unique query number
                                         int *segment,    // output: segment number of master exe
                                         int *cpu,        // output: cpu number
                                         int *pin,        // output: pin
                                         long *timestamp   // output: master exe process
                                         //         start time
  );

  static UInt64 computeQueryHash(char *input_str);
  static UInt64 computeQueryHash(char *input_str, int len);

 private:
  static int getSqlIdAttr(int attr,           // which attr (SqlQueryIDAttr)
                            const char *queryId,  // query ID
                            int queryIdLen,     // query ID len.
                            long &value,         // If returned attr is of string type, this value is the
                                                  // max length of the buffer pointed to by stringValue.
                                                  // If returned attr is numeric, this field contains
                                                  // the returned value.
                            char *stringValue);   // null terminated returned value for string attrs.

  CollHeap *heap_;
};

#endif
