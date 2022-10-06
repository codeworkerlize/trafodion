
/* -*-C++-*- ***************************************************************
 *
 * File:         ComSqlId.cpp
 * Description:
 *
 * Created:      10/31/2006
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "common/ComSqlId.h"
#include "exp/ExpError.h"
#include "common/str.h"
#include <fstream>
#include <unistd.h>
#include <math.h>

ComSqlId::ComSqlId(CollHeap *heap) : heap_(heap) {}

// num:       input number to be packed in str
// outStr:    output buffer where str will be returned.
// outStrLen: length of outStr. Must include one byte for null terminator.
//            Number of converted chars is outStrLen - 1
// backwardCompatability:
//           if backward compatability is to be maintained to differentiate
//           between a previously packed str, then set the 2nd bit from
//           left (0x40) of the leftmost byte of outStr.
short ComSqlId::packNumIntoStr(long num, char *outStr, int outStrLen, NABoolean backwardCompatibility) {
  // max number of packed chars is 10. Could be increased at some point.
  int outNumChars = outStrLen - 1;
  if ((outNumChars <= 0) || (outNumChars > 10)) return -1;

  // if provided num is less than max base-10 number, then use base10 to pack
  NABoolean useBase10 = FALSE;
  long maxNumValForBase10 = pow(10, outNumChars);
  if (num < maxNumValForBase10) useBase10 = TRUE;

  // convert number to str using characters from ascii code 33(!) to 126(~)
  int startBase = (useBase10 ? 48 /*'0'*/ : 33 /*'!'*/);
  int endBase = (useBase10 ? 57 /*'9'*/ : 126 /*'~'*/);
  char startBaseChar = (char)startBase;
  char endBaseChar = (char)endBase;
  int base = endBase - startBase + 1;

  // Backward compatibility support will reduce the max num that can be packed.
  // This check is not needed if base10 is being used.
  long maxNum = 0;
  if (NOT backwardCompatibility)
    maxNum = pow(base, outNumChars);
  else  // support backward compatibility
  {
    if (useBase10)
      maxNum = maxNumValForBase10;
    else {
      // exclude 2 leftmost bits for leftmost byte.
      // max num for leftmost byte is 63-33+1 to keep in printable range.
      maxNum = pow(base, (outNumChars - 1)) * 31 /*63 - 33 + 1*/;
    }
  }
  if (num > maxNum) return -1;

  int tnum = num;
  outStr[outNumChars] = 0;
  for (int i = 0; i < outNumChars; i++) {
    int rem = tnum % base;
    int qot = tnum / base;
    tnum = qot;
    char c = (rem == 0 ? startBaseChar : startBaseChar + rem);
    outStr[outNumChars - i - 1] = c;
  }

  // set 2nd bit from left of the leftmost char;
  // That indicates that this is the new format.
  if ((NOT useBase10) && (backwardCompatibility)) outStr[0] |= 0x40;

  return 0;
}

// inStr:     input str to be unpacked
// inStrLen:  length of input string. Null terminator not included.
// Return:    unpacked number.  or -1 if an error.
long ComSqlId::unpackNumFromStr(const char *inStr, int inStrLen, NABoolean backwardCompatibility) {
  if ((!inStr) || (inStrLen <= 0) || (inStrLen > 10)) return -1;

  NABoolean useBase10 = FALSE;
  if ((backwardCompatibility) && ((inStr[0] & 0x40) == 0)) {
    useBase10 = TRUE;
  }

  // convert number str using characters from ascii code 33(!) to 126(~)
  int startBase = (useBase10 ? 48 /*'0'*/ : 33 /*'!'*/);
  int endBase = (useBase10 ? 57 /*'9'*/ : 126 /*'~'*/);
  char startBaseChar = (char)startBase;
  char endBaseChar = (char)endBase;
  int base = endBase - startBase + 1;

  int tnum = 0;
  for (int j = 0; j < inStrLen; j++) {
    char c = inStr[j];
    if ((backwardCompatibility) && (j == 0)) {
      // zero out the left 2 bits to get the actual value.
      c &= 0x3F;
    }
    tnum = tnum * base + (c - startBaseChar);
  }

  return tnum;
}

int ComSqlId::createSqlSessionId(char *sessionId,             // INOUT
                                   int maxSessionIdLen,       // IN
                                   int &actualSessionIdLen,   // OUT
                                   int nodeNumber,            // IN
                                   int cpu,                   // IN
                                   int pin,                   // IN
                                   long processStartTS,        // IN
                                   long sessionUniqueNum,      // IN
                                   int userNameLen,           // IN
                                   const char *userName,        // IN
                                   int tenantIdLen,           // IN
                                   const char *tenantId,        // IN
                                   int userSessionNameLen,    // IN
                                   const char *userSessionName  // IN
) {
  // if input buffer space is less then the needed space,
  // return error.
  if (maxSessionIdLen < MAX_SESSION_ID_LEN) return -CLI_INVALID_ATTR_VALUE;

  char nidStr[4];
  if (packNumIntoStr(nodeNumber, nidStr, 4)) return -CLI_INVALID_ATTR_VALUE;

  // The format of current session id is:
  //  MXID<version><segment><cpu><pin><processStartTS><sessNum><unLen><userName><snLen><sessionName>
  str_sprintf(sessionId, "%s%02d%3s%06d%018ld%010d%02d%s%02d%s%02d%s", COM_SESSION_ID_PREFIX, SQ_SQL_ID_VERSION, nidStr,
              pin,                      // 6 digits
              processStartTS,           // 18 digits
              (int)sessionUniqueNum,  // 10 digits
              userNameLen,              // 2 digits
              userName,                 //
              tenantIdLen,              // 2 digits
              tenantId,                 //
              userSessionNameLen,       // 2 digits
              userSessionName);

  actualSessionIdLen = (int)strlen(sessionId);

  return 0;
}

int ComSqlId::createSqlQueryId(char *queryId,            // INOUT
                                 int maxQueryIdLen,      // IN
                                 int &actualQueryIdLen,  // OUT
                                 int sessionIdLen,       // IN
                                 char *sessionId,          // IN
                                 long queryUniqueNum,     // IN
                                 int queryNameLen,       // IN
                                 char *queryName           // IN
) {
  // if input buffer space is less then the needed space,
  // return error.
  if (maxQueryIdLen < MAX_QUERY_ID_LEN) return -CLI_INVALID_ATTR_VALUE;

  // The format of current query id is:
  //  <session_id>_<unique_number>_<identifier_or_handle>
  // <unique_number> is max 18 digits. Stored value is the actual
  // number of digits delimited by underscores.
  // Ex, queryNum of 12 will be store as _12_
  //
  str_sprintf(queryId, "%s_%ld_", sessionId, queryUniqueNum);

  int queryIdLen = (int)strlen(queryId);
  // check queryName length
  if (queryNameLen > maxQueryIdLen - queryIdLen - 1) queryNameLen = maxQueryIdLen - queryIdLen - 1;

  // copy queryName to queryId
  strncpy(&queryId[queryIdLen], queryName, queryNameLen);

  actualQueryIdLen = queryIdLen + queryNameLen;
  queryId[actualQueryIdLen] = 0;

  return 0;
}

int ComSqlId::getSqlIdAttr(int attr,           // which attr (SqlQueryIDAttr)
                             const char *queryId,  // query ID
                             int queryIdLen,     // query ID len
                             long &value,         // If returned attr is numeric,
                                                   //    this field contains the returned value.
                                                   // If attr is of string type, this value is:
                                                   //   on input,the max length of the buffer pointed to by stringValue
                                                   //   on output, the actual length of the string attribute
                             char *stringValue)    // null terminated string returned here
{
  int retcode = 0;

  long version = str_atoi(&queryId[VERSION_OFFSET], VERSION_LEN);

  switch (attr) {
    case SQLQUERYID_VERSION:
      value = version;
      if (value < 0) return -CLI_INVALID_ATTR_VALUE;
      break;
    case SQLQUERYID_SEGMENTNUM: {
      value = unpackNumFromStr(&queryId[SEGMENT_OFFSET], SEGMENT_LEN);
      if (value < 0) return -CLI_INVALID_ATTR_VALUE;
    } break;

    case SQLQUERYID_SEGMENTNAME: {
      // not implemented yet
      if (stringValue && value) stringValue[0] = 0;
    } break;

    case SQLQUERYID_CPUNUM: {
      // on SQ, the SEGMENT_NUM and CPUNUM is the same field - 3 digits
      if (version == NEO_SQL_ID_VERSION)
        value = str_atoi(&queryId[NEO_CPU_OFFSET], NEO_CPU_LEN);
      else
        value = str_atoi(&queryId[CPU_OFFSET], CPU_LEN);
      if (value < 0) return -CLI_INVALID_ATTR_VALUE;
    } break;

    case SQLQUERYID_PIN: {
      if (version == NEO_SQL_ID_VERSION)
        value = str_atoi(&queryId[NEO_PIN_OFFSET], NEO_PIN_LEN);
      else
        value = str_atoi(&queryId[PIN_OFFSET], PIN_LEN);
      if (value < 0) return -CLI_INVALID_ATTR_VALUE;
    } break;

    case SQLQUERYID_EXESTARTTIME: {
      value = str_atoi(&queryId[STARTTS_OFFSET], STARTTS_LEN);
      if (value < 0) return -CLI_INVALID_ATTR_VALUE;
    } break;

    case SQLQUERYID_SESSIONNUM: {
      value = str_atoi(&queryId[SESSIONNUM_OFFSET], SESSIONNUM_LEN);
      if (value < 0) return -CLI_INVALID_ATTR_VALUE;
    } break;

    case SQLQUERYID_USERNAME: {
      int userNameLen = (int)str_atoi(&queryId[USERNAMELEN_OFFSET], USERNAMELEN_LEN);
      if (userNameLen < 0) return -CLI_INVALID_ATTR_VALUE;
      if (stringValue && value > userNameLen) {
        strncpy(stringValue, &queryId[USERNAME_OFFSET], userNameLen);
        stringValue[userNameLen] = 0;
      } else
        retcode = -CLI_INVALID_ATTR_VALUE;

      value = userNameLen;
    } break;

    case SQLQUERYID_TENANTID: {
      int userNameLen = (int)str_atoi(&queryId[USERNAMELEN_OFFSET], USERNAMELEN_LEN);
      if (userNameLen < 0) return -CLI_INVALID_ATTR_VALUE;

      int tenantIdLenOffset = USERNAMELEN_OFFSET + USERNAMELEN_LEN + userNameLen;
      int tenantIdLen = (int)str_atoi(&queryId[tenantIdLenOffset], TENANTIDLEN_LEN);
      if (tenantIdLen < 0) return -CLI_INVALID_ATTR_VALUE;
      if (stringValue && value > tenantIdLen) {
        strncpy(stringValue, &queryId[tenantIdLenOffset + TENANTIDLEN_LEN], tenantIdLen);

        stringValue[tenantIdLen] = 0;
      } else
        retcode = -CLI_INVALID_ATTR_VALUE;

      value = tenantIdLen;
    } break;

    case SQLQUERYID_SESSIONNAME: {
      int currOffset = USERNAMELEN_OFFSET;
      int userIdLen = (int)str_atoi(&queryId[currOffset], USERNAMELEN_LEN);
      if (userIdLen < 0) return -CLI_INVALID_ATTR_VALUE;
      // go past userid
      currOffset += USERNAMELEN_LEN + userIdLen;
      // get tenant id length
      int tenantIdLen = (int)str_atoi(&queryId[currOffset], TENANTIDLEN_LEN);
      if (tenantIdLen < 0) return CLI_INVALID_ATTR_VALUE;
      // go past tenant id
      currOffset += TENANTIDLEN_LEN + tenantIdLen;

      int sessionNameLenOffset = currOffset;

      int sessionNameLen = (int)str_atoi(&queryId[sessionNameLenOffset], SESSIONNAMELEN_LEN);
      if (sessionNameLen < 0) return -CLI_INVALID_ATTR_VALUE;
      if (stringValue && value > sessionNameLen) {
        strncpy(stringValue, &queryId[sessionNameLenOffset + SESSIONNAMELEN_LEN], sessionNameLen);

        stringValue[sessionNameLen] = 0;
      } else
        retcode = -CLI_INVALID_ATTR_VALUE;

      value = sessionNameLen;
    } break;

    case SQLQUERYID_SESSIONID: {
      // session Id = qid from start thru SessionName

      // set current offset to userNameLen
      int currOffset = USERNAMELEN_OFFSET;

      // get userNameLen
      int nameLen = (int)str_atoi(&queryId[currOffset], USERNAMELEN_LEN);

      // go past Username
      currOffset += USERNAMELEN_LEN + nameLen;

      // get tenant id length
      int tenantIdLen = (int)str_atoi(&queryId[currOffset], TENANTIDLEN_LEN);
      if (tenantIdLen < 0) return CLI_INVALID_ATTR_VALUE;
      // go past tenant id
      currOffset += TENANTIDLEN_LEN + tenantIdLen;

      // get sessionName length
      nameLen = (int)str_atoi(&queryId[currOffset], SESSIONNAMELEN_LEN);
      if (nameLen < 0) return -CLI_INVALID_ATTR_VALUE;

      // go past sessionName
      currOffset += SESSIONNAMELEN_LEN + nameLen;

      // check value for length
      if (stringValue && value > currOffset) {
        // copy sessionId to stringValue
        strncpy(stringValue, queryId, currOffset);
        stringValue[currOffset] = 0;
      } else
        retcode = -CLI_INVALID_ATTR_VALUE;

      value = currOffset;
    } break;

    case SQLQUERYID_QUERYNUM: {
      int currOffset = USERNAMELEN_OFFSET;
      int nameLen = (int)str_atoi(&queryId[currOffset], USERNAMELEN_LEN);
      if (nameLen < 0) return -CLI_INVALID_ATTR_VALUE;

      // skip user name
      currOffset += USERNAMELEN_LEN + nameLen;

      // get tenant id length
      int tenantIdLen = (int)str_atoi(&queryId[currOffset], TENANTIDLEN_LEN);
      if (tenantIdLen < 0) return CLI_INVALID_ATTR_VALUE;
      // go past tenant id
      currOffset += TENANTIDLEN_LEN + tenantIdLen;

      // skip session name
      nameLen = (int)str_atoi(&queryId[currOffset], SESSIONNAMELEN_LEN);
      if (nameLen < 0) return -CLI_INVALID_ATTR_VALUE;
      currOffset += SESSIONNAMELEN_LEN + nameLen;

      if (currOffset >= queryIdLen) return -CLI_INVALID_ATTR_VALUE;

      if (queryId[currOffset] != '_') return -CLI_INVALID_ATTR_VALUE;

      // skip "_" separator
      currOffset += 1;

      // find the next occurance of "_"
      char *us = str_chr(&queryId[currOffset], '_');
      if (us == NULL) return -CLI_INVALID_ATTR_VALUE;

      if ((currOffset + (us - (char *)(&queryId[currOffset]))) >= queryIdLen) return -CLI_INVALID_ATTR_VALUE;

      value = str_atoi(&queryId[currOffset], us - (char *)(&queryId[currOffset]));
      if (value < 0) return -CLI_INVALID_ATTR_VALUE;
    } break;

    case SQLQUERYID_STMTNAME: {
      int currOffset = USERNAMELEN_OFFSET;
      int nameLen = (int)str_atoi(&queryId[currOffset], USERNAMELEN_LEN);
      if (nameLen < 0) return -CLI_INVALID_ATTR_VALUE;

      // skip user name
      currOffset += USERNAMELEN_LEN + nameLen;

      // get tenant id length
      int tenantIdLen = (int)str_atoi(&queryId[currOffset], TENANTIDLEN_LEN);
      if (tenantIdLen < 0) return CLI_INVALID_ATTR_VALUE;
      // go past tenant id
      currOffset += TENANTIDLEN_LEN + tenantIdLen;

      // skip session name
      nameLen = (int)str_atoi(&queryId[currOffset], SESSIONNAMELEN_LEN);
      if (nameLen < 0) return -CLI_INVALID_ATTR_VALUE;
      currOffset += SESSIONNAMELEN_LEN + nameLen;

      if (currOffset >= queryIdLen) return -CLI_INVALID_ATTR_VALUE;

      if (queryId[currOffset] != '_') return -CLI_INVALID_ATTR_VALUE;

      // skip "_" separator
      currOffset += 1;

      // find the next occurance of "_"
      char *us = str_chr(&queryId[currOffset], '_');
      if (us == NULL) return -CLI_INVALID_ATTR_VALUE;

      // skip queryNum
      currOffset += us - (char *)(&queryId[currOffset]);

      if (currOffset >= queryIdLen) return -CLI_INVALID_ATTR_VALUE;

      // skip "_" separator
      currOffset += 1;

      // get statementName length as remaining string except trailing blanks
      while (queryIdLen && queryId[queryIdLen - 1] == ' ') queryIdLen--;

      int qidStatementNameLen = queryIdLen - currOffset;

      // check for enough space in StringValue
      if (stringValue && value > qidStatementNameLen) {
        // copy and null terminate
        strncpy(stringValue, &queryId[currOffset], qidStatementNameLen);
        stringValue[qidStatementNameLen] = 0;
      } else
        retcode = -CLI_INVALID_ATTR_VALUE;

      value = qidStatementNameLen;

    } break;

    default: {
      retcode = -CLI_INVALID_ATTR_NAME;
    }
  }

  return retcode;
}

int ComSqlId::getSqlQueryIdAttr(int attr,         // which attr (SqlQueryIDAttr)
                                  char *queryId,      // query ID
                                  int queryIdLen,   // query ID len
                                  long &value,       // If returned attr is of string type, this value is the
                                                      // max length of the buffer pointed to by stringValue.
                                                      // If returned attr is numeric, this field contains
                                                      // the returned value.
                                  char *stringValue)  // null terminated returned value for string attrs.
{
  int retcode = 0;

  if ((queryId == NULL) || (queryIdLen < MIN_QUERY_ID_LEN) ||
      (strncmp(queryId, COM_SESSION_ID_PREFIX, strlen(COM_SESSION_ID_PREFIX)) != 0))
    return -CLI_INVALID_ATTR_VALUE;

  retcode = getSqlIdAttr(attr, queryId, queryIdLen, value, stringValue);
  return retcode;
}

int ComSqlId::getSqlSessionIdAttr(int attr,           // which attr (SqlQueryIDAttr)
                                    const char *queryId,  // query ID
                                    int queryIdLen,     // query ID len
                                    long &value,         // If returned attr is of string type, this value is the
                                                          // max length of the buffer pointed to by stringValue.
                                                          // If returned attr is numeric, this field contains
                                                          // the returned value.
                                    char *stringValue)    // null terminated returned value for string attrs.
{
  int retcode = 0;

  if ((queryId == NULL) || (queryIdLen < MIN_SESSION_ID_LEN) ||
      (strncmp(queryId, COM_SESSION_ID_PREFIX, strlen(COM_SESSION_ID_PREFIX)) != 0))
    return -CLI_INVALID_ATTR_VALUE;

  retcode = getSqlIdAttr(attr, queryId, queryIdLen, value, stringValue);
  return retcode;
}

int ComSqlId::extractSqlSessionIdAttrs(const char *sessionId,      // IN
                                         int maxSessionIdLen,      // IN
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
                                         int *version) {
  int retcode = 0;
  long lc_version;

  if ((sessionId == NULL) || ((maxSessionIdLen > 0) && (maxSessionIdLen < MIN_SESSION_ID_LEN)) ||
      (strncmp(sessionId, COM_SESSION_ID_PREFIX, strlen(COM_SESSION_ID_PREFIX)) != 0))
    return -1;

  retcode = getSqlIdAttr(SQLQUERYID_SEGMENTNUM, sessionId, maxSessionIdLen, segmentNumber, NULL);
  if (retcode) return retcode;

  retcode = getSqlIdAttr(SQLQUERYID_CPUNUM, sessionId, maxSessionIdLen, cpu, NULL);
  if (retcode) return retcode;

  retcode = getSqlIdAttr(SQLQUERYID_PIN, sessionId, maxSessionIdLen, pin, NULL);
  if (retcode) return retcode;

  retcode = getSqlIdAttr(SQLQUERYID_EXESTARTTIME, sessionId, maxSessionIdLen, processStartTS, NULL);
  if (retcode) return retcode;

  retcode = getSqlIdAttr(SQLQUERYID_SESSIONNUM, sessionId, maxSessionIdLen, sessionUniqueNum, NULL);
  if (retcode) return retcode;

  if (userName) {
    long dummy = 0;
    retcode = getSqlIdAttr(SQLQUERYID_USERNAME, sessionId, userNameLen, dummy, userName);
    if (retcode) return retcode;
  }

  if (tenantId) {
    long dummy = 0;
    retcode = getSqlIdAttr(SQLQUERYID_TENANTID, sessionId, tenantIdLen, dummy, tenantId);
    if (retcode) return retcode;
  }
  if (userSessionName) {
    long dummy = 0;
    retcode = getSqlIdAttr(SQLQUERYID_SESSIONNAME, sessionId, userSessionNameLen, dummy, userSessionName);
    if (retcode) return retcode;
  }
  if (version != NULL) {
    retcode = getSqlIdAttr(SQLQUERYID_VERSION, sessionId, maxSessionIdLen, lc_version, NULL);
    if (retcode) return retcode;
    *version = (int)lc_version;
  }
  return 0;
}

/*
  DP2 Query Id Format:
    Each of the part is in binary numeric format.

  <segment><cpu><pin><processStartTS><queryNum>

  <segment>        :      1 byte
  <cpu>            :      1 byte
  <pin>            :      2 bytes(short)
  <processStartTS> :      8 bytes (long)
  <queryNum>       :      4 bytes (int)
*/
int ComSqlId::getDp2QueryIdString(char *queryId,        // IN
                                    int queryIdLen,     // IN
                                    char *dp2QueryId,     // INOUT
                                    int &dp2QueryIdLen  // OUT
) {
  if ((!queryId) || (queryIdLen < MAX_DP2_QUERY_ID_LEN)) return -1;

  long value;
  int currOffset = 0;
  // In case of Linux and NT, segment num and cpu num are same
  // Copy them as short
  // get cpu
  getSqlQueryIdAttr(SQLQUERYID_CPUNUM, queryId, queryIdLen, value, NULL);
  *(short *)&dp2QueryId[currOffset] = (short)value;
  currOffset += sizeof(short);

  // get pin
  getSqlQueryIdAttr(SQLQUERYID_PIN, queryId, queryIdLen, value, NULL);
  *(short *)&dp2QueryId[currOffset] = (short)value;
  currOffset += sizeof(short);

  // get process start time
  getSqlQueryIdAttr(SQLQUERYID_EXESTARTTIME, queryId, queryIdLen, value, NULL);
  str_cpy_all(&dp2QueryId[currOffset], (char *)&value, sizeof(long));
  currOffset += sizeof(long);

  // get query num
  getSqlQueryIdAttr(SQLQUERYID_QUERYNUM, queryId, queryIdLen, value, NULL);
  int qNum = (int)value;
  str_cpy_all(&dp2QueryId[currOffset], (char *)&qNum, sizeof(int));

  currOffset += sizeof(int);

  dp2QueryIdLen = currOffset;

  return 0;
}

int ComSqlId::decomposeDp2QueryIdString(char *queryId,     // input: buffer containing dp2 query id
                                          int queryIdLen,  // input: length of query id
                                          int *queryNum,   // output: unique query number
                                          int *segment,    // output: segment number of master exe
                                          int *cpu,        // output: cpu number
                                          int *pin,        // output: pin
                                          long *timestamp   // output: master exe process
                                                             //         start time
) {
  if ((!queryId) || (queryIdLen < MAX_DP2_QUERY_ID_LEN)) return -1;

  int currOffset = 0;
  *segment = *(short *)&queryId[currOffset];
  *cpu = *segment;
  currOffset += sizeof(short);

  *pin = *(short *)&queryId[currOffset];
  currOffset += sizeof(short);

  str_cpy_all((char *)timestamp, &queryId[currOffset], sizeof(long));
  currOffset += sizeof(long);

  str_cpy_all((char *)queryNum, &queryId[currOffset], sizeof(int));
  currOffset += sizeof(int);

  return 0;
}

UInt64 ComSqlId::computeQueryHash(char *input_str) { return ComSqlId::computeQueryHash(input_str, strlen(input_str)); }

UInt64 ComSqlId::computeQueryHash(char *input_str, int len) {
  /*
  char buf[128];
  sprintf(buf, "/tmp/dop.rtstats.info.%d", getpid());

  fstream fout(buf, std::fstream::out | std::fstream::app);
  fout << "ComSqlId::computeQueryHash()" << endl;
  fout << "query:" << input_str << endl;
  fout << "original len:" << len << endl;
  */

  // ignore any trailing ';' characters
  while (len >= 0 && input_str[len - 1] == ';') {
    len--;
  }

  // fout << "adjusted len:" << len << endl;

  // The following implemnents FNV-1a hash described at
  // https://en.wikipedia.org/wiki/Fowler–Noll–Vo_hash_function.
  const UInt64 prime = 1099511628211;
  UInt64 hash = 0xcbf29ce484222325;
  for (int i = 0; i < len; i++) {
    hash ^= input_str[i];
    hash *= prime;
  }

  /*
  sprintf(buf, "%Lu", hash);
  fout << "hash:" << buf<< endl << endl;
  fout.close();
  */

  return hash;
}
