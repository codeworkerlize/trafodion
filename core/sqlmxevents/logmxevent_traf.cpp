
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         logmxevent_traf.cpp
 * Description:  Eventlogging functions for SQL
 *
 * Created:      02/05/96
 * Language:     C++
 *
 *
 *
 *
 ****************************************************************************/

#include <cxxabi.h>
#include <execinfo.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdlib.h>

#include "common/NLSConversion.h"
#include "common/str.h"
#include "sqlmxevents/logmxevent.h"

#ifdef _MSC_VER
#undef _MSC_VER
#endif

#include "seabed/fs.h"
//#include "qmscommon/QRLogger.h"

// forward declaration
static void check_assert_bug_catcher();

pthread_mutex_t SQLMXLoggingArea::loggingMutex_;
pthread_mutexattr_t SQLMXLoggingArea::loggingMutexAttr_;
bool SQLMXLoggingArea::loggingMutexInitialized_ = false;

void SQLMXLoggingArea::init() {
  char buffer[80];
  int rc;

  if (!loggingMutexInitialized_) {
    rc = pthread_mutexattr_init(&loggingMutexAttr_);
    if (rc != 0) {
      sprintf(buffer, "SQLMXLoggingArea::init() pthread_mutexattr_init() rc=%d", rc);
      abort();
    }

    rc = pthread_mutexattr_settype(&loggingMutexAttr_, PTHREAD_MUTEX_ERRORCHECK);
    if (rc != 0) {
      sprintf(buffer, "SQLMXLoggingArea::init() pthread_mutexattr_settype() rc=%d", rc);
      abort();
    }

    rc = pthread_mutex_init(&loggingMutex_, &loggingMutexAttr_);
    if (rc != 0) {
      sprintf(buffer, "SQLMXLoggingArea::init() pthread_mutex_init() rc=%d", rc);
      abort();
    }

    loggingMutexInitialized_ = true;
  }
}

bool SQLMXLoggingArea::lockMutex() {
  char buffer[80];
  int rc = 0;
  if (loggingMutexInitialized_) {
    rc = pthread_mutex_lock(&loggingMutex_);
    if (rc == EDEADLK) {
      printf("Deadlock happens in class SQLMXLoggingArea\n");
      return false;
    }

    if (rc) {
      sprintf(buffer, "SQLMXLoggingArea::lockMutex() pthread_mutex_lock() rc=%d", rc);
      abort();
    }
  }
  return rc ? false : true;
}

void SQLMXLoggingArea::unlockMutex() {
  char buffer[80];
  int rc = 0;
  if (loggingMutexInitialized_) rc = pthread_mutex_unlock(&loggingMutex_);
  if (rc) {
    sprintf(buffer, "SQLMXLoggingArea::unlockMutex() pthread_mutex_unlock() rc=%d", rc);
    abort();
  }
}

SQLMXLoggingArea::~SQLMXLoggingArea(){

};

/*
 * the format to all the log entries in the log4cxx logs is as follows:

 * GenerationTS  Severity  Component  NodeNumber CPU PIN  ProcessName   SQLCODE  QID MessageTxt
 * Example:
 * 2014-10-29 21:25:06,880, INFO, SQL.COMP, Node Number: 0, CPU: 0, PIN: 14321, Process Name: $Z000BP6, SQLCODE: 4082,
 QID: MXID11...X, *** ERROR[4082] Object T ...
 * The main caller of this method is function logAnMXEventForError in cli/CLIExtern.cpp. This is the where cli
 intercepts requests to return the diag to log
 * any errors or warnings in the diag.
 */
void SQLMXLoggingArea::logSQLMXEventForError(int sqlcode, const char *msgTxt, const char *sqlId, NABoolean isWarning) {
  if (!lockMutex()) {
    printf("%s\n", msgTxt);
    return;
  }

  logLevel level = isWarning ? LL_INFO : LL_ERROR;

  try {
    QRLogger::log(QRLogger::instance().getMyDefaultCat(), level, sqlcode, sqlId, "%s", msgTxt);
  } catch (...) {
    // log exception need not to throw
    printf("%s\n", msgTxt);
  }

  unlockMutex();
}

void SQLMXLoggingArea::logCompNQCretryEvent(const char *stmt) {
  if (!lockMutex()) {
    printf("Statement was compiled as if query plan caching were off: %s\n", stmt);
    return;
  }

  try {
    QRLogger::log(QRLogger::instance().getMyDefaultCat(), LL_WARN,
                  "Statement was compiled as if query plan caching were off: %s", stmt);
  } catch (...) {
    // log exception need not to throw
    printf("Statement was compiled as if query plan caching were off: %s\n", stmt);
  }

  unlockMutex();
}

void SQLMXLoggingArea::logExecRtInfo(const char *fileName, int lineNo, const char *msgTxt, int explainSeqNum) {
  if (!lockMutex()) {
    printf("%s  Explain Sequence Number: %d FILE: %s LINE: %d\n", msgTxt, explainSeqNum, fileName, lineNo);
    return;
  }

  try {
    QRLogger::log(QRLogger::instance().getMyDefaultCat(), LL_INFO, "%s  Explain Sequence Number: %d FILE: %s LINE: %d",
                  msgTxt, explainSeqNum, fileName, lineNo);
  } catch (...) {
    // log exception need not to throw
    printf("%s  Explain Sequence Number: %d FILE: %s LINE: %d\n", msgTxt, explainSeqNum, fileName, lineNo);
  }

  unlockMutex();
}

void SQLMXLoggingArea::logExecRtDebug(const char *fileName, int lineNo, const char *msgTxt, int explainSeqNum) {
  if (!lockMutex()) {
    printf("%s  Explain Sequence Number: %d FILE: %s LINE: %d\n", msgTxt, explainSeqNum, fileName, lineNo);
    return;
  }

  try {
    QRLogger::log(QRLogger::instance().getMyDefaultCat(), LL_DEBUG, "%s  Explain Sequence Number: %d FILE: %s LINE: %d",
                  msgTxt, explainSeqNum, fileName, lineNo);
  } catch (...) {
    // log exception need not to throw
    printf("%s  Explain Sequence Number: %d FILE: %s LINE: %d\n", msgTxt, explainSeqNum, fileName, lineNo);
  }

  unlockMutex();
}

// ----------------------------------------------------------------------------
// Method: logPrivMgrInfo
//
// When privMgr debugging is set, logs information
// ----------------------------------------------------------------------------
void SQLMXLoggingArea::logPrivMgrInfo(const char *filename, int lineNo, const char *msg, int level) {
  if (!lockMutex()) {
    printf("%s\n", msg);
    return;
  }

  try {
    QRLogger::log(QRLogger::instance().getMyDefaultCat(), LL_DEBUG, "%s ", msg);
  } catch (...) {
    // log exception need not to throw
    printf("%s\n", msg);
  }

  unlockMutex();
}

int writeStackTrace(char *s, int bufLen) {
  void *bt[20];
  char **strings;
  size_t funcsize = 256;
  size_t newfuncsize = funcsize;
  const int safetyMargin = 128;
  size_t size = backtrace(bt, 20);
  strings = backtrace_symbols(bt, size);

  if (bufLen <= safetyMargin) return 0;

  // Note this string should fit within safetyMargin size.
  int len = sprintf(s, "Process Stack Trace:\n");

  // allocate string which will be filled with the demangled function name
  // malloc needed since demangle function uses realloc.

  char *funcname = (char *)malloc(funcsize);

  // iterate over the returned symbol lines. skip the first, it is the
  // address of this function.
  for (int i = 1; i < size; i++) {
    char *begin_name = 0, *begin_offset = 0, *end_offset = 0;

    // find parentheses and +address offset surrounding the mangled name:
    // ./module(function+0x15c) [0x8048a6d]
    for (char *p = strings[i]; *p; ++p) {
      if (*p == '(')
        begin_name = p;
      else if (*p == '+')
        begin_offset = p;
      else if (*p == ')' && begin_offset) {
        end_offset = p;
        break;
      }
    }

    if (begin_name && begin_offset && end_offset && begin_name < begin_offset) {
      *begin_name++ = '\0';
      *begin_offset++ = '\0';
      *end_offset = '\0';

      int status;
      char *ret = abi::__cxa_demangle(begin_name, funcname, &newfuncsize, &status);
      if (status == 0) {
        funcname = ret;
        if ((len + strlen(strings[i]) + newfuncsize + safetyMargin) < bufLen) {
          len += sprintf(s + len, "%s  : %s+%s\n", strings[i], funcname, begin_offset);
        } else {
          break;  // bufLen is short. Break out.
        }

      } else {
        // demangling failed. just store begin name and offset as is.
        if ((len + strlen(strings[i]) + strlen(begin_name) + safetyMargin) < bufLen) {
          len += sprintf(s + len, "  %s : %s+%s\n", strings[i], begin_name, begin_offset);
        } else {
          break;  // bufLen is short. Break out.
        }
      }
    } else {
      // couldn't parse the line. Do nothing. Move to next line.
      if ((len + strlen(strings[i]) + safetyMargin) < bufLen) {
        len += sprintf(s + len, "  %s\n", strings[i]);
      } else {
        break;  // bufLen is short. Break out.
      }
    }
  }

  free(funcname);
  free(strings);
  len += sprintf(s + len, "\n");  // safetyMargin assumed.
  return len;
}

void SQLMXLoggingArea::logErr97Event(int rc) {
#if 0
// to be completed, need event id 516 
  const int LEN=8192;
  if (rc == 97) {
    char msg[LEN];
    writeStackTrace(msg, LEN);
    logSQLMXEventForError(SQLMX_ERR97_OCCURED, msg);
  }
#endif
}

void SQLMXLoggingArea::logSQLMXPredefinedEvent(const char *msg, logLevel level) {
  if (!lockMutex()) {
    printf("%s\n", msg);
    return;
  }

  try {
    QRLogger::log(QRLogger::instance().getMyDefaultCat(), level, "%s ", msg);
  } catch (...) {
    // log exception need not to throw
    printf("%s\n", msg);
  }

  unlockMutex();
}

void SQLMXLoggingArea::logSQLMXDebugEvent(const char *msg, short errorcode, int line) {
  if (!lockMutex()) {
    printf("%s ERRORCODE:%d LINE:%d\n", msg, errorcode, line);
    return;
  }

  try {
    QRLogger::log(QRLogger::instance().getMyDefaultCat(), LL_DEBUG, "%s ERRORCODE:%d LINE:%d ", msg, errorcode, line);
  } catch (...) {
    // log exception need not to throw
    printf("%s ERRORCODE:%d LINE:%d\n", msg, errorcode, line);
  }

  unlockMutex();
}

// log an ABORT event
void SQLMXLoggingArea::logSQLMXAbortEvent(const char *file, int line, const char *msgTxt) {
  const char *file_name = "";
  if (file) {
    file_name = file;
  }

  if (!lockMutex()) {
    printf("%s in FILE: %s LINE: %d\n", msgTxt, file_name, line);
    return;
  }

  try {
    QRLogger::log(QRLogger::instance().getMyDefaultCat(), LL_FATAL, "%s in FILE: %s LINE: %d ", msgTxt, file_name,
                  line);
  } catch (...) {
    // log exception need not to throw
    printf("%s in FILE: %s LINE: %d\n", msgTxt, file_name, line);
  }

  unlockMutex();
}

// log an ASSERTION FAILURE event
void SQLMXLoggingArea::logSQLMXAssertionFailureEvent(const char *file, int line, const char *msgTxt,
                                                     const char *condition, const int *tid, const char *stackTrace) {
  int LEN = SQLEVENT_BUF_SIZE + STACK_TRACE_SIZE;
  char msg[LEN];
  memset(msg, 0, LEN);

  int sLen = str_len(msgTxt);
  int sTotalLen = sLen;
  str_cpy_all(msg, msgTxt, sLen);

  char fileLineStr[200];
  if (file) {
    str_sprintf(fileLineStr, ", FILE: %s, LINE: %d ", file, line);
    sLen = str_len(fileLineStr);
    str_cpy_all(msg + sTotalLen, fileLineStr, sLen);
    sTotalLen += sLen;
  }

  if (tid && (*tid != -1)) {
    char transId[100];
    str_sprintf(transId, " TRANSACTION ID: %d ", *tid);
    sLen = str_len(transId);
    str_cpy_all(msg + sTotalLen, transId, sLen);
    sTotalLen += sLen;
  }

  if (condition) {
    char condStr[100];
    str_sprintf(condStr, " CONDITION: %s ", condition);
    sLen = str_len(condStr);
    str_cpy_all(msg + sTotalLen, condStr, sLen);
    sTotalLen += sLen;
  }

  if (stackTrace) {
    str_ncpy(msg + sTotalLen, stackTrace, LEN - sTotalLen);
  } else {
    if ((LEN - sTotalLen) > 512) writeStackTrace(msg + sTotalLen, LEN - sTotalLen);
  }

  if (!lockMutex()) {
    printf("%s\n", msg);
    return;
  }

  try {
    QRLogger::log(QRLogger::instance().getMyDefaultCat(), LL_FATAL, "%s", msg);
  } catch (...) {
    // log exception need not to throw
    printf("%s\n", msg);
  }

  unlockMutex();
}

void SQLMXLoggingArea::logMVRefreshInfoEvent(const char *msg) {
  if (!lockMutex()) {
    printf("%s\n", msg);
    return;
  }

  try {
    QRLogger::log(CAT_SQL_COMP_MV_REFRESH, LL_INFO, "%s", msg);
  } catch (...) {
    // log exception need not to throw
    printf("%s\n", msg);
  }

  unlockMutex();
}

void SQLMXLoggingArea::logMVRefreshErrorEvent(const char *msg) {
  if (!lockMutex()) {
    printf("%s\n", msg);
    return;
  }

  try {
    QRLogger::log(CAT_SQL_COMP_MV_REFRESH, LL_ERROR, "%s", msg);
  } catch (...) {
    // log exception need not to throw
    printf("%s\n", msg);
  }

  unlockMutex();
}

void SQLMXLoggingArea::logCliReclaimSpaceEvent(int freeSize, int totalSize, int totalContexts, int totalStatements) {
  int LEN = 8192;
  char msg[8192];
  memset(msg, 0, LEN);

  str_sprintf(msg, "CLI reclaim space event. Free Size: %d, Total Size: %d, Total Contexts: %d, Total Statements: %d",
              freeSize, totalSize, totalContexts, totalStatements);

  if (!lockMutex()) {
    printf("%s\n", msg);
    return;
  }

  try {
    QRLogger::log(QRLogger::instance().getMyDefaultCat(), LL_INFO, msg);
  } catch (...) {
    // log exception need not to throw
    printf("%s\n", msg);
  }

  unlockMutex();
}

void SQLMXLoggingArea::logSortDiskInfo(const char *diskname, short percentfree, short diskerror) {
  //
  // TBD or rewrite needed
  //** use event id SQEV_SQL_SRT_INFO **
}
