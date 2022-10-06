
#include "common/CommonLogger.h"

#include <errno.h>
#include <log4cplus/configurator.h>
#include <log4cplus/helpers/loglog.h>
#include <log4cplus/helpers/stringhelper.h>
#include <log4cplus/logger.h>
#include <log4cplus/loggingmacros.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <iomanip>  //std::setprecision
#include <stdexcept>
#include <string>
#define SLASH_S "/"
#define SLASH_C '/'

using namespace std;
using namespace log4cplus;
using namespace log4cplus::helpers;

bool gv_plusLoggerInitialized = false;

// **************************************************************************
// Reads configuration file, creates an appender, layout, and categories.
// Attaches layout to the appender and appender to categories.
// Will be called by constructor qms, qmp, qmm, and mxcmp after calling
// setLogFileName().
// Return TRUE if all was well, or FALSE if the config file was not read
// successfully.
// **************************************************************************
bool CommonLogger::initLog4cplus(const char *configFileName, const char *fileSuffix) {
  std::string configPath;

  logFolder_ = "";

  // Form the config path $TRAF_CONF
  char *installPath = getenv("TRAF_CONF");
  if (installPath) {
    configPath = installPath;
    // Last char may or may not be a slash.
    if (configPath[configPath.length() - 1] != SLASH_C) configPath += SLASH_C;  // LCOV_EXCL_LINE :nu
  } else {
    // Environment variable $TRAF_CONF not found.
    // use the current directory.
    configPath = "";
  }

  if (configFileName[0] == SLASH_C)
    configPath = configFileName;
  else
    configPath += configFileName;

  try {
    if (fileSuffix && fileSuffix[0] != '\0') {
      setenv("TRAFODION_LOG_FILENAME_SUFFIX", fileSuffix, 1);
    } else {
      char tmp[100];
      const char *host = getenv("HOSTNAME");
      sprintf(tmp, ".%s.log", host);
      setenv("TRAFODION_LOG_FILENAME_SUFFIX", tmp, 1);
    }

    log4cplus::initialize();
    PropertyConfigurator::doConfigure(configPath.data());
    gv_plusLoggerInitialized = true;
    return true;

  }
  // catch (log4cxx::ConfigureFailure e)
  catch (std::exception e) {
    return false;
  }
}

CommonLogger &CommonLogger::instance() {
  static CommonLogger onlyInstance_;
  return onlyInstance_;
}

// **************************************************************************
// Is the category set to log DEBUG messages?
// This method should be used when the work of producing the logging text
// is expensive, such as a join graph or an extensive dump.
// **************************************************************************
bool CommonLogger::isCategoryInDebug(const std::string &cat) {
  return log4cplus::Logger::getInstance(cat).isEnabledFor(LL_DEBUG);
}

// **************************************************************************
// The generic message logging method for any message type and length.
// **************************************************************************
void CommonLogger::log1(const std::string &cat, logLevel level, const char *cmsg, unsigned int eventId) {
  // Have to use a std::string for the message. If a char* is passed, the
  // overload of the various logging functions (debug(), fatal(), etc.) that
  // accepts a char* is used, and they all use the char* as a format string.
  // This causes any contained % character to be treated as the beginning of
  // a format specifier, with ugly consequences.
  std::string msg(cmsg);

  eventId = eventId;  // touch

  switch (level) {
    case LL_FATAL:
      LOG4CPLUS_FATAL(log4cplus::Logger::getInstance(cat), msg);
      break;

    case LL_ERROR:
      LOG4CPLUS_ERROR(log4cplus::Logger::getInstance(cat), msg);
      break;

    case LL_MVQR_FAIL:
      LOG4CPLUS_ERROR(log4cplus::Logger::getInstance(cat), msg);
      break;

    case LL_WARN:
      LOG4CPLUS_WARN(log4cplus::Logger::getInstance(cat), msg);
      break;

    case LL_INFO:
      LOG4CPLUS_INFO(log4cplus::Logger::getInstance(cat), msg);
      break;

    case LL_DEBUG:
      LOG4CPLUS_DEBUG(log4cplus::Logger::getInstance(cat), msg);
      break;

    case LL_TRACE:
      LOG4CPLUS_TRACE(log4cplus::Logger::getInstance(cat), msg);
      break;
    case LL_OFF:
      break;
  }
}

char *CommonLogger::buildMsgBuffer(const std::string &cat, logLevel level, const char *logMsgTemplate, va_list args) {
  // calculate needed bytes to allocate memory from system heap.
  int bufferSize = 20049;
  int msgSize = 0;
  static __thread char *buffer = NULL;
  va_list args2;
  va_copy(args2, args);
  bool secondTry = false;
  level = level;  // touch
  if (buffer == NULL) buffer = new char[bufferSize];

  // For messages shorter than the initial 20K limit, a single pass through
  // the loop will suffice.
  // For longer messages, 2 passes will do it. The 2nd pass uses a buffer of the
  // size returned by the call to vsnprintf. If a second pass is necessary, we
  // pass the copy of the va_list we made above, because the first call changes
  // the state of the va_list, and can cause a crash if reused.
  while (1) {
    msgSize = vsnprintf(buffer, bufferSize, logMsgTemplate, secondTry ? args2 : args);

    // if the initial size of 20K works, then break out from while loop.
    if (msgSize < 0) {
// LCOV_EXCL_START :rfi
// Some error happened.
// Report the first 40 chars.
#define SHORTSIZE 40
      char smallBuff[SHORTSIZE + 5];
      memset(smallBuff, 0, SHORTSIZE + 5);
      if (strlen(logMsgTemplate) <= SHORTSIZE)
        strcpy(smallBuff, logMsgTemplate);
      else {
        memcpy(smallBuff, logMsgTemplate, SHORTSIZE);
        strcpy(smallBuff + SHORTSIZE, "...");
      }

      char errorMsg[100];
      sprintf(errorMsg, "Cannot log full message, error %d returned from vsnprintf()", msgSize);
      LOG4CPLUS_ERROR(log4cplus::Logger::getInstance(cat), errorMsg);
      LOG4CPLUS_ERROR(log4cplus::Logger::getInstance(cat), smallBuff);

      return buffer;

    } else if (msgSize < bufferSize) {
      // Buffer is large enough - we're good.
      break;
    } else {
      // Buffer is too small, reallocate it and go through the loop again.
      bufferSize = msgSize + 1;  // Use correct size now.
      delete[] buffer;
      buffer = new char[bufferSize];
      secondTry = true;
    }
  }

  va_end(args2);
  return buffer;
}
//
// **************************************************************************
// The generic message logging method for any message type and length.
// **************************************************************************
void CommonLogger::log(const std::string &cat, logLevel level, const char *logMsgTemplate...) {
  // Don't do anything if this message will be ignored anyway.
  log4cplus::Logger myLogger = log4cplus::Logger::getInstance(cat);
  if (myLogger.getLogLevel() > level) {
    return;
  }

  va_list args;
  va_start(args, logMsgTemplate);

  char *buffer = buildMsgBuffer(cat, level, logMsgTemplate, args);
  log1(cat, level, buffer);

  va_end(args);
}
