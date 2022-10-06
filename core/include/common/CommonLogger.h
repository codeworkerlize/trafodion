
#ifndef _COMMONLOGGER_H_
#define _COMMONLOGGER_H_

#include <log4cplus/configurator.h>
#include <log4cplus/helpers/loglog.h>
#include <log4cplus/helpers/stringhelper.h>
#include <log4cplus/logger.h>
#include <log4cplus/loggingmacros.h>
#include <log4cplus/loglevel.h>
#include <stdarg.h>
#include <stdio.h>

#include <cstdlib>
#include <iomanip>  //std::setprecision
#include <string>

extern bool gv_commonLoggerInitialized;

using namespace std;
using namespace log4cplus;
using namespace log4cplus::helpers;

/**
 * \file
 * Contains the CommonLogger class and some defines to facilitate logging requests.
 * This file should be included by files that use logging.
 */
// Indicator for logging level.
enum logLevel {
  LL_OFF = log4cplus::OFF_LOG_LEVEL,
  LL_FATAL = log4cplus::FATAL_LOG_LEVEL,
  LL_ERROR = log4cplus::ERROR_LOG_LEVEL,
  LL_WARN = log4cplus::WARN_LOG_LEVEL,
  LL_INFO = log4cplus::INFO_LOG_LEVEL,
  LL_DEBUG = log4cplus::DEBUG_LOG_LEVEL,
  LL_TRACE = log4cplus::TRACE_LOG_LEVEL

  ,
  LL_MVQR_FAIL = 35000
};
/**
 * A generic logger class encapsulating the log4cplus library.
 */
class CommonLogger {
 public:
  /**
   * Creates the single instance of this class, with logging initially enabled.
   * Append mode must be used to prevent separate processes from overwriting
   * each other's logged messages.
   */
  CommonLogger() {}

  virtual ~CommonLogger() {}

  /**
   * Returns a reference to the %QRLogger singelton instance in use.
   * @return Reference to the singleton instance of this class.
   */
  static CommonLogger &instance();

  /**
   * Initializes log4cplus by using the configuration file.
   * If the path given is relative (does not start with a
   * slash), it is appended to the $TRAF_HOME environment variable.
   * @param configFileName name of the log4cplus configuration file.
   * @return FALSE if the configuration file is not found.
   */
  virtual bool initLog4cplus(const char *configFileName, const char *fileSuffix = NULL);

  /**
   * Enters a message in the log. \c logMsgTemplate supplies a
   * printf-style template for constructing the message text, and
   * the arguments used to fill in the placeholders in the template are
   * supplied in a variable argument list.
   * This method can handle messages of arbitrary length.
   *
   * @param[in] cat The logging category to use.
   * @param[in] level The logging priority to use.
   * @param[in] logMsgTemplate The message template.
   * @param[in] ... Variable argument list supplying values to insert in the
   *                message template.
   */

  static void log1(const std::string &cat, logLevel level, const char *cmsg, unsigned int eventId = 0);

  static void log(const std::string &cat, logLevel level, const char *logMsgTemplate...);

  /**
   * Is the category set to log DEBUG messages?
   * This method should be used when the work of producing the logging text
   * is expensive, such as a join graph or an extensive dump.
   * @param cat The name of the category to check.
   * @return TRUE if DEBUG messages are logged.
   */
  static bool isCategoryInDebug(const std::string &cat);

 protected:
  static char *buildMsgBuffer(const std::string &cat, logLevel level, const char *logMsgTemplate, va_list args);

  std::string logFolder_;

 private:
  // Copy constructor and assignment operator are not defined.
  CommonLogger(const CommonLogger &);
  CommonLogger &operator=(const CommonLogger &);
};

#endif /* _COMMONLOGGER_H_ */
