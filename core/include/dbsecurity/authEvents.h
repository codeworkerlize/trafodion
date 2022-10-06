//******************************************************************************

//******************************************************************************
#ifndef INCLUDE_AUTHEVENT_H
#define INCLUDE_AUTHEVENT_H 1
#include "common/CommonLogger.h"
#include <string>
#include <vector>

// From a web search, the log message max length is a bit over 8000 bytes.
// For DBSecurity don't believe any messages will be more than 1M
#define MAX_EVENT_MSG_SIZE 1024

// Different outcomes can be returned when authenticating the user.
// AUTH_OUTCOME is a status for each of these outcomes.
// getAuthOutcome translates the status into text form.
enum AUTH_OUTCOME {
  AUTH_OK = 0,
  AUTH_REJECTED = 1,
  AUTH_FAILED = 2,
  AUTH_NOT_REGISTERED = 3,
  AUTH_MD_NOT_AVAILABLE = 4,
  AUTH_USER_INVALID = 5,
  AUTH_TYPE_INCORRECT = 6,
  AUTH_NO_PASSWORD = 7,
  AUTH_TENANT_INVALID = 8,
  AUTH_USER_TENANT = 9,
  AUTH_NO_TENANT = 10,
  AUTH_USER_REGISTERED = 11,
  AUTH_PASSWORD_WILL_EXPIRE = 12,
  AUTH_PASSWORD_EXPIRE = 13,
  AUTH_PASSWORD_ERROR_COUNT = 14,
  AUTH_PASSWORD_GRACE_COUNT = 15,
  AUTH_NO_LICENSE = 16
};

std::string getAuthOutcome(AUTH_OUTCOME outcome, int32_t error);

enum DB_SECURITY_EVENTID {
  DBS_GENERIC_ERROR = 1,
  DBS_NO_LDAP_SEARCH_CONNECTION = 2,
  DBS_LDAP_SEARCH_ERROR = 3,
  DBS_NO_LDAP_AUTH_CONNECTION = 4,
  DBS_UNKNOWN_AUTH_STATUS_ERROR = 5,
  DBS_AUTH_RETRY_SEARCH = 6,
  DBS_AUTH_RETRY_BIND = 7,
  DBS_AUTH_CONFIG = 8,
  DBS_AUTHENTICATION_ATTEMPT = 9,
  DBS_AUTH_CONFIG_SECTION_NOT_FOUND = 10,
  DBS_LDAP_GROUP_SEARCH_ERROR = 11,
  DBS_LDAP_NOT_ENABLED = 12,
  DBS_DEBUGMSG = 50
};

// The ported code had the caller sending in the filename and line number
// for certain events.  This has not been implemented at this time.
class AuthEvent {
 private:
  DB_SECURITY_EVENTID eventID_;
  std::string eventText_;
  logLevel severity_;
  std::string filename_;
  int32_t lineNumber_;
  std::string callerName_;

 public:
  AuthEvent() : eventID_(DBS_GENERIC_ERROR), severity_(LL_INFO), lineNumber_(0), callerName_("??") {}

  AuthEvent(DB_SECURITY_EVENTID eventID, std::string eventText, logLevel severity)
      : eventID_(eventID), eventText_(eventText), severity_(severity), lineNumber_(0), callerName_("??") {}

  DB_SECURITY_EVENTID getEventID() { return eventID_; }
  logLevel getSeverity() { return severity_; }
  int32_t getLineNum() { return lineNumber_; }
  std::string getEventText() { return eventText_; }
  std::string getFilename() { return filename_; }
  std::string getCallerName() { return callerName_; }

  void setCallerName(std::string callerName) { callerName_ = callerName; }
  void setLineNumber(int32_t lineNumber) { lineNumber_ = lineNumber; }
  void setFilename(std::string filename) { filename_ = filename; }

  static std::string formatEventText(const char *eventText);

  void logAuthEvent();
};

void authInitEventLog();
void insertAuthEvent(std::vector<AuthEvent> &authEvents, DB_SECURITY_EVENTID eventID, const char *eventText,
                     logLevel severity);
bool authEventExists(std::vector<AuthEvent> &authEvents, DB_SECURITY_EVENTID eventID);

#endif
