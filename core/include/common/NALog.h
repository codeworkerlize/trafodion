
#ifndef NALOG_H
#define NALOG_H

#include <stdio.h>

#include <iostream>

#include "NAStdioFile.h"
#include "common/NAString.h"

// ----------------------------------------------------------------------------
// class:  CNALogfile
//
// This class supports a basic logging capability and is a child class of
// CNAStdioFile. Logging:
//   -- Optionally logs the current time for each request
//   -- Optionally logs the process ID for each request
//   -- Optionally flushes the request to disk
//   -- Controls the level of logging
//   -- Either appends to an existing log or clears the log before continuing
//      If the file does not exist, it will be created
//   -- Optionally generates a log file name
// ----------------------------------------------------------------------------
class CNALogfile : public CNAStdioFile {
  typedef CNAStdioFile inherited;

 public:
  // eLEVEL1 is basic logging
  // eLEVEL2 is basic plus detailed logging
  // eLEVEL3 is basic, detailed, plus logging performed only with DEBUG code
  enum ELoggingLevel { eUNKNOWN, eLEVEL1, eLEVEL2, eLEVEL3 };

 private:
  NABoolean m_bPrintTime;
  NABoolean m_bPrintProcessID;
  NABoolean m_bFlushAtLogTime;
  NABoolean m_bPrintLogHeader;
  NABoolean m_bDashToUnderscore;

  ELoggingLevel m_eLoggingLevel;
  NABoolean m_bClearLogAtOpen;
  char *m_pLogName;

  // m_bLoggingEnabled is set when information is successfully extracted from
  // the SQLMX_LOGNAME environment variable and/or when a log has been
  // successfully opened.  Everytime a message is logged, m_bLoggingEnabled
  // must be on
  NABoolean m_bLoggingEnabled;

  // m_bLogFailure is set if a call to either OpenLog or WriteLog to a fails.
  // This signifies that there is a problem with the log and we should not
  // access the file anymore
  NABoolean m_bLogFailure;

  // These are helper methods used to scan the environment variable
  //  SQLMX_UTIL_LOGNAME to get the requested values.
  void ScanEnvVar(char *pEnvVarInfo);
  char *GetNextToken(char *pStrToScan, int &length);

  // This is the helper method to convert any dash in
  // log filename to underscore.
  void ConvertDashToUnderscore(char *pLogName);

 public:
  CNALogfile();
  virtual ~CNALogfile();

  int Open(void);
  int Open(NAString logName, CNAStdioFile::EOpenMode mode);
  int Open(char *pLogName, CNAStdioFile::EOpenMode mode);

  void Close();

  int Log(const char *pBuffer, ELoggingLevel level = CNALogfile::eLEVEL1);
  int Log(const NAString &str, ELoggingLevel level = CNALogfile::eLEVEL1) { return Log(str.data(), level); }

  NABoolean Write(const char *pBuffer, int buflen);
  NABoolean Write(const NAString &str, int buflen) { return Write(str.data(), buflen); }

  NABoolean GetTimePrint() { return m_bPrintTime; }
  void SetTimePrint(NABoolean flag) { m_bPrintTime = flag; }

  NABoolean GetPrintProcessID() { return m_bPrintProcessID; }
  void SetPrintProcessID(NABoolean flag) { m_bPrintProcessID = flag; }

  NABoolean GetFlushRow() { return m_bFlushAtLogTime; }
  void SetFlushRow(NABoolean flag) { m_bFlushAtLogTime = flag; }

  NABoolean IsLoggingEnabled() { return m_bLoggingEnabled; }
  void SetLoggingEnabled(NABoolean flag) { m_bLoggingEnabled = flag; }

  ELoggingLevel GetLoggingLevel() { return m_eLoggingLevel; }
  void SetLoggingLevel(ELoggingLevel loggingLevel) { m_eLoggingLevel = loggingLevel; }

  char *GetLogfileName() { return m_pLogName; }
  void SetLogfileName(char *pLogfileName) { m_pLogName = pLogfileName; }

  void SetClearLogAtOpen(NABoolean openMode) { m_bClearLogAtOpen = openMode; }

  NABoolean GetLogFailure() { return m_bLogFailure; }
  void SetLogFailure(NABoolean logFailure) { m_bLogFailure = logFailure; }

  NABoolean GetPrintLogHeader() { return m_bPrintLogHeader; }
  void SetPrintLogHeader(NABoolean header) { m_bPrintLogHeader = header; }

  NABoolean GetDashToUnderscore() { return m_bDashToUnderscore; }
  void SetDashToUnderscore(NABoolean DashToUnderscore) { m_bDashToUnderscore = DashToUnderscore; }

  NABoolean isHeaderString(const char *strHeader);

  // These methods that format string to log.
  NAString GetTimeToLog();
  NAString GetProcessToLog();
};

#endif  // NALOG_H
