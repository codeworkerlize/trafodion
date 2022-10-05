/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         logmxevent.h
 * RCS:          $Id: logmxevent_sq.h,v 1.1 2007/10/10 06:37:41  Exp $
 * Description:  Eventlogging functions for SQL
 *
 *
 *
 * Created:      02/05/96
 * Modified:     $ $Date: 2007/10/10 06:37:41 $ (GMT)
 * Modified:     $ $Date: 2007/10/10 06:37:41 $ (GMT)
 * Language:     C++
 * Status:       $State: Exp $
 *
 *
 *
 *
 ****************************************************************************/
#ifndef LOGMXEVENT_SQ_H
#define LOGMXEVENT_SQ_H

#include <stdio.h>
#include <pthread.h>
#include "common/Platform.h"
#include "sqevlog/evl_sqlog_writer.h"
#include "SqlEventsDllDefines.h"
#include "common/nawstring.h"
// sealog headers
#define EVENTSAPI
#define SQLEVENT_BUF_SIZE 4024
#ifndef ZMXC_INCLUDED
#include "zmxc"
#define ZMXC_INCLUDED
#endif
#ifdef _MSC_VER
#undef _MSC_VER
#define UNDEFINED_MSC_VER
#endif
#ifndef SP_DIS
#include "common/sql.info_event.pb.h"
#endif
#ifdef UNDEFINED_MSC_VER
#define _MSC_VER 1
#endif

class SQLMXLoggingArea {
 public:
  // the experience level of the SQL/MX release user
  enum ExperienceLevel {
    // beginner should be the smallest experience level
    // and advanced the most advanced. Any new experience
    // level should be between these two values
    eUndefinedEL = -1,
    eBeginnerEL = 1,
    eAdvancedEL = 100
  };

  // the target or consumer of the event
  enum EventTarget { eUndefinedT, eDialOutT, eLogOnlyT, eDBAT };

  static NABoolean establishedAMQPConnection_;
  // A linker error will occur if the constructor or destructor (below) are referenced
  SQLEVENTS_LIB_FUNC SQLMXLoggingArea();   // Should not be referenced
  SQLEVENTS_LIB_FUNC ~SQLMXLoggingArea();  // Should not be referenced
  static pthread_mutex_t loggingMutex_;
  static bool loggingMutexInitialized_;
  SQLEVENTS_LIB_FUNC static void init();

#pragma SRLExportClassMembers ZEVNTSRL *
  enum Category                     // For NT this needs to correspong to
                                    // theLogEvent::Categories enum
                                    // in the file tdm_logevent/tdm_logevent.h.
                                    // For NSK ignore this.
  { SoftwareFailure = SQ_LOG_ERR,   /* 3 */
    NonStopGeneral = SQ_LOG_NOTICE, /* 5 */
    Informational = SQ_LOG_INFO,    /* 6 */
    TraceData = SQ_LOG_DEBUG        /* 7 */

  };

  SQLEVENTS_LIB_FUNC static void logErr97Event(int rc);

  SQLEVENTS_LIB_FUNC static void logSQLMXPredefinedEvent(ULng32 eventId, SQLMXLoggingArea::Category category);

  SQLEVENTS_LIB_FUNC static void setSqlText(const NAWString &x);
  SQLEVENTS_LIB_FUNC static NABoolean establishedAMQPConnection() { return establishedAMQPConnection_; };

  SQLEVENTS_LIB_FUNC static void resetSqlText();
  SQLEVENTS_LIB_FUNC static void logSeaquestInitEvent(char *msg);
  SQLEVENTS_LIB_FUNC static void logSQLMXAbortEvent(const char *filename, int lineno, const char *msg);

  SQLEVENTS_LIB_FUNC static void logSQLMXAssertionFailureEvent(const char *filename, int lineno, const char *msg,
                                                               const char *condition = NULL, const int *tid = NULL);

  // TBD

  SQLEVENTS_LIB_FUNC static void logSortDiskInfo(char *diskname, short percentfreespace, short diskerror);

  SQLEVENTS_LIB_FUNC static int logSQLMXEventForError(
      ULng32 eventId, const char *ExperienceLevel, const char *SeverityLevel, const char *EventTarget,
      const char *msgtxt, const char *sqlId = NULL, const int Int0 = 0, const int Int1 = 0, const int Int2 = 0,
      const int Int3 = 0, const int Int4 = 0, const char *String0 = NULL, const char *String1 = NULL,
      const char *String2 = NULL, const char *String3 = NULL, const char *String4 = NULL, const char *serverName = NULL,
      const char *connectionName = NULL, const char *constraintCatalog = NULL, const char *constraintSchema = NULL,
      const char *constraintName = NULL, const char *triggerCatalog = NULL, const char *triggerSchema = NULL,
      const char *triggerName = NULL, const char *catalogName = NULL, const char *schemaName = NULL,
      const char *tableName = NULL, const char *columnName = NULL, const long currTransid = 0,
      const int rowNumber = 0, const int platformCode = 0, NABoolean isWarning = FALSE);

  // generate an EMS event for executor runtime informational message
  SQLEVENTS_LIB_FUNC static void logExecRtInfo(const char *fileName, ULng32 lineNo, const char *msg,
                                               int explainSeqNum);
  SQLEVENTS_LIB_FUNC static void logSQLMXDebugEvent(const char *msg, short errorcode, bool lock = true);

  SQLEVENTS_LIB_FUNC static void logPOSInfoEvent(const char *msg);

  SQLEVENTS_LIB_FUNC static void logPOSErrorEvent(const int errorCode, const char *msg1, const char *msg2,
                                                  const char *msg3 = NULL);

  // events that correspond to messages generated by CommonLogger or its subclasses.
  SQLEVENTS_LIB_FUNC static void logCommonLoggerInfoEvent(ULng32 eventId, const char *msg);
  SQLEVENTS_LIB_FUNC static void logCommonLoggerErrorEvent(ULng32 eventId, const char *msg);
  SQLEVENTS_LIB_FUNC static void logCommonLoggerFailureEvent(ULng32 eventId, const char *msg);

  // events that correspond to mvqr processes (qms, qmm, qmp) status
  SQLEVENTS_LIB_FUNC static void logMVQRInfoEvent(const char *msg);
  SQLEVENTS_LIB_FUNC static void logMVQRErrorEvent(const char *msg);
  SQLEVENTS_LIB_FUNC static void logMVQRFailureEvent(const char *msg);

  // events that correspond to refresh status in the refresh log
  SQLEVENTS_LIB_FUNC static void logMVRefreshInfoEvent(const char *msg);
  SQLEVENTS_LIB_FUNC static void logMVRefreshErrorEvent(const char *msg);
  SQLEVENTS_LIB_FUNC static void logCompNQCretryEvent(char *stmt);

  SQLEVENTS_LIB_FUNC static void logUtilOperationStatusEvent(ULng32 eventId, const char *utilName, const char *objType,
                                                             const char *objAnsiName, const char *utilStatus);

  SQLEVENTS_LIB_FUNC static void logUtilErrorsEvent(const char *utilName, const int numOfErrors,
                                                    const int errorCode, const char *msg1, const char *msg2 = NULL,
                                                    const char *msg3 = NULL, const char *msg4 = NULL,
                                                    const char *msg5 = NULL);

  SQLEVENTS_LIB_FUNC static void logPMEvent(ULng32 eventId);
  SQLEVENTS_LIB_FUNC static bool lockMutex();
  SQLEVENTS_LIB_FUNC static void unlockMutex();

// These are not currently needed
#if 0
  SQLEVENTS_LIB_FUNC static void logPMOperationStatusEvent
    (ULng32 eventID,
     const char *operation, 
     const char *objType,
     const char *objAnsiName);
  
  SQLEVENTS_LIB_FUNC static void logPMDataCopyStatusEvent
    (ULng32 eventId, 
     TInt64 elapsedTime,
     TInt64 totalCopyCount,
     const char *unitName);

  SQLEVENTS_LIB_FUNC static void logPMEventWithGuardianName
    (ULng32 eventId,
     const char *location);

  SQLEVENTS_LIB_FUNC static void logPMEventWithDumpFileName
    (ULng32 eventId,
     const char *location);

  SQLEVENTS_LIB_FUNC static void logPMEventWithInterval(ULng32 eventId,
							TInt64 interval);

  SQLEVENTS_LIB_FUNC static void logPMErrorsEvent (const char *operation,
						   const int numOfErrors,
						   const int errorCode,
						   const char *msg1,
						   const char *msg2 = NULL,
						   const char *msg3 = NULL,
						   const char *msg4 = NULL,
						   const char *msg5 = NULL
						   );

  SQLEVENTS_LIB_FUNC static void logPMAudInitEvent(ULng32 eventId,
                                                   TInt64 interval);

  SQLEVENTS_LIB_FUNC static void logPMAudStartEvent(ULng32 eventId,
                                                    const short audNum);

  SQLEVENTS_LIB_FUNC static void logPMAudDoneEvent(ULng32 eventId,
                                                   const short audNum,
                                                   TInt64 interval);
#endif  // 0

  SQLEVENTS_LIB_FUNC static void logCliReclaimSpaceEvent(int freeSize, int totalSize, int totalContexts,
                                                         int totalStatements);
};

class SqlSealogEvent {
 public:
  SqlSealogEvent(){};
  ~SqlSealogEvent() { closeConnection(); };
  // open a qpid node port and create an AMQP connection
  short openConnection();
  // set methods
  void setQueryId(char *queryId);

  void setMessageText(char *messageText);
  void setExperienceLevel(const char *el);
  void setTarget(const char *target);
  void setFileName(char *fn);
  void setLineNumber(int ln);
  void setExplainSeqNum(int esn);
  void setError1(int e1);
  void setError2(int e2);
  void setError3(int e3);
  void setInt0(int i0);
  void setInt1(int i1);
  void setInt2(int i2);
  void setInt3(int i3);
  void setInt4(int i4);
  void setString0(char *string0);
  void setString1(char *string1);
  void setString2(char *string2);
  void setString3(char *string3);
  void setString4(char *string4);
  void setInt64_0(long i64_0);
  void setInt64_1(long i64_1);
  void setInt64_2(long i64_2);

  // send AMQP message to sealog
  short sendEvent(Int16 eventId, int slSeverity);
  // close connection
  short closeConnection();

 private:
#ifndef SP_DIS
  sql::info_event sqlInfoEvent_;
#endif
};

#endif
