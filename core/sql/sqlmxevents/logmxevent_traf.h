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
* File:         logmxevent_traf.h
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
#ifndef LOGMXEVENT_TRAF_H
#define LOGMXEVENT_TRAF_H

#include <stdio.h>
#include <pthread.h>
#include "Platform.h"
#include "sq_sql_eventids.h"
#include "nawstring.h"

#define EVENTSAPI
#define SQLEVENT_BUF_SIZE 4024
#define STACK_TRACE_SIZE  8192
#define EXCEPTION_MSG_SIZE 256
#define EXCEPTION_CONDITION_SIZE 128


#ifdef _MSC_VER
#undef _MSC_VER
#define UNDEFINED_MSC_VER
#endif

#ifdef UNDEFINED_MSC_VER
#define _MSC_VER 1
#endif 

#include "QRLogger.h"
#include "ComDiags.h"

class SQLMXLoggingArea
{
 public:
  
  // the experience level of the SQL/MX release user
  enum ExperienceLevel
  {
    // beginner should be the smallest experience level
    // and advanced the most advanced. Any new experience 
    // level should be between these two values
    eUndefinedEL = -1,
    eBeginnerEL = 1,
    eAdvancedEL = 100
  };

  // the target or consumer of the event
  enum EventTarget
  {
    eUndefinedT,
    eDialOutT,
    eLogOnlyT,
    eDBAT
  };
	  
  static NABoolean establishedAMQPConnection_;	 
  // A linker error will occur if the constructor or destructor (below) are referenced 
  SQLMXLoggingArea() ;  // Should not be referenced
  ~SQLMXLoggingArea() ; // Should not be referenced
  static pthread_mutex_t loggingMutex_;
  static pthread_mutexattr_t loggingMutexAttr_;
  static bool loggingMutexInitialized_;
  static void init() ;
 

  enum Category   // For NT this needs to correspong to 
  // theLogEvent::Categories enum
  // in the file tdm_logevent/tdm_logevent.h. 
  // For NSK ignore this.
  {				
    SoftwareFailure=0,	     	 
    General,	     	
    Informational,        
    TraceData     	 
    // Severity from sealog header file (foll values) : 
    // SQ_LOG_CRIT
    // SQ_LOG_ALERT
    // SQ_LOG_ERR
    // SQ_LOG_INFO
  };

  static void logErr97Event(int rc);  

  static void logSQLMXPredefinedEvent(const char *msg, logLevel level);
  
  static void setSqlText(const NAWString& x);
  static NABoolean establishedAMQPConnection() {return establishedAMQPConnection_;}  ;

  static void resetSqlText();
  static void logSeaquestInitEvent(char* msg);
  static void logSQLMXAbortEvent(
				 const char* filename, 
				 Int32 lineno, 
				 const char* msg
				 );
  
  static void logSQLMXAssertionFailureEvent(
					    const char* filename, 
					    Int32 lineno, 
					    const char* msg,
					    const char* condition = NULL,
					    const Lng32* tid = NULL,
					    const char* stackTrace = NULL);
 
  //TBD

  static void logSortDiskInfo(
			      const char *diskname,
			      short percentfreespace,
			      short diskerror);		 
 
                                                       

  static void logSQLMXEventForError(
              ULng32 eventId,
			  const char *msgtxt,
			  const char* sqlId = NULL,
			  NABoolean isWarning = FALSE
			  );

  
  // generate an EMS event for executor runtime informational message
  static void logExecRtInfo(const char *fileName,
			    ULng32 lineNo,
			    const char *msg, 
			    Lng32 explainSeqNum);

  // generate an EMS event for executor runtime debug message
  static void logExecRtDebug(const char *fileName,
                             ULng32 lineNo,
                             const char *msg, 
                             Lng32 explainSeqNum);

  // generate an event for privilege manager tree traversal
  static void logPrivMgrInfo(const char *filename,
                             ULng32 lineNo,
                             const char *msg,
                             Int32 level);

  static void logSQLMXDebugEvent(const char *msg, short errorcode, Int32 lineno);
  // events that correspond to messages generated by CommonLogger or its subclasses.
  static void logCommonLoggerInfoEvent(ULng32 eventId,
				       const char *msg);
  static void logCommonLoggerErrorEvent(ULng32 eventId,
					const char *msg);
  static void logCommonLoggerFailureEvent(ULng32 eventId,
					  const char *msg);

  // events that correspond to refresh status in the refresh log
  static void logMVRefreshInfoEvent(const char *msg);
  static void logMVRefreshErrorEvent(const char *msg);
  static void logCompNQCretryEvent(const char *stmt);

   
  static bool lockMutex();
  static void unlockMutex();
  static void logCliReclaimSpaceEvent(Lng32 freeSize, 
				      Lng32 totalSize, Lng32 totalContexts,
				      Lng32 totalStatements);
																									 
};

void logAnMXEventForError( ComCondition & condition, SQLMXLoggingArea::ExperienceLevel emsEventEL);


#endif
