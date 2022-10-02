// **********************************************************************
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
// **********************************************************************

#include <log4cplus/logger.h>
#include <log4cplus/fileappender.h>
#include <log4cplus/layout.h>
#include <log4cplus/loglevel.h>
#include <log4cplus/configurator.h>
#include <log4cplus/helpers/loglog.h>
#include <log4cplus/helpers/stringhelper.h>
#include <log4cplus/loggingmacros.h>

#include <QRLogger.h>

#include "ComDiags.h"
#include "logmxevent.h"
#include "NLSConversion.h"
#include "Globals.h"

#include "PortProcessCalls.h"
#include "seabed/ms.h"
#include "seabed/fserr.h"
#include <sys/syscall.h>

BOOL gv_QRLoggerInitialized_ = FALSE;
QRLogger *gv_QRLoggerInstance_ = NULL;

using namespace log4cplus;
using namespace log4cplus::helpers;

// SQL categories
std::string CAT_SQL                           = "SQL";
std::string CAT_SQL_EXE                       = "SQL.EXE";
std::string CAT_SQL_COMP                      = "SQL.COMP";
std::string CAT_SQL_ESP                       = "SQL.ESP";
std::string CAT_SQL_LOB                       = "SQL.LOB";
std::string CAT_SQL_SSMP                      = "SQL.SSMP";
std::string CAT_SQL_SSCP                      = "SQL.SSCP";
std::string CAT_SQL_UDR                       = "SQL.UDR";
std::string CAT_SQL_PRIVMGR                   = "SQL.PRIVMGR";
std::string CAT_SQL_USTAT                     = "SQL.USTAT";
std::string CAT_SQL_SHARED_CACHE              = "SQL.SharedCache";
std::string CAT_SQL_STMT                      = "SQL.STMT";
std::string CAT_SQL_LOCK                      = "SQL.LOCK"; // Object lock
std::string CAT_SQL_CI                        = "SQL.CI";   // SQLCI
// hdfs
std::string CAT_SQL_HDFS_JNI_TOP              =  "SQL.HDFS.JniTop";
std::string CAT_SQL_HDFS_SEQ_FILE_READER      =  "SQL.HDFS.SeqFileReader";
std::string CAT_SQL_HDFS_SEQ_FILE_WRITER      =  "SQL.HDFS.SeqFileWriter";
std::string CAT_SQL_HDFS_ORC_FILE_READER      =  "SQL.HDFS.OrcFileReader";
std::string CAT_SQL_HDFS_ORC_FILE_WRITER      =  "SQL.HDFS.OrcFileWriter";
std::string CAT_SQL_HDFS_EXT_SCAN_TCB         =  "SQL.HDFS.ExtStorageScanTcb";
std::string CAT_SQL_EX_SPLIT_BOTTOM           =  "SQL.HDFS.SplitBottom";
std::string CAT_SQL_EX_ONLJ                   =  "SQL.HDFS.NestedJoin";
std::string CAT_SQL_HBASE                     =  "SQL.HBase";
std::string CAT_SQL_HDFS                      =  "SQL.HDFS";

// minmax
std::string CAT_SQL_EXE_MINMAX                =  "SQL.EXE.MINMAX";

// these categories are currently not used 
std::string CAT_SQL_QMP                       = "SQL.Qmp";
std::string CAT_SQL_QMM                       = "SQL.Qmm";
std::string CAT_SQL_COMP_QR_DESC_GEN          = "SQL.COMP"; // ".DescGen";
std::string CAT_SQL_COMP_QR_HANDLER           = "SQL.COMP"; // ".QRHandler";
std::string CAT_SQL_COMP_QR_COMMON            = "SQL.COMP"; // ".QRCommon";
std::string CAT_SQL_COMP_QR_IPC               = "SQL.COMP"; // ".QRCommon.IPC";
std::string CAT_SQL_COMP_MV_REFRESH           = "SQL.COMP"; // ".MV.REFRESH";
std::string CAT_SQL_COMP_MVCAND               = "SQL.COMP"; // ".MVCandidates";
std::string CAT_SQL_MEMORY                    = "SQL.COMP"; // ".Memory";
std::string CAT_SQL_COMP_RANGE                = "SQL.COMP"; // ".Range";
std::string CAT_QR_TRACER                     = "QRCommon.Tracer";
std::string CAT_SQL_QMS                       = "SQL.Qms";
std::string CAT_SQL_QMS_MAIN                  = "SQL.Qms.Main";
std::string CAT_SQL_QMS_INIT                  = "SQL.Qms.Init";
std::string CAT_SQL_MVMEMO_JOINGRAPH          = "SQL.Qms.MvmemoJoingraph";
std::string CAT_SQL_MVMEMO_STATS              = "SQL.Qms.MvmemoStats";
std::string CAT_SQL_QMS_GRP_LATTCE_INDX       = "SQL.Qms.LatticeIndex";
std::string CAT_SQL_QMS_MATCHTST_MVDETAILS    = "SQL.Qms.MatchTest";
std::string CAT_SQL_QMS_XML                   = "SQL.Qms.XML";
std::string CAT_SQL_COMP_XML                  = "SQL.COMP"; // ".XML";

// **************************************************************************
// **************************************************************************
QRLogger::QRLogger()
  : CommonLogger()
   ,module_(QRL_NONE)
   ,processInfo_("")
{
}

// **************************************************************************
// **************************************************************************
QRLogger& QRLogger::instance()
{
  if (gv_QRLoggerInstance_ == NULL)
     gv_QRLoggerInstance_ = new QRLogger();
  return *gv_QRLoggerInstance_;
}


// **************************************************************************
// construct a string with the node number and PIN of the ancestor 
// executor process and the string ".log". This will be used as the suffix
// of the log name. So if the log name specified by the user in the 
// configuration file is "sql_events", the actual log name will be
// something like "sql_events_0_12345.log"
// **************************************************************************
void getMyTopAncestor(char ancsNidPid[])
{
  short result = 0;

  NAProcessHandle myPhandle;
  myPhandle.getmine();
  myPhandle.decompose();
  char processName[MS_MON_MAX_PROCESS_NAME + 1];
  char ppName[MS_MON_MAX_PROCESS_NAME + 1];

  memset(processName, 0, sizeof(processName));
  memcpy(processName, myPhandle.getPhandleString(), myPhandle.getPhandleStringLen());

  memset(ppName, 0, sizeof(ppName));

  MS_Mon_Process_Info_Type procInfo;
  Int32 rc = 0;

  Int32 ancsNid = 0;
  Int32 ancsPid = 0;
  do
  {
     rc = msg_mon_get_process_info_detail(processName, &procInfo);
     ancsNid = procInfo.nid;
     ancsPid = procInfo.pid;
     strcpy(ppName, procInfo.parent_name);
     strcpy(processName, ppName);
  } while ((rc == XZFIL_ERR_OK) && (procInfo.parent_nid != -1) && (procInfo.parent_pid != -1));

  snprintf (ancsNidPid, 5+2*sizeof(Int32), "_%d_%d.log", ancsNid, ancsPid);
}
// **************************************************************************
// construct a string with the node number of the 
//  process and the string ".log". This will be used as the suffix
// of the log name forthis process . So if the log name specified by the 
// user in the 
// configuration file is "sql_events", the actual log name will be
// something like "sql_events_<nid>.log"
// **************************************************************************
void getMyNidSuffix(char stringNidSuffix[])
{
  short result = 0;

  NAProcessHandle myPhandle;
  myPhandle.getmine();
  myPhandle.decompose();
  
  Int32 myNid = myPhandle.getCpu();

  snprintf (stringNidSuffix, 5+sizeof(Int32), "_%d.log", myNid);
}
// **************************************************************************
// Call the superclass to configure log4cplus from the config file.
// If the configuration file is not found, perform default initialization:
// Create an appender, layout, and categories.
// Attaches layout to the appender and appender to categories.
// **************************************************************************
NABoolean QRLogger::initLog4cplus(const char* configFileName)
{
  NAString logFileName;

  if (gv_QRLoggerInitialized_)
     return TRUE;
  
  // Log4cplus logging
  if (CommonLogger::initLog4cplus(configFileName))
    {
      introduceSelf();
      gv_QRLoggerInitialized_ = TRUE;
      return TRUE;
    }
  return FALSE;
}


std::string &QRLogger::getMyDefaultCat()
{
  switch (module_)
    {
    case QRL_MXCMP:
      return CAT_SQL_COMP;
      break;
    case QRL_MXEXE:
      return CAT_SQL_EXE;
      break;
    case QRL_ESP:
      return CAT_SQL_ESP;
      break;
    case QRL_LOB:
      return CAT_SQL_LOB;
      break;
    case QRL_SSMP:
      return CAT_SQL_SSMP;
      break;
    case QRL_SSCP:
      return CAT_SQL_SSCP;
      break;
    case QRL_UDR:
      return CAT_SQL_UDR;
      break;
    default:
      return CAT_SQL;   
    }

}

const char*  QRLogger::getMyProcessInfo()
{
  char procInfo[300];
  SB_Phandle_Type myphandle;
  XPROCESSHANDLE_GETMINE_(&myphandle);
  char charProcHandle[200];
  char myprocname[30];
  Int32 mycpu,mypin,mynodenumber=0;
  short myproclength = 0;
  XPROCESSHANDLE_DECOMPOSE_(&myphandle, &mycpu, &mypin, &mynodenumber,NULL,100, NULL, myprocname,100, &myproclength);
  myprocname[myproclength] = '\0';

  snprintf(procInfo, 300, "%s NID %d PID %d TID %ld txID ", myprocname, mycpu, mypin, syscall(SYS_gettid));

  processInfo_ = procInfo;
  return processInfo_.data();
}

void QRLogger::introduceSelf ()
{
  char msg[300];
  std::string &myCat = getMyDefaultCat(); 
  // save previous default priority
  LogLevel defaultLevel = log4cplus::Logger::getInstance(myCat).getLogLevel();
  
  NAString procInfo = getMyProcessInfo();

  // change default priority to INFO to be able to printout some informational messages
  log4cplus::Logger myLogger(log4cplus::Logger::getInstance(myCat));
  myLogger.setLogLevel(LL_INFO);
 
  switch (module_)
    {
    case QRL_NONE:
    case QRL_MXCMP:
      snprintf (msg, 200, "%s,,, A compiler process is launched.", procInfo.data());
      break;
 
    case QRL_ESP:
      snprintf (msg, 300, "%s,,, An ESP process is launched.", procInfo.data());
      break;
 
    case QRL_MXEXE:
      snprintf (msg, 300, "%s,,, An executor process is launched.", procInfo.data());
      break;
    case QRL_LOB:
      snprintf (msg, 300, "%s,,, A lobserver  process is launched.", procInfo.data());
      break;
    case QRL_SSMP:
      snprintf (msg, 300, "%s,,, An ssmp  process is launched.", procInfo.data());
      break;
    case QRL_SSCP:
      snprintf (msg, 300, "%s,,, An sscp  process is launched.", procInfo.data());
      break;
   case QRL_UDR:
      snprintf (msg, 300, "%s,,, A udrserver  process is launched.", procInfo.data());
      break;
    }

    LOG4CPLUS_INFO(myLogger,msg);

   // restore default priority
   myLogger.setLogLevel(defaultLevel);
}


// **************************************************************************
// Log an exception-type error message:
// First log "Error thrown at line <line> in file <filename>:"
// Then log the actual error message, which is passed with a variable 
// number of parameters. The error message is epected to be a single line 
// of text not exceeding 2K.
// This error message can be logged as either ERROR or FATAL type.
// Both lines of text are sent to the log file
// MVQR failures that prevent only rewrite, and not execution of the original
// form of the query, cause an informational rather than an error notification
// to be entered in the system log.
// **************************************************************************
void QRLogger::logError(const char* file, 
                            Int32       line, 
                            std::string &cat,
                            logLevel    level,
                            const char* logMsgTemplate...)
{
  #define BUFFER_SIZE 2048

  char buffer[BUFFER_SIZE]; // This method expects a single line to text.
  NAString logData = QRLogger::instance().getMyProcessInfo();

  if (level == LL_MVQR_FAIL)
    snprintf(buffer, sizeof(buffer), "%s,,, Attempted MVQR operation (rewrite of query or publication of MV) failed at line %d in file %s:", logData.data(), line, file);
  else
    snprintf(buffer, sizeof(buffer), "%s,,, Error thrown at line %d in file %s:", logData.data(), line, file);

  va_list args;
  va_start(args, logMsgTemplate);

  if (level == LL_FATAL)
  {
    LOG4CPLUS_FATAL(log4cplus::Logger::getInstance(cat), buffer);
  }
  else
  {
    LOG4CPLUS_ERROR(log4cplus::Logger::getInstance(cat), buffer);
  }

  // format actual error message
  vsnprintf(buffer, BUFFER_SIZE, logMsgTemplate, args);

  NAString logData2 = QRLogger::instance().getMyProcessInfo();
  logData2 += ",,, ";
  logData2 += buffer;

  // log actual error msg to logfile.
  if (level == LL_FATAL)
  {
    LOG4CPLUS_FATAL(log4cplus::Logger::getInstance(cat), logData2.data());
  }
  else
  {
    LOG4CPLUS_ERROR(log4cplus::Logger::getInstance(cat), logData2.data());
  }

  va_end(args);
}

// This temporary function is used for logging QVP messages only. More extensive
// changes to the CommonLogger hierarchy will be made when the QI integration
// stream is merged into the mainline. For now, we avoid making changes that
// affect MVQR to minimize merge difficulties.
void QRLogger::logQVP(ULng32 eventId,
                          std::string &cat,
                          logLevel    level,
                          const char* logMsgTemplate...)
{
  // Don't do anything if this message will be ignored anyway.
  log4cplus::Logger myLogger = log4cplus::Logger::getInstance(cat);
  logLevel myLevel = (logLevel)myLogger.getLogLevel();
  if ( myLevel == LL_OFF )
      return;
  // If the configured logging level is greater (more restrictive) than
  // the requested level, don't log.
  if ( myLevel > level)
      return;

  va_list args;
  va_start(args, logMsgTemplate);

  char* buffer = buildMsgBuffer(cat, level, logMsgTemplate, args);
  log1(cat, level, buffer, eventId);

  va_end(args);
}


void QRLogger::logDiags(ComDiagsArea* diagsArea, std::string &cat)
{
  const NAWchar* diagMsg;
  NAString* diagStr;
  Int32 i;
  ComCondition* condition;
  
  log4cplus::Logger myLogger = log4cplus::Logger::getInstance(cat);
  logLevel myLevel = (logLevel)myLogger.getLogLevel();
  // If configured Level is the same or less restrictive than WARN (30000)
  // than report the warning
  if ( myLevel != LL_OFF && myLevel <= LL_WARN )
  {
    for (i=1; i<=diagsArea->getNumber(DgSqlCode::WARNING_); i++)
    {
      condition = diagsArea->getWarningEntry(i);
      diagMsg = condition->getMessageText();
      diagStr = unicodeToChar(diagMsg, NAWstrlen(diagMsg),
                                      CharInfo::ISO88591, NULL, TRUE);
      log(cat, LL_WARN, condition->getSQLCODE(), "",
        "  warn condition %d: %s", i, diagStr->data());
      delete diagStr;
    }
  }
}

// **************************************************************************
// **************************************************************************
CommonTracer::CommonTracer(const char*   fnName,
                           CommonLogger& logger,
                           std::string &logCategory,
                           TraceLevel    level,
                           const char*   file,
                           Int32         line)
  : fnName_(fnName),
    level_(level),
    file_(file ? file : ""),
    line_(line),
    logger_(logger),
    category_(logCategory)
{
  if (level_ == TL_all)
  {
    if (file_.length() == 0)
      logger_.log(category_, LL_DEBUG,
                  "Entering %s", fnName_.data());
    else
      logger_.log(category_, LL_DEBUG,
                  "Entering %s (file %s, line %d)",
                  fnName_.data(), file_.data(), line_);
  }
}

// **************************************************************************
// **************************************************************************
CommonTracer::~CommonTracer()
{
  NAString logMsg;

  if (level_ >= TL_exceptionOnly && std::uncaught_exception())
    logMsg.append("Exiting %s with uncaught exception");
  else if (level_ == TL_all)
    logMsg.append("Exiting %s");
  else
    return;

  if (file_.length() > 0)
  {
    logMsg.append("(file %s, line %d)");
    logger_.log(category_, LL_DEBUG,
                logMsg, fnName_.data(), file_.data(), line_);
  }
  else
  {
    logger_.log(category_, LL_DEBUG, logMsg, fnName_.data());
  }
}

void QRLogger::log(const std::string &cat,
                   logLevel    level,
                   int         sqlCode,
                   const char  *queryId,
                   const char  *logMsgTemplate...)
{

  log4cplus::Logger myLogger = log4cplus::Logger::getInstance(cat);
  logLevel myLevel = (logLevel)myLogger.getLogLevel();
  if ( myLevel == LL_OFF )
    return;
  // If the configured logging level is greater (more restrictive) than
  // the requested level, don't log.
  if ( myLevel > level)
    return;

  va_list args ;
  va_start(args, logMsgTemplate);

  char* buffer = buildMsgBuffer(cat, level, logMsgTemplate, args);
  NAString logData = QRLogger::instance().getMyProcessInfo();
  char sqlCodeBuf[30];
  snprintf(sqlCodeBuf, sizeof(sqlCodeBuf), "%d", sqlCode);
  logData += ",";
  if (sqlCode != 0)
    {
      logData += " SQLCODE: ";
      logData += sqlCodeBuf;
    }
  logData += ",";
  if (queryId != NULL && queryId[0] != '\0')
    {
      logData += " QID: ";
      logData += queryId;
    }
  logData += ", ";
  logData += buffer;
  log1(cat, level, logData.data());
  va_end(args);
}

void QRLogger::log(const std::string &cat,
                   logLevel    level,
                   const char  *logMsgTemplate...)
{
  log4cplus::Logger myLogger = log4cplus::Logger::getInstance(cat);
  logLevel myLevel = (logLevel)myLogger.getLogLevel();
  if (myLevel == LL_OFF)
      return;
  // If the configured logging level is greater (more restrictive) than
  // the requested level, don't log.
  if (myLevel > level)
      return;

  va_list args ;
  va_start(args, logMsgTemplate);

  char* buffer = buildMsgBuffer(cat, level, logMsgTemplate, args);
  NAString logData = QRLogger::instance().getMyProcessInfo();
  // SQLCode and Query id are not provided in this flavor
  logData += ",,,";
  logData += buffer;
  log1(cat, level, logData.data());

  va_end(args);
}

NABoolean QRLogger::initLog4cplus(ExecutableModule module)
{
   NABoolean retcode;
   static bool singleSqlLogFile = (getenv("TRAF_MULTIPLE_SQL_LOG_FILE") == NULL);
   QRLogger::instance().setModule(module);
   if (singleSqlLogFile)
      retcode =  QRLogger::instance().initLog4cplus("log4cplus.trafodion.sql.config");
   else
      retcode =  QRLogger::instance().initLog4cplus("log4cplus.trafodion.masterexe.config");
   return retcode;
}


NABoolean QRLogger::startLogFile(const std::string &cat, const char * logFileName)
{
  NABoolean retcode = TRUE;  // assume success
  try 
    {
      log4cplus::Logger logger(Logger::getInstance(cat));
      logger.setAdditivity(false);  // make this logger non-additive

      log4cplus::tstring pattern = LOG4CPLUS_TEXT("%d, %p, %c, %m%n");
      log4cplus::PatternLayout *layout = new PatternLayout(pattern);
      log4cplus::tstring fileName = log4cplus::helpers::tostring(logFileName);
      log4cplus::SharedAppenderPtr fap(new FileAppender(fileName /* no append */));
      fap->setLayout(std::auto_ptr<Layout>(layout));
      logger.addAppender(fap);
    }
  catch (...)
    {
      retcode = FALSE;
    }

  return retcode;
}

NABoolean QRLogger::stopLogFile(const std::string &cat)
{
  NABoolean retcode = TRUE;  // assume success
  try 
    {
      log4cplus::Logger logger(Logger::getInstance(cat));
      logger.removeAllAppenders();
    }
  catch (...)
    {
      retcode = FALSE;
    }

  return retcode;
}

NABoolean QRLogger::getRootLogDirectory(const std::string &cat, std::string &out)
{
  NABoolean retcode = TRUE;  // assume success
   
  out.clear();

  // strip off all but the first qualifier of the category name

  size_t firstDot = cat.find_first_of('.');
  std::string firstQualifier;
  if (firstDot == std::string::npos)
    firstQualifier = cat;  // no dot, use the whole thing
  else
    firstQualifier = cat.substr(0,firstDot);

  try {
      log4cplus::Logger logger(Logger::getInstance(firstQualifier));
      log4cplus::SharedAppenderPtrList appenderList = logger.getAllAppenders();
      for (size_t i = 0; i < appenderList.size(); i++) {
        log4cplus::SharedAppenderPtr appender = appenderList[i];
        log4cplus::tstring appenderName = appender->getName();
        out = appenderName;
        size_t lastSlash = out.find_last_of('/');
        if (lastSlash != std::string::npos)
          out = out.substr(0, lastSlash);
        return TRUE;
      }
  }
  catch (...) {
    retcode = FALSE;
  }

  return retcode;
}


