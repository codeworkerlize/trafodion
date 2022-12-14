
/* -*-C++-*-
******************************************************************************
*
* File:         CliExtern.cpp
* Description:  Separation of Cli.cpp into a stub and client routine.
*               Originally done to help with NT work on single-threading
*               access to the CLI routines.  Will help to segregate
*               other work like DLL/Library work etc.
*
* Created:      7/19/97
* Language:     C and C++
*
*
*
******************************************************************************
*/

#include <time.h>

#include "cli/Cli.h"
#include "cli/CliSemaphore.h"
#include "cli/cli_stdh.h"
#include "common/ComRtUtils.h"
#include "common/NLSConversion.h"
#include "common/Platform.h"
#include "common/cextdecs.h"
#include "common/feerrors.h"
#include "common/wstr.h"
#include "exp/ExpError.h"
#include "exp/exp_clause_derived.h"
#include "sqlmxevents/logmxevent.h"
//#include "common/NAString.h"


#ifndef pdctctlz_h_dct_get_by_name_
#define pdctctlz_h_dct_get_by_name_  // so that we only get dct_get_by_name
#endif
#ifndef pdctctlz_h_including_section
#define pdctctlz_h_including_section  // from pdctctlz.h
#endif
#ifndef pdctctlz_h_
#define pdctctlz_h_
#endif

//#include "common/ComRegAPI.h"

#include <unistd.h>

#include "cli/Context.h"
#include "cli/Statement.h"
#include "common/ComExeTrace.h"
#include "common/ComSqlId.h"
#include "common/dfs2rec.h"
#include "exp/ExpLOBenums.h"
#include "qmscommon/QRLogger.h"
#include "runtimestats/SqlStats.h"
#include "seabed/fs.h"
#include "seabed/fserr.h"
#include "seabed/ms.h"
#include "seabed/sqstatehi.h"
#include "seabed/thread.h"

extern char **environ;

// this is set to true after the first CLI call.
// On the first cli call, method CliNonPrivPrologue is called.
// Only used for the top level CLI calls and not if cli is called
// from within priv srl.
NABoolean __CLI_NONPRIV_INIT__ = FALSE;

#define CLI_NONPRIV_PROLOGUE(rc)  \
  if (NOT __CLI_NONPRIV_INIT__) { \
    rc = CliNonPrivPrologue();    \
    if (rc) return rc;            \
    __CLI_NONPRIV_INIT__ = TRUE;  \
  }

#define CLI_NONPRIV_PROLOGUE_SHORT(rc) \
  if (NOT __CLI_NONPRIV_INIT__) {      \
    rc = (short)CliNonPrivPrologue();  \
    if (rc) return rc;                 \
    __CLI_NONPRIV_INIT__ = TRUE;       \
  }

// Check if the current system default experience level and the error experience levels are
// compatible. Compatible here means that the error experience level is higher or equal to
// the system default experience level. For instance, the ADVANCED experience level is higher
// than the BEGINNER experience level.

// SQSTATE segment start here

int muse(NAHeap *heap, size_t minTotalSize, char *repBuffer, size_t maxRspSize, size_t *rspSize, bool *bufferFull);

extern CliGlobals *cli_globals;

enum { MAX_IC_ARGS = 20 };
enum { MAX_RSP = 1024 * 1024 };  // 1MB

// module name
#define MYMODULE    exestate
#define MYMODULESTR "exestate"

// ic externs
// SQSTATE_IC_EXTERN(MYMODULE,t1);

// pi externs
// SQSTATE_PI_EXTERN(MYMODULE,t1);

//
// ic entry point for IPC info
//

SQSTATE_IC_EP(MYMODULE, ipc, sre) {
  int arg;
  int argc;
  char *argv[MAX_IC_ARGS];
  char rsp[MAX_RSP];
  int rsp_len = 0;
  char *myProgName;
  char buffer[80];
  sprintf(buffer, "CliGlobals %p", cli_globals);

  if (sqstateic_get_ic_args(sre, &argc, &argv[0], MAX_IC_ARGS, rsp, MAX_RSP, &rsp_len)) {
    if (NULL == cli_globals) {
      rsp_len += sprintf(&rsp[rsp_len], "Cli not initialized, no ipc information available\n\n");
    } else if (NULL == cli_globals->exCollHeap()) {
      rsp_len += sprintf(&rsp[rsp_len], "No Executor Heap, no ipc information available\n\n");
    } else if (NULL == cli_globals->getEnvironment()) {
      rsp_len += sprintf(&rsp[rsp_len], "IPC Environment not initialized, no ipc information available\n\n");
    } else {
      NAHeap *executorHeap = (NAHeap *)cli_globals->exCollHeap();
      IpcEnvironment *env = cli_globals->getEnvironment();
      IpcAllConnections *allc = env->getAllConnections();
      IpcSetOfConnections pendingIOs = allc->getPendingIOs();
      CollIndex allEntries = env->getAllConnections()->entries();
      CollIndex pendingIOEntries = pendingIOs.entries();
      bool infoAllConnections = false;
      char allConnectionsArgVal[] = "allconnections";

      if (argc > 0 && strlen(argv[0]) <= strlen(allConnectionsArgVal) &&
          (memcmp(argv[0], allConnectionsArgVal, strlen(argv[0])) == 0))
        infoAllConnections = true;
      if (strlen(cli_globals->myProgName()) == 0) cli_globals->initMyProgName();
      myProgName = cli_globals->myProgName();
      rsp_len += sprintf(&rsp[rsp_len], "in %s/ic-%s/%s\n", myProgName, MYMODULESTR, "ipc");
      rsp[rsp_len++] = '\n';
      rsp_len +=
          sprintf(&rsp[rsp_len], "IpcEnvironment: allConnections_ %d, pendingIOs_ %d\n", allEntries, pendingIOEntries);
      if (infoAllConnections)
        allc->infoAllConnections(rsp, MAX_RSP, &rsp_len);
      else
        pendingIOs.infoPendingConnections(rsp, MAX_RSP, &rsp_len);
    }
    sqstateic_reply(sre, rsp, rsp_len);
  }
}

//
// pi entry point for IPC info
//
SQSTATE_PI_EP(MYMODULE, ipc, node, proc, info, lib) {
  char rsp[MAX_RSP];
  int rsp_len;

  sqstatepi_printf("%s/%s: pi\n", MYMODULESTR, "ipc");
  if ((MS_Mon_Node_Info_Entry_Type *)NULL == node) {
    sqstatepi_printf("  Invalid node\n");
    return;
  }
  if ((MS_Mon_Process_Info_Type *)NULL == proc) {
    sqstatepi_printf("  Invalid proc\n");
    return;
  }
  sqstatepi_printf("  node-info. nname=%s, nid=%d\n", node->node_name, node->nid);
  sqstatepi_printf("  proc-info. pname=%s, p-id=%d/%d\n", proc->process_name, proc->nid, proc->pid);
  sqstatepi_printf("  plugin-info. verbose=%d, verbosev=%d\n", info->verbose, info->verbosev);
  if (sqstatepi_send_ic_ok(MYMODULESTR,  // module
                           "ipc",        // call
                           node,         // node
                           proc,         // proc
                           info,         // info
                           lib,          // library
                           rsp,          // rsp
                           MAX_RSP,      // rsp-len
                           &rsp_len))    // rsp-len
    sqstatepi_print_ic_reply(rsp, rsp_len);
}

//
// ic entry point for muse
//
SQSTATE_IC_EP(MYMODULE, muse, sre) {
  int arg;
  int argc;
  char *argv[MAX_IC_ARGS];
  char rsp[MAX_RSP];
  int rsp_len = 0;
  size_t muse_len;
  size_t size = 64;
  char *myProgName;
  bool bufferFull;

  if (sqstateic_get_ic_args(sre, &argc, &argv[0], MAX_IC_ARGS, rsp, MAX_RSP, &rsp_len)) {
    if (NULL == cli_globals) {
      rsp_len += sprintf(&rsp[rsp_len], "Cli not initialized. No memory information available\n\n");
    } else if (NULL == cli_globals->exCollHeap()) {
      rsp_len += sprintf(&rsp[rsp_len], "Executor heap not initialized, No memory information available\n\n");
    } else {
      NAHeap *executorHeap = (NAHeap *)cli_globals->exCollHeap();

      if (argc > 0) size = atoi(argv[0]);
      if (strlen(cli_globals->myProgName()) == 0) cli_globals->initMyProgName();
      myProgName = cli_globals->myProgName();
      rsp_len += sprintf(&rsp[rsp_len], "in %s/ic-%s/%s\n", myProgName, MYMODULESTR, "muse");
      rsp[rsp_len++] = '\n';
      muse(executorHeap, size * 1024, &rsp[rsp_len], (size_t)(MAX_RSP - rsp_len), &muse_len, &bufferFull);
      rsp_len += muse_len;
      if ((!bufferFull) && (NULL != cli_globals->getIpcHeap())) {
        muse(cli_globals->getIpcHeap(), size * 1024, &rsp[rsp_len], (size_t)(MAX_RSP - rsp_len), &muse_len,
             &bufferFull);
        rsp_len += muse_len;
      }
      if ((!bufferFull) && (NULL != cli_globals->getStatsHeap())) {
        muse(cli_globals->getStatsHeap()->getParent(), size * 1024, &rsp[rsp_len], (size_t)(MAX_RSP - rsp_len),
             &muse_len, &bufferFull);
        rsp_len += muse_len;
      }
    }
    sqstateic_reply(sre, rsp, rsp_len);
  }
}

//
// pi entry point for muse
//
SQSTATE_PI_EP(MYMODULE, muse, node, proc, info, lib) {
  char rsp[MAX_RSP];
  int rsp_len;

  sqstatepi_printf("%s/%s: pi\n", MYMODULESTR, "muse");
  if ((MS_Mon_Node_Info_Entry_Type *)NULL == node) {
    sqstatepi_printf("  Invalid node\n");
    return;
  }
  if ((MS_Mon_Process_Info_Type *)NULL == proc) {
    sqstatepi_printf("  Invalid proc\n");
    return;
  }
  sqstatepi_printf("  node-info. nname=%s, nid=%d\n", node->node_name, node->nid);
  sqstatepi_printf("  proc-info. pname=%s, p-id=%d/%d\n", proc->process_name, proc->nid, proc->pid);
  sqstatepi_printf("  plugin-info. verbose=%d, verbosev=%d\n", info->verbose, info->verbosev);
  if (sqstatepi_send_ic_ok(MYMODULESTR,  // module
                           "muse",       // call
                           node,         // node
                           proc,         // proc
                           info,         // info
                           lib,          // library
                           rsp,          // rsp
                           MAX_RSP,      // rsp-len
                           &rsp_len))    // rsp-len
    sqstatepi_print_ic_reply(rsp, rsp_len);
}

const char *exetrace = "exetrace";

//
// usage info for executor tracing
//
int trPrintUsage(char usageStr[], int len) {
  len += sprintf(&usageStr[len], "sqstate <sqstate args> exetrace-exetrace [[-icarg <TraceOpt>] ... ]\n");
  len += sprintf(&usageStr[len],
                 //  "  <TraceOpt> := progname=<value> | ofilename=<value> | <listOpt>\n");
                 "  <TraceOpt> := progname=<value> | <listOpt>\n");
  len += sprintf(&usageStr[len], "  <value> := tdm_arkesp | tdm_arkcmp | mxosrvr | <file_name>\n");
  len += sprintf(&usageStr[len],
                 //  "  <listOpt> := listall | listone=<trace_id> | listinfoall | suspend | help\n");
                 "  <listOpt> := listinfoall | listone=<trace_id> | suspend | help\n");

  len += sprintf(&usageStr[len], "\n  Note:\n");
  // len += sprintf(&usageStr[len],
  //  "    * use ofilename option to specify output file name\n");
  len += sprintf(&usageStr[len], "    * use listinfoall option to list currently registered traces\n");
  len += sprintf(&usageStr[len], "    * use listone option to list trace data for a trace id\n");
  // len += sprintf(&usageStr[len],
  //  "    * use listall option to dump all date from registered traces\n");
  len += sprintf(&usageStr[len], "    * use suspend option to halt all threads on subject process(es)\n");
  len += sprintf(&usageStr[len], "      while retreiving trace data\n");

  return len;
}

//
// ic entry point for esp tracing
// to set break point in debug, use sqstate_ic_exestate_exetrace as the name
//
SQSTATE_IC_EP(MYMODULE, exetrace, sre) {
  int arg;
  int argc;
  char *argv[MAX_IC_ARGS];
  char rsp[MAX_RSP];
  int rsp_len;
  bool showMine = true;
  bool showAllTraces = false;
  bool showOneTrace = false;
  bool showTraceInfoAll = false;
  bool toSuspend = false;
  void *traceId = 0;

  if (sqstateic_get_ic_args(sre, &argc, argv, MAX_IC_ARGS, rsp, MAX_RSP, &rsp_len)) {
    CliGlobals *g = cli_globals;
    if (g) {
      if (strlen(g->myProgName()) == 0) g->initMyProgName();

      if (argc > 0) {
        //   rsp_len += sprintf(&rsp[rsp_len], "ic-args:\n");
        for (arg = 0; arg < argc; arg++) {
          if (strncmp(argv[arg], "progname", 8) == 0) {
            char *pname = strchr(argv[arg], '=');
            if (strncmp((++pname), g->myProgName(), strlen(pname))) showMine = false;
          } else if (strncmp(argv[arg], "ofilename", 9) == 0) {
            // to be coded
          } else if (strncmp(argv[arg], "listall", 7) == 0) {
            showAllTraces = true;
            // more to be coded
          } else if (strncmp(argv[arg], "listone", 7) == 0) {
            showOneTrace = true;
            char *p = strchr(argv[arg], '=');
            while (p && *p == ' ') p++;
            if (!p) {
              // bad input
              rsp_len += sprintf(&rsp[rsp_len], "Invalid: arg[%d]='%s'\n", arg, argv[arg]);
              rsp_len = trPrintUsage(rsp, rsp_len);
            } else {
              sscanf(p, "=%p", &traceId);
            }
          } else if (strncmp(argv[arg], "listinfoall", 11) == 0) {
            showTraceInfoAll = true;
          } else if (strncmp(argv[arg], "suspend", 7) == 0) {
            toSuspend = true;
          } else if (strncmp(argv[arg], "help", 4) == 0) {
            // print usage
            rsp_len = trPrintUsage(rsp, rsp_len);
            showMine = false;
            break;
          } else {
            // invalid option
            rsp_len += sprintf(&rsp[rsp_len], "Invalid: arg[%d]='%s'\n", arg, argv[arg]);
            rsp_len = trPrintUsage(rsp, rsp_len);
            showMine = false;
            break;
          }
        }
      }

      if (showMine) {
        if (toSuspend) thread_suspend_all();
        rsp_len += sprintf(&rsp[rsp_len], "From process %s (%s pin %.8d):\n", g->myProcessNameString(), g->myProgName(),
                           g->myPin());
        rsp_len += sprintf(&rsp[rsp_len], "progName=%s\n", g->myProgName());
        if (showTraceInfoAll) {
          if (g->getExeTraceInfo()) {
            int len = 0;
            ExeTraceInfo *ti = g->getExeTraceInfo();
            int ret = ti->getExeTraceInfoAll(&rsp[rsp_len], MAX_RSP - rsp_len, &len);
            rsp_len += len;

            if (ret == 0) {  // not more trace info
              rsp_len += sprintf(&rsp[rsp_len], "No more Executor Trace Info.\n");
            } else if (len == 0) {  // not enough space
              rsp_len -= 80;        // back a bit in order to print this:
              rsp_len += sprintf(&rsp[rsp_len], "\n\tBuffer not big enough!! Information truncated.\n");
            }
          } else {
            rsp_len += sprintf(&rsp[rsp_len], "Executor Trace Info unavailable.\n");
          }
        }
        if (showOneTrace) {
          if (g->getExeTraceInfo()) {
            int len = 0;
            ExeTraceInfo *ti = g->getExeTraceInfo();
            int ret = ti->getExeTraceById(traceId, &rsp[rsp_len], MAX_RSP - rsp_len, &len);
            rsp_len += len;

            if (ret < 0) {  // not more trace info
              rsp_len += sprintf(&rsp[rsp_len], "No Executor Trace for id=%p.\n", traceId);
            } else if (ret > 0) {  // not enough space
              // back some space to print the warning message... (80 bytes)
              rsp_len = (MAX_RSP - rsp_len > 80) ? rsp_len : MAX_RSP - 80;
              rsp_len += sprintf(&rsp[rsp_len], "\n\tBuffer not big enough for trace data!!\n");
            }
          } else {
            rsp_len += sprintf(&rsp[rsp_len], "Executor Trace Info unavailable.\n");
          }
          showOneTrace = false;  // reset
        }
        rsp_len += sprintf(&rsp[rsp_len], "Done\n");
        if (toSuspend) thread_resume_suspended();
      }
    } else {
      rsp_len += sprintf(&rsp[rsp_len], "Not yet initialized to get exe trace info.\n");
    }
  }
  sqstateic_reply(sre, rsp, rsp_len);
}

//
// pi entry point
//
SQSTATE_PI_EP(MYMODULE, exetrace, node, proc, info, lib) {
  char rsp[MAX_RSP];
  int rsp_len;

  sqstatepi_printf("%s/%s: pi\n", MYMODULESTR, "exetrace");
  if ((MS_Mon_Node_Info_Entry_Type *)NULL == node) {
    sqstatepi_printf("  Invalid node\n");
    return;
  }
  if ((MS_Mon_Process_Info_Type *)NULL == proc) {
    sqstatepi_printf("  Invalid proc\n");
    return;
  }

  if (sqstatepi_send_ic_ok(MYMODULESTR,  // module
                           exetrace,     // call
                           node,         // node
                           proc,         // proc
                           info,         // info
                           lib,          // lib
                           rsp,          // rsp
                           MAX_RSP,      // rsp-len
                           &rsp_len))    // rsp-len
    sqstatepi_print_ic_reply(rsp, rsp_len);
}

// SQSTATE segment end

NABoolean doGenerateAnEMSEvent(char *localEMSExperienceLevel, const int experienceLevelLen,
                               SQLMXLoggingArea::ExperienceLevel emsEventEL) {
  if (getenv("TEST_ERROR_EVENT")) return TRUE;  // generate an error event for any error for testing

  if (!((emsEventEL != SQLMXLoggingArea::eBeginnerEL) &&
        (!str_cmp(localEMSExperienceLevel, "BEGINNER", experienceLevelLen))))

  {
    return TRUE;  // go ahead and generate an EMS event
  }
  return FALSE;
}

// Generate an EMS event when the condition's diagnostics information is retrieved
// In the case where the same diag condition is queried multiple times, only one EMS event
// is generated for this condition. This assertion is ensured by the getEMSEventVisits()
// method of the ComCondition class. The repeated querying of the same condition may be originated
// internally or externally. It can be originated internally from components such as the catalog
// manager or MXCI and externally from a user application.
//
// We do not generate events for errors with the following SQLCodes
//   0:     normal returns - no error or warning
//   100:   EOF
//   8822:  the "The statement was not prepared" error code. Ignore this error since
//          it is not descriptive and more information should be available with an
//          acommpanying condition.
//   20109: MXCI return code that encapsulates a utility error. The utility error should
//          be handled by the utility layer so ignore it here
//   EXE_RTS_QID_NOT_FOUND:    Filter out any retcode occuring from any CLI call
//          that can be ignored from logging into EMS

void logAnMXEventForError(ComCondition &condition, SQLMXLoggingArea::ExperienceLevel emsEventEL)

{
  // all the lengths are defined in logmxevent.h
  char buf[EMS_BUFFER_SIZE + 1] = "\0";
  char localEMSSeverity[EMS_SEVERITY_LEN + 1] = "\0";
  char localEMSEventTarget[EMS_EVENT_TARGET_LEN + 1] = "\0";
  char localEMSExperienceLevel[EMS_EXPERIENCE_LEVEL_LEN + 1] = "\0";
  NABoolean forceDialout = FALSE;
  NABoolean isWarning = FALSE;
  long transid = -1;

  if (condition.getEMSEventVisits() >= 1) return;

  int sqlCode = condition.getSQLCODE();

  if ((sqlCode == 0) || (sqlCode == 100) || (sqlCode == -EXE_RTS_QID_NOT_FOUND) || (sqlCode == -8822) ||
      (sqlCode == -20109))
    return;

  if (sqlCode == -8551) {
    int nskCode = condition.getNskCode();
    if ((nskCode == 40) || (nskCode == 35)) forceDialout = TRUE;
    // If the caller is UNC then suppress error 73
    if (GetCliGlobals()->isUncProcess()) {
      if (sqlCode == -8551)
        if (nskCode == 73) return;
    }
  }

  // event-ids for the  events to be generated are the absolute values for the
  // errors' sqlcodes
  int sqlcodeToEventId = sqlCode;
  if (sqlCode > 0) isWarning = TRUE;
  if (sqlCode < 0) sqlcodeToEventId = sqlCode * (-1);
  transid = GetCliGlobals()->currContext()->getTransaction()->getTransid();

  // the EMS event attributes are always in UTF8
  UnicodeStringToLocale(CharInfo::UTF8,
                        (NAWchar *)(condition.getMessageText(TRUE,              // NABoolean prefixAdded
                                                             CharInfo::UTF8)),  // msg in UCS2
                        condition.getMessageLength(), buf, EMS_BUFFER_SIZE, TRUE /* addNullAtEnd */,
                        TRUE /*allowInvalidCodePoint */);

  // retrieve the EMS event attributes for error with sqlcode sqlCode
  ComEMSSeverity(sqlCode, localEMSSeverity);

  // Note: This function will downgrade any warnings which have
  // an EventTarget as DIALOUT to LOGONLY
  // This function will also force a dialout if the flag is TRUE
  ComEMSEventTarget(sqlCode, localEMSEventTarget, forceDialout);
  ComEMSExperienceLevel(sqlCode, localEMSExperienceLevel);

  // generate an event if the user experience level of the error is compatible with the
  // current system default user experience level. Compatible here means that the error
  // experience level is higher or equal to the system default experience level.
  // For instance, the ADVANCED experience level is higher than the BEGINNER experience level.
  if (doGenerateAnEMSEvent(localEMSExperienceLevel, EMS_EXPERIENCE_LEVEL_LEN, emsEventEL)) {
    // convert String params to locale
    char lstring0[EMS_BUFFER_SIZE + 1] = "\0";
    char lstring1[EMS_BUFFER_SIZE + 1] = "\0";
    char lstring2[EMS_BUFFER_SIZE + 1] = "\0";
    char lstring3[EMS_BUFFER_SIZE + 1] = "\0";
    char lstring4[EMS_BUFFER_SIZE + 1] = "\0";

    // convert event attributes to  UTF8
    for (int i = 0; i < 5; i++) {
      char *lstringi = NULL;
      switch (i) {
        case 0:
          lstringi = lstring0;
          break;
        case 1:
          lstringi = lstring1;
          break;
        case 2:
          lstringi = lstring2;
          break;
        case 3:
          lstringi = lstring3;
          break;
        case 4:
          lstringi = lstring4;
          break;
      }

      if (condition.hasOptionalString(i)) {
        if (condition.getOptionalString(i) && condition.getOptionalStringCharSet(i) == CharInfo::UTF8) {
          strcpy(lstringi, condition.getOptionalString(i));
        } else if (condition.getOptionalWString(i)) {
          assert(condition.getOptionalStringCharSet(i) == CharInfo::UNICODE);

          UnicodeStringToLocale(CharInfo::UTF8, condition.getOptionalWString(i),
                                na_wcslen(condition.getOptionalWString(i)), lstringi, EMS_BUFFER_SIZE,
                                TRUE /* addNullAtEnd */, TRUE /*allowInvalidCodePoint */);
        } else {
          char *dummyFirstUntranslatedChar;

          LocaleToUTF8(cnv_version1, condition.getOptionalString(i), str_len(condition.getOptionalString(i)), lstringi,
                       EMS_BUFFER_SIZE, convertCharsetEnum(condition.getOptionalStringCharSet(i)),
                       dummyFirstUntranslatedChar, NULL, TRUE); /* addNullAtEnd */
        }
      }
    }

    if (NOT isWarning && NULL != condition.getSqlID()) {
      StatsGlobals *statsGlobals = GetCliGlobals()->getStatsGlobals();
      if (NULL != statsGlobals) {
        int errorCode = statsGlobals->getStatsSemaphore(GetCliGlobals()->getSemId(), GetCliGlobals()->myPin());
        if (0 == errorCode) {
          StmtStats *stmtStats = statsGlobals->getMasterStmtStats(condition.getSqlID(), str_len(condition.getSqlID()),
                                                                  RtsQueryId::ANY_QUERY_);
          if (NULL != stmtStats) {
            ExMasterStats *masterStats = stmtStats->getMasterStats();
            if (NULL != masterStats) {
              size_t bufLen = strlen(buf);
              snprintf(buf + bufLen, EMS_BUFFER_SIZE - bufLen, ", ERROR SQLINFO: %s", masterStats->getSourceString());
            }
          }
        }
        statsGlobals->releaseStatsSemaphore(GetCliGlobals()->getSemId(), GetCliGlobals()->myPin());
      }
    }

    // when logging using log4cxx these are the only tokens used
    SQLMXLoggingArea::logSQLMXEventForError(sqlcodeToEventId, buf, condition.getSqlID(), isWarning);

    /*
          // when logging using seapilot more tokens can be used
          SQLMXLoggingArea::logSQLMXEventForError( sqlcodeToEventId,
                                                    localEMSExperienceLevel,
                                                    localEMSSeverity,
                                                    localEMSEventTarget,
                                                    buf ,
                                                    condition.getSqlID(),
                                                    condition.getOptionalInteger(0),
                                                    condition.getOptionalInteger(1),
                                                    condition.getOptionalInteger(2),
                                                    condition.getOptionalInteger(3),
                                                    condition.getOptionalInteger(4),
                                                    lstring0,
                                                    lstring1,
                                                    lstring2,
                                                    lstring3,
                                                    lstring4,
                                                    condition.getServerName(),
                                                    condition.getConnectionName(),
                                                    condition.getConstraintCatalog(),
                                                    condition.getConstraintSchema(),
                                                    condition.getConstraintName(),
                                                    condition.getTriggerCatalog(),
                                                    condition.getTriggerSchema(),
                                                    condition.getTriggerName(),
                                                    condition.getCatalogName(),
                                                    condition.getSchemaName(),
                                                    condition.getTableName(),
                                                    condition.getColumnName(),
                                                    transid,
                                                    condition.getRowNumber(),
                                                    condition.getNskCode(),
                                                    isWarning);
    */
  }
}

// RecordError
//
// This wrapper is called after every CLI function to capture the SQLID and
// associate it with the conditions as well as RTS

int RecordError(SQLSTMT_ID *currentSqlStmt, int inRetcode) {
  if (inRetcode == 0) return inRetcode;

  // Get the SQL ID (aka SQL_ATTR_UNIQUE_STMT_ID) from the statement (SQLSTMT_ID)

  if (currentSqlStmt != NULL) {
    SQL_EXEC_SetStmtAttr(currentSqlStmt, SQL_ATTR_COPY_STMT_ID_TO_DIAGS, 0, NULL);

    if (inRetcode != -EXE_RTS_QID_NOT_FOUND) {
      SQL_EXEC_SetErrorCodeInRTS(currentSqlStmt, inRetcode);
    }
  }
  return (inRetcode);
}

// DllMain DLL_PROCESS_DETACH routine suspends the memory manager update thread so that an
// access violation does not occur after Cheyenne unloads the DLL.
#ifdef SQ_CPP_INTF
extern short my_mpi_setup(int *argc, char **argv[]);
#endif
extern "C" {
#ifndef SQ_CPP_INTF
short my_mpi_setup(int *argc, char **argv[]);
#endif
};

#define MY_ARG_MAX 2097152

short sqInit() {
  static bool sbInitialized = false;

  if (!sbInitialized) {
    sbInitialized = true;
    int largc = 0;
    char **largv = 0;
    char procFileName[128];
    FILE *proc_file = 0;
    char *buf = 0;
    int p_i = 0;
    int c = 0;

    long lv_arg_max = sysconf(_SC_ARG_MAX);
    if ((errno == EINVAL) ||  // In the remote chance that sysconf returns an error
        (lv_arg_max < 0) ||   // just some sanity check
        (lv_arg_max > MY_ARG_MAX)) {
      lv_arg_max = MY_ARG_MAX;
    }

    buf = (char *)malloc((lv_arg_max + 1) * sizeof(char));
    if (buf == NULL) {
      cerr << "sqInit: Error while allocatting memory for " << getpid() << ". Exiting..." << endl;
      exit(1);
    }
    // This memory is never freed.
    largv = (char **)malloc(512 * sizeof(char *));

    sprintf(procFileName, "/proc/%d/cmdline", getpid());
    proc_file = fopen(procFileName, "r");

    buf[0] = 0;
    p_i = 0;
    while ((c = fgetc(proc_file)) != EOF) {
      buf[p_i++] = c;
      if (p_i >= lv_arg_max) abort();
      if (c == 0) {
        // This memory is never freed.
        largv[largc] = (char *)malloc((p_i + 1) * sizeof(char));
        strcpy(largv[largc++], buf);
        p_i = 0;
        buf[0] = 0;
      }
    }

    free(buf);
    fclose(proc_file);

    try {
      short retcode = my_mpi_setup(&largc, &largv);
      QRLogger::initLog4cplus(QRLogger::QRL_MXEXE);
    } catch (...) {
      cerr << "Error while initializing messaging system. Exiting..." << endl;
      exit(1);
    }

    // Initialize an Instruction Info array's offset index
    ex_conv_clause::populateInstrOffsetIndex();
  }

  return 0;
}

static int CliNonPrivPrologue() {
  int retcode;

  if (cli_globals == NULL || cli_globals->getIsInitialized() == FALSE) {
    globalSemaphore.get();
    if (cli_globals != NULL) {
      globalSemaphore.release();
      return 0;
    }
    sqInit();
    CliGlobals::createCliGlobals(FALSE);  // this call will create the globals.
    ex_assert(SQL_EXEC_SetEnviron_Internal(1) == 0, "Unable to set the Environment");
    globalSemaphore.release();
  }
  return 0;
}

CLISemaphore *getCliSemaphore(ContextCli *&context) {
  context = cli_globals->currContext();
  if (context != NULL)
    return context->getSemaphore();
  else
    return cli_globals->getSemaphore();
}
#if 0
int cliWillThrow()
{
#ifdef _DEBUG
  // It is used to execute debug code like no-debug (release) code
  static int willThrow = -1;  
  if (willThrow == -1)
    {
      char *ptr = getenv("SQLMX_CLI_WILL_THROW");
      if (ptr)
        {
          willThrow = atoi(ptr);
          if (willThrow != 0)
            willThrow = 1;
        } 
      else
        willThrow = 1;
    }
  return willThrow;
#else
  return 1;
#endif
}
#endif

int SQL_EXEC_AllocDesc(/*INOUT*/ SQLDESC_ID *desc_id,
                       /*IN OPTIONAL*/ SQLDESC_ID *input_descriptor) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();
    retcode = SQLCLI_AllocDesc(GetCliGlobals(), desc_id, input_descriptor);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }
  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();

  retcode = RecordError(NULL, retcode);
  return retcode;
}

int SQL_EXEC_ALLOCDESC(
    /*INOUT*/ SQLDESC_ID *desc_id,
    /*IN OPTIONAL*/ SQLDESC_ID *input_descriptor) {
  // See comment above  : "COBOL change"
  if (input_descriptor && (input_descriptor->name_mode == -1)) input_descriptor = NULL;

  return SQL_EXEC_AllocDesc(desc_id, input_descriptor);
};

int SQL_EXEC_AllocDescBasic(/*INOUT*/ SQLDESC_ID *desc_id,
                            /*IN OPTIONAL*/ int max_entries) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();
    retcode = SQLCLI_AllocDescInt(GetCliGlobals(), desc_id, max_entries);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }
  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();
  retcode = RecordError(NULL, retcode);
  return retcode;
}

int SQL_EXEC_AllocStmt(/*INOUT*/ SQLSTMT_ID *new_statement_id,
                       /*IN OPTIONAL*/ SQLSTMT_ID *cloned_statement) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();
    retcode = SQLCLI_AllocStmt(GetCliGlobals(), new_statement_id, cloned_statement);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }

  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();
  retcode = RecordError(NULL, retcode);
  return retcode;
}

int SQL_EXEC_ALLOCSTMT(
    /*INOUT*/ SQLSTMT_ID *new_statement_id,
    /*IN OPTIONAL*/ SQLSTMT_ID *cloned_statement) {
  // See comment above  : "COBOL change"
  if (cloned_statement && (cloned_statement->name_mode == -1)) cloned_statement = NULL;

  return SQL_EXEC_AllocStmt(new_statement_id, cloned_statement);
};

int SQL_EXEC_AllocStmtForRS(/*IN*/ SQLSTMT_ID *callStmtId,
                            /*IN*/ int resultSetIndex,
                            /*INOUT*/ SQLSTMT_ID *resultSetStmtId) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();
    retcode = SQLCLI_AllocStmtForRS(GetCliGlobals(), callStmtId, resultSetIndex, resultSetStmtId);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }

  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();
  return retcode;
}
int SQL_EXEC_ALLOCSTMTFORRS(
    /*IN*/ SQLSTMT_ID *callStmtId,
    /*IN*/ int resultSetIndex,
    /*INOUT*/ SQLSTMT_ID *resultSetStmtId) {
  return SQL_EXEC_AllocStmtForRS(callStmtId, resultSetIndex, resultSetStmtId);
}

// nowait CLI
int SQL_EXEC_AssocFileNumber(/*IN*/ SQLSTMT_ID *statement_id,
                             /*IN*/ short file_number) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();
    retcode = SQLCLI_AssocFileNumber(GetCliGlobals(), statement_id, file_number);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }

  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();
  retcode = RecordError(statement_id, retcode);
  return retcode;
}

int SQL_EXEC_ASSOCFILENUMBER(/*IN*/ SQLSTMT_ID *statement_id,
                             /*IN*/ short file_number) {
  return SQL_EXEC_AssocFileNumber(statement_id, file_number);
};

int SQL_EXEC_ClearDiagnostics(/*IN*/ SQLSTMT_ID *statement_id) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();
    retcode = SQLCLI_ClearDiagnostics(GetCliGlobals(), statement_id);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }

  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();
  retcode = RecordError(statement_id, retcode);
  return retcode;
}
int SQL_EXEC_CLEARDIAGNOSTICS(/*IN*/ SQLSTMT_ID *statement_id) { return SQL_EXEC_ClearDiagnostics(statement_id); };

#if defined(CLI_LIB)
#pragma srlexports
#endif
int SQL_EXEC_CLI_VERSION() { return CLI_VERSION; }

int SQL_EXEC_CloseStmt(/*IN*/ SQLSTMT_ID *statement_id) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();
    retcode = SQLCLI_CloseStmt(GetCliGlobals(), statement_id);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }

  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();
  retcode = RecordError(statement_id, retcode);
  return retcode;
}

int SQL_EXEC_CLOSESTMT(
    /*IN*/ SQLSTMT_ID *statement_id) {
  return SQL_EXEC_CloseStmt(statement_id);
};

int SQL_EXEC_CreateContext(/*OUT*/ SQLCTX_HANDLE *context_handle,
                           /*IN*/ char *sqlAuthId,
                           /*IN*/ int forFutureUse) {
  int retcode;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    retcode = SQLCLI_CreateContext(GetCliGlobals(), context_handle, sqlAuthId, forFutureUse);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      throw;
    }
#endif
  }

  retcode = RecordError(NULL, retcode);
  return retcode;
}

int SQL_EXEC_CREATECONTEXT(/*OUT*/ SQLCTX_HANDLE *context_handle,
                           /*IN*/ char *sqlAuthId,
                           /*IN*/ int forFutureUse)

{
  return SQL_EXEC_CreateContext(context_handle, sqlAuthId, forFutureUse);
}

int SQL_EXEC_CurrentContext(/*OUT*/ SQLCTX_HANDLE *contextHandle) {
  int retcode;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    retcode = SQLCLI_CurrentContext(GetCliGlobals(), contextHandle);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      throw;
    }
#endif
  }

  retcode = RecordError(NULL, retcode);
  return retcode;
}
int SQL_EXEC_CURRENTCONTEXT(/*OUT*/ SQLCTX_HANDLE *contextHandle) { return SQL_EXEC_CurrentContext(contextHandle); };

int SQL_EXEC_DeleteContext(/*IN*/ SQLCTX_HANDLE contextHandle) {
  int retcode;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    retcode = SQLCLI_DeleteContext(GetCliGlobals(), contextHandle);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      throw;
    }
#endif
  }

  retcode = RecordError(NULL, retcode);
  return retcode;
}

int SQL_EXEC_DELETECONTEXT(/*IN*/ SQLCTX_HANDLE contextHandle) { return SQL_EXEC_DeleteContext(contextHandle); };

int SQL_EXEC_DropModule(/*IN*/ SQLMODULE_ID *module_name) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();
    retcode = SQLCLI_DropModule(GetCliGlobals(), module_name);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }

  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();
  retcode = RecordError(NULL, retcode);
  return retcode;
}

int SQL_EXEC_ResetContext(/*IN*/ SQLCTX_HANDLE contextHandle, /*IN*/ void *contextMsg) {
  int retcode;

  try {
    retcode = SQLCLI_ResetContext(GetCliGlobals(), contextHandle, contextMsg);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      throw;
    }
#endif
  }

  retcode = RecordError(NULL, retcode);
  return retcode;
}

int SQL_EXEC_RESETCONTEXT(/*IN*/ SQLCTX_HANDLE contextHandle, /*IN*/ void *contextMsg) {
  return SQL_EXEC_ResetContext(contextHandle, contextMsg);
};

// new UDR interface, internal use

int SQL_EXEC_GetUdrErrorFlags_Internal(/*OUT*/ int *udrErrorFlags) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();

    retcode = SQLCLI_GetUdrErrorFlags_Internal(GetCliGlobals(), udrErrorFlags);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }

  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();

  return retcode;
}
int SQL_EXEC_SetUdrAttributes_Internal(/*IN*/ int sqlAccessMode,
                                       /*IN*/ int forFutureUse) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();

    retcode = SQLCLI_SetUdrAttributes_Internal(GetCliGlobals(), sqlAccessMode, forFutureUse);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }

  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();

  return retcode;
}

int SQL_EXEC_ResetUdrErrorFlags_Internal() {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();

    retcode = SQLCLI_ResetUdrErrorFlags_Internal(GetCliGlobals());
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }

  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();

  return retcode;
}

int SQL_EXEC_SetUdrRuntimeOptions_Internal(/*IN*/ const char *options,
                                           /*IN*/ int optionsLen,
                                           /*IN*/ const char *delimiters,
                                           /*IN*/ int delimsLen) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  CLI_NONPRIV_PROLOGUE(retcode);

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();

    retcode = SQLCLI_SetUdrRuntimeOptions_Internal(GetCliGlobals(), options, optionsLen, delimiters, delimsLen);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      throw;
    }
#endif
  }

  threadContext->decrNumOfCliCalls();
  tmpSemaphore->release();

  return retcode;
}

int SQL_EXEC_DeallocDesc(/*IN*/ SQLDESC_ID *desc_id) {
  int retcode;
  CLISemaphore *tmpSemaphore = NULL;
  ContextCli *threadContext;

  try {
    tmpSemaphore = getCliSemaphore(threadContext);
    tmpSemaphore->get();
    threadContext->incrNumOfCliCalls();
    retcode = SQLCLI_DeallocDesc(GetCliGlobals(), desc_id);
  } catch (...) {
    retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
    if (cliWillThrow()) {
      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }
  int SQL_EXEC_DEALLOCDESC(
      /*IN*/ SQLDESC_ID * desc_id) {
    return SQL_EXEC_DeallocDesc(desc_id);
  };
  int SQL_EXEC_DeallocStmt(/*IN*/ SQLSTMT_ID * statement_id) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_DeallocStmt(GetCliGlobals(), statement_id);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    // Filter out -8804 since preprocessor always issue DeallocStmt and then
    // allocate statement in case of embedded dynamic sql statements
    if (retcode == -CLI_STMT_NOT_EXISTS) {
      if (statement_id->module == NULL || (statement_id->module != NULL && statement_id->module->module_name == NULL))
        retcode = RecordError(statement_id, retcode);
    } else {
      retcode = RecordError(statement_id, retcode);
    }
    return retcode;
  }
  int SQL_EXEC_DEALLOCSTMT(
      /*IN*/ SQLSTMT_ID * statement_id) {
    return SQL_EXEC_DeallocStmt(statement_id);
  };

  int SQL_EXEC_DefineDesc(/*IN*/ SQLSTMT_ID * statement_id,
                          /* (SQLWHAT_DESC) *IN*/ int what_descriptor,
                          /*IN*/ SQLDESC_ID *sql_descriptor) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_DefineDesc(GetCliGlobals(), statement_id, what_descriptor, sql_descriptor);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_DEFINEDESC(
      /*IN*/ SQLSTMT_ID * statement_id,
      /*IN* (SQLWHAT_DESC) */ int what_descriptor,
      /*IN*/ SQLDESC_ID *sql_descriptor) {
    return SQL_EXEC_DefineDesc(statement_id, what_descriptor, sql_descriptor);
  };
  int SQL_EXEC_DescribeStmt(/*IN*/ SQLSTMT_ID * statement_id,
                            /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
                            /*IN OPTIONAL*/ SQLDESC_ID * output_descriptor) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_DescribeStmt(GetCliGlobals(), statement_id, input_descriptor, output_descriptor);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_DESCRIBESTMT(
      /*IN*/ SQLSTMT_ID * statement_id,
      /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
      /*IN OPTIONAL*/ SQLDESC_ID * output_descriptor) {
    // See comment above  : "COBOL change"
    if (input_descriptor && (input_descriptor->name_mode == -1)) input_descriptor = NULL;

    if (output_descriptor && (output_descriptor->name_mode == -1)) output_descriptor = NULL;

    return SQL_EXEC_DescribeStmt(statement_id, input_descriptor, output_descriptor);
  };

  int SQL_EXEC_DisassocFileNumber(/*IN*/ SQLSTMT_ID * statement_id) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_DisassocFileNumber(GetCliGlobals(), statement_id);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_DISASSOCFILENUMBER(/*IN*/ SQLSTMT_ID * statement_id) {
    return SQL_EXEC_DisassocFileNumber(statement_id);
  };

  int SQL_EXEC_DropContext(/*IN*/ SQLCTX_HANDLE context_handle) {
    int retcode;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      retcode = SQLCLI_DropContext(GetCliGlobals(), context_handle);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        throw;
      }
#endif
    }

    retcode = RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_DROPCONTEXT(
      /*IN*/ SQLCTX_HANDLE context_handle) {
    return SQL_EXEC_DropContext(context_handle);
  };

  int SQL_EXEC_Exec(/*IN*/ SQLSTMT_ID * statement_id,
                    /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
                    /*IN*/ int num_ptr_pairs,
                    /*IN*/ int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode = SQLCLI_Exec(GetCliGlobals(), statement_id, input_descriptor, num_ptr_pairs, num_ap, ap, 0);

      if (retcode < 0) {
        retcode = SQLCLI_ProcessRetryQuery(GetCliGlobals(), statement_id, retcode, 0, 1, 0, 0);
      }

    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_EXEC(
      /*IN*/ SQLSTMT_ID * statement_id,
      /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
      /*IN*/ int num_ptr_pairs,
      /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_Exec(GetCliGlobals(), statement_id, input_descriptor, num_ptr_pairs, 0, VA_LIST_NULL, ptr_pairs);

    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  };
  int SQL_EXEC_ExecClose(/*IN*/ SQLSTMT_ID * statement_id,
                         /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
                         /*IN*/ int num_ptr_pairs,
                         /*IN*/ int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode = SQLCLI_ExecClose(GetCliGlobals(), statement_id, input_descriptor, num_ptr_pairs, num_ap, ap, 0);

    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_EXECCLOSE(
      /*IN*/ SQLSTMT_ID * statement_id,
      /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
      /*IN*/ int num_ptr_pairs,
      /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode =
          SQLCLI_ExecClose(GetCliGlobals(), statement_id, input_descriptor, num_ptr_pairs, 0, VA_LIST_NULL, ptr_pairs);

    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  };

  int SQL_EXEC_ExecDirect(/*IN*/ SQLSTMT_ID * statement_id,
                          /*IN*/ SQLDESC_ID * sql_source,
                          /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
                          /*IN*/ int num_ptr_pairs,
                          /*IN*/ int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode =
          SQLCLI_ExecDirect(GetCliGlobals(), statement_id, sql_source, input_descriptor, num_ptr_pairs, num_ap, ap, 0);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_ExecDirect2(/*IN*/ SQLSTMT_ID * statement_id,
                           /*IN*/ SQLDESC_ID * sql_source,
                           /*IN */ int prep_flags,
                           /*IN OPTIONAL*/ SQLDESC_ID *input_descriptor,
                           /*IN*/ int num_ptr_pairs,
                           /*IN*/ int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode = SQLCLI_ExecDirect2(GetCliGlobals(), statement_id, sql_source, prep_flags, input_descriptor,

                                   num_ptr_pairs, num_ap, ap, 0);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    retcode = RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_EXECDIRECT(
      /*IN*/ SQLSTMT_ID * statement_id,
      /*IN*/ SQLDESC_ID * sql_source,
      /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
      /*IN*/ int num_ptr_pairs,
      /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_ExecDirect(GetCliGlobals(), statement_id, sql_source, input_descriptor, num_ptr_pairs, 0,
                                  VA_LIST_NULL, ptr_pairs);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  };
  int SQL_EXEC_ExecDirectDealloc(/*IN*/ SQLSTMT_ID * statement_id,
                                 /*IN*/ SQLDESC_ID * sql_source,
                                 /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
                                 /*IN*/ int num_ptr_pairs,
                                 /*IN*/ int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode = SQLCLI_ExecDirectDealloc(GetCliGlobals(), statement_id, sql_source, input_descriptor, num_ptr_pairs,
                                         num_ap, ap, 0);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(NULL /* statement_id is now deallocated */, retcode);
    return retcode;
  }
  int SQL_EXEC_EXECDIRECTDEALLOC(/*IN*/ SQLSTMT_ID * statement_id,
                                 /*IN*/ SQLDESC_ID * sql_source,
                                 /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
                                 /*IN*/ int num_ptr_pairs,
                                 /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_ExecDirectDealloc(GetCliGlobals(), statement_id, sql_source, input_descriptor, num_ptr_pairs, 0,
                                         VA_LIST_NULL, ptr_pairs);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(NULL /* statement_id is now deallocated */, retcode);
    return retcode;
  }

  int SQL_EXEC_ExecFetch(/*IN*/ SQLSTMT_ID * statement_id,
                         /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
                         /*IN*/ int num_ptr_pairs,
                         /*IN*/ int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode = SQLCLI_ExecFetch(GetCliGlobals(), statement_id, input_descriptor, num_ptr_pairs, num_ap, ap, 0);

      if (retcode < 0) {
        retcode = SQLCLI_ProcessRetryQuery(GetCliGlobals(), statement_id, retcode, 0, 1, 1, 0);
      }

    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_EXECFETCH(/*IN*/ SQLSTMT_ID * statement_id,
                         /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
                         /*IN*/ int num_ptr_pairs,
                         /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode =
          SQLCLI_ExecFetch(GetCliGlobals(), statement_id, input_descriptor, num_ptr_pairs, 0, VA_LIST_NULL, ptr_pairs);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_ClearExecFetchClose(/*IN*/ SQLSTMT_ID * statement_id,
                                   /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
                                   /*IN OPTIONAL*/ SQLDESC_ID * output_descriptor,
                                   /*IN*/ int num_input_ptr_pairs,
                                   /*IN*/ int num_output_ptr_pairs,
                                   /*IN*/ int num_total_ptr_pairs,
                                   /*IN*/ int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode =
          SQLCLI_ClearExecFetchClose(GetCliGlobals(), statement_id, input_descriptor, output_descriptor,
                                     num_input_ptr_pairs, num_output_ptr_pairs, num_total_ptr_pairs, num_ap, ap, 0, 0);

      if (retcode < 0) {
        retcode = SQLCLI_ProcessRetryQuery(GetCliGlobals(), statement_id, retcode, 0, 0, 0, 1);
      }

    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_CLEAREXECFETCHCLOSE(/*IN*/ SQLSTMT_ID * statement_id,
                                   /*IN OPTIONAL*/ SQLDESC_ID * input_descriptor,
                                   /*IN OPTIONAL*/ SQLDESC_ID * output_descriptor,
                                   /*IN*/ int num_input_ptr_pairs,
                                   /*IN*/ int num_output_ptr_pairs,
                                   /*IN*/ int num_total_ptr_pairs,
                                   /*IN*/ SQLCLI_PTR_PAIRS input_ptr_pairs[],
                                   /*IN*/ SQLCLI_PTR_PAIRS output_ptr_pairs[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_ClearExecFetchClose(GetCliGlobals(), statement_id, input_descriptor, output_descriptor,
                                           num_input_ptr_pairs, num_output_ptr_pairs, num_total_ptr_pairs, 0,
                                           VA_LIST_NULL, input_ptr_pairs, output_ptr_pairs);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_Fetch(/*IN*/ SQLSTMT_ID * statement_id,
                     /*IN OPTIONAL*/ SQLDESC_ID * output_descriptor,
                     /*IN*/ int num_ptr_pairs,
                     /*IN*/ int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode = SQLCLI_Fetch(GetCliGlobals(), statement_id, output_descriptor, num_ptr_pairs, num_ap, ap, 0);

      if (retcode < 0) {
        retcode = SQLCLI_ProcessRetryQuery(GetCliGlobals(), statement_id, retcode, 0, 0, 1, 0);
      }
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_FETCH(/*IN*/ SQLSTMT_ID * statement_id,
                     /*IN OPTIONAL*/ SQLDESC_ID * output_descriptor,
                     /*IN*/ int num_ptr_pairs,
                     /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode =
          SQLCLI_Fetch(GetCliGlobals(), statement_id, output_descriptor, num_ptr_pairs, 0, VA_LIST_NULL, ptr_pairs);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    retcode = RecordError(statement_id, retcode);

    return retcode;
  }

  int SQL_EXEC_FetchClose(/*IN*/ SQLSTMT_ID * statement_id,
                          /*IN OPTIONAL*/ SQLDESC_ID * output_descriptor,
                          /*IN*/ int num_ptr_pairs,
                          /*IN*/ int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode = SQLCLI_FetchClose(GetCliGlobals(), statement_id, output_descriptor, num_ptr_pairs, num_ap, ap, 0);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_FETCHCLOSE(/*IN*/ SQLSTMT_ID * statement_id,
                          /*IN OPTIONAL*/ SQLDESC_ID * output_descriptor,
                          /*IN*/ int num_ptr_pairs,
                          /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_FetchClose(GetCliGlobals(), statement_id, output_descriptor, num_ptr_pairs, 0, VA_LIST_NULL,
                                  ptr_pairs);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_FetchMultiple(/*IN*/ SQLSTMT_ID * statement_id,
                             /*IN  OPTIONAL*/ SQLDESC_ID * output_descriptor,
                             /*IN*/ int rowset_size,
                             /*IN*/ int *rowset_status_ptr,
                             /*OUT*/ int *rowset_nfetched,
                             /*IN*/ int num_quadruple_fields,
                             /*IN*/ int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode = SQLCLI_FetchMultiple(GetCliGlobals(), statement_id, output_descriptor, rowset_size, rowset_status_ptr,
                                     rowset_nfetched, num_quadruple_fields, num_ap, ap, 0);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_FETCHMULTIPLE(/*IN*/ SQLSTMT_ID * statement_id,
                             /*IN  OPTIONAL*/ SQLDESC_ID * output_descriptor,
                             /*IN*/ int rowset_size,
                             /*IN*/ int *rowset_status_ptr,
                             /*OUT*/ int *rowset_nfetched,
                             /*IN*/ int num_quadruple_fields,
                             /*IN*/ SQLCLI_QUAD_FIELDS quad_fields[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_FetchMultiple(GetCliGlobals(), statement_id, output_descriptor, rowset_size, rowset_status_ptr,
                                     rowset_nfetched, num_quadruple_fields, 0, VA_LIST_NULL, quad_fields);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  // Called by the cancel thread.
  // Don't use getCliSemaphore(currContext)->to acquire a critical section.
  // Don't use cliSemaphore->to acquire a critical section.
  // cancelSemaphore is used inside SQLCLI_Cancel.
  int SQL_EXEC_Cancel(/*IN OPTIONAL*/ SQLSTMT_ID * statement_id) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      retcode = SQLCLI_Cancel(GetCliGlobals(), statement_id);
    } catch (...) {
#if defined(_THROW_EXCEPTIONS)
      throw;
#else
    retcode = -CLI_INTERNAL_ERROR;
#endif
    }

    // avoid compete semaphore
    // retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_CANCEL(
      /*IN OPTIONAL*/ SQLSTMT_ID * statement_id) {
    // See comment above  : "COBOL change"
    if (statement_id && (statement_id->name_mode == -1)) statement_id = NULL;

    return SQL_EXEC_Cancel(statement_id);
  };

  int SQL_EXEC_CancelOperation(long transid) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      retcode = SQLCLI_CancelOperation(GetCliGlobals(), transid);
    } catch (...) {
#if defined(_THROW_EXCEPTIONS)
      throw;
#else
    retcode = -CLI_INTERNAL_ERROR;
#endif
    }

    return retcode;
  }

  int SQL_EXEC_GetDescEntryCount(/*IN*/ SQLDESC_ID * sql_descriptor,
                                 /*IN*/ SQLDESC_ID * output_descriptor) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetDescEntryCount(GetCliGlobals(), sql_descriptor, output_descriptor);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(NULL, retcode);
    return retcode;
  }
  int SQL_EXEC_GETDESCENTRYCOUNT(
      /*IN*/ SQLDESC_ID * sql_descriptor,
      /*IN*/ SQLDESC_ID * output_descriptor) {
    return SQL_EXEC_GetDescEntryCount(sql_descriptor, output_descriptor);
  };

  int SQL_EXEC_GetDescEntryCountBasic(/*IN*/ SQLDESC_ID * sql_descriptor,
                                      /*OUT*/ int *num_entries) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetDescEntryCountInt(GetCliGlobals(), sql_descriptor, num_entries);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetDescItem(/*IN*/ SQLDESC_ID * sql_descriptor,
                           /*IN*/ int entry,
                           /* (SQLDESC_ITEM_ID) *IN*/ int what_to_get,
                           /*OUT OPTIONAL*/ void *numeric_value,
                           /*OUT OPTIONAL*/ char *string_value,
                           /*IN OPTIONAL*/ int max_string_len,
                           /*OUT OPTIONAL*/ int *len_of_item,
                           /*IN OPTIONAL*/ int start_from_offset) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetDescItem(GetCliGlobals(), sql_descriptor, entry, what_to_get, numeric_value, string_value,
                                   max_string_len, len_of_item, start_from_offset);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(NULL, retcode);
    return retcode;
  }
  int SQL_EXEC_GETDESCITEM(
      /*IN*/ SQLDESC_ID * sql_descriptor,
      /*IN*/ int entry,
      /*IN* (SQLDESC_ITEM_ID) */ int what_to_get,
      /*OUT OPTIONAL*/ void *numeric_value,
      /*OUT OPTIONAL*/ char *string_value,
      /*IN OPTIONAL*/ int max_string_len,
      /*OUT OPTIONAL*/ int *len_of_item,
      /*IN OPTIONAL*/ int start_from_offset) {
    return SQL_EXEC_GetDescItem(sql_descriptor, entry, what_to_get, numeric_value, string_value, max_string_len,
                                len_of_item, start_from_offset);
  };
  int SQL_EXEC_GetDescItems(/*IN*/ SQLDESC_ID * sql_descriptor,
                            /*IN*/ SQLDESC_ITEM desc_items[],
                            /*IN*/ SQLDESC_ID * value_num_descriptor,
                            /*IN*/ SQLDESC_ID * output_descriptor) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode =
          SQLCLI_GetDescItems(GetCliGlobals(), sql_descriptor, desc_items, value_num_descriptor, output_descriptor);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(NULL, retcode);
    return retcode;
  }
  int SQL_EXEC_GETDESCITEMS(
      /*IN*/ SQLDESC_ID * sql_descriptor,
      /*IN*/ SQLDESC_ITEM desc_items[],
      /*IN*/ SQLDESC_ID * value_num_descriptor,
      /*IN*/ SQLDESC_ID * output_descriptor) {
    return SQL_EXEC_GetDescItems(sql_descriptor, desc_items, value_num_descriptor, output_descriptor);
  };
  int SQL_EXEC_GetDescItems2(/*IN*/ SQLDESC_ID * sql_descriptor,
                             /*IN*/ int no_of_desc_items,
                             /*IN*/ SQLDESC_ITEM desc_items[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetDescItems2(GetCliGlobals(), sql_descriptor, no_of_desc_items, desc_items);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(NULL, retcode);
    return retcode;
  }
  int SQL_EXEC_GETDESCITEMS2(
      /*IN*/ SQLDESC_ID * sql_descriptor,
      /*IN*/ int no_of_desc_items,
      /*IN*/ SQLDESC_ITEM desc_items[]) {
    return SQL_EXEC_GetDescItems2(sql_descriptor, no_of_desc_items, desc_items);
  };
  int SQL_EXEC_GetDiagnosticsStmtInfo(/*IN*/ int *stmt_info_items,
                                      /*IN*/ SQLDESC_ID *output_descriptor) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetDiagnosticsStmtInfo(GetCliGlobals(), stmt_info_items, output_descriptor);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetDiagnosticsStmtInfo2(
      /*IN OPTIONAL*/ SQLSTMT_ID * statement_id,
      /*IN* (SQLDIAG_STMT_INFO_ITEM_ID) */ int what_to_get,
      /*OUT OPTIONAL*/ void *numeric_value,
      /*OUT OPTIONAL*/ char *string_value,
      /*IN OPTIONAL*/ int max_string_len,
      /*OUT OPTIONAL*/ int *len_of_item) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetDiagnosticsStmtInfo2(GetCliGlobals(), statement_id, what_to_get, numeric_value, string_value,
                                               max_string_len, len_of_item);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_GETDIAGNOSTICSSTMTINFO(
      /*IN*/ int *stmt_info_items,
      /*IN*/ SQLDESC_ID *output_descriptor) {
    return SQL_EXEC_GetDiagnosticsStmtInfo(stmt_info_items, output_descriptor);
  };

  int SQL_EXEC_GETDIAGNOSTICSSTMTINFO2(
      /*IN OPTIONAL*/ SQLSTMT_ID * statement_id,
      /*IN* (SQLDIAG_STMT_INFO_ITEM_ID) */ int what_to_get,
      /*OUT OPTIONAL*/ void *numeric_value,
      /*OUT OPTIONAL*/ char *string_value,
      /*IN OPTIONAL*/ int max_string_len,
      /*OUT OPTIONAL*/ int *len_of_item) {
    return SQL_EXEC_GetDiagnosticsStmtInfo2(statement_id, what_to_get, numeric_value, string_value, max_string_len,
                                            len_of_item);
  };

  // same method as in Cli.cpp
  static void copyResultString(char *dest, const char *src, int destLen, int *copyLen = NULL) {
    assert(dest != NULL);

    if (copyLen) *copyLen = 0;
    if (src) {
      int len = MINOF(str_len(src) + 1, destLen);
      str_cpy_all(dest, src, len);
      if (copyLen) *copyLen = len;
    } else
      *dest = '\0';
  }

  //
  // GetAutoSizedCondInfo
  //
  // Handles all of the details of GetDiagnosticsCondInfo, resizing
  // and unpacking the diagnostics area.  (refactored out of
  // SQL_EXEC_GetDiagnosticsCondInfo)
  //

  static int GetAutoSizedCondInfo(int *retcode, SQLDIAG_COND_INFO_ITEM *cond_info_items,
                                  SQLDESC_ID *cond_num_descriptor, SQLDESC_ID *output_descriptor,
                                  ComDiagsArea *diagsArea, int try_count, DiagsConditionItem **condition_item_array,
                                  SQLMXLoggingArea::ExperienceLevel *emsEventEL) {
    NABoolean resize = TRUE;
    const int try_size = 512;
    int condition_item_count_needed, condition_item_count = try_count;
    IpcMessageObjSize message_obj_size_needed, message_obj_size = try_size;
    IpcMessageBufferPtr message_buffer_ptr = new char[try_size];
    IpcMessageObjType message_obj_type;
    IpcMessageObjVersion message_obj_version;

    *retcode = 0;
    // The array pointed to by condition_item_array and the buffer pointed to by
    // message_buffer_ptr have been allocated with sizes that should be adequate
    // most of the time. If either is not large enough, SQLCLI_GetDiagnosticsCondInfo
    // will return with the sizes that are needed. When we detect this we will resize,
    // and call again.

    // Can't this be refactored to a simple if-statment, rather than a loop? -KBC

    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    while (*retcode == 0 && resize) {
      try {
        tmpSemaphore = getCliSemaphore(threadContext);
        tmpSemaphore->get();
        threadContext->incrNumOfCliCalls();
        *retcode = SQLCLI_GetDiagnosticsCondInfo(
            GetCliGlobals(), cond_info_items, cond_num_descriptor, output_descriptor, message_buffer_ptr,
            message_obj_size, &message_obj_size_needed, &message_obj_type, &message_obj_version, condition_item_count,
            &condition_item_count_needed, (*condition_item_array), emsEventEL);
      } catch (...) {
        *retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
        if (cliWillThrow()) {
          threadContext->decrNumOfCliCalls();
          tmpSemaphore->release();
          throw;
        }
#endif
      }

      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();
      if (*retcode != 0) continue;
      resize = FALSE;
      if (condition_item_count_needed > condition_item_count) {
        resize = TRUE;
        delete[](*condition_item_array);
        condition_item_count = condition_item_count_needed;
        (*condition_item_array) = new DiagsConditionItem[condition_item_count];
      }
      if (message_obj_size_needed > message_obj_size) {
        resize = TRUE;
        delete[] message_buffer_ptr;
        message_obj_size = message_obj_size_needed;
        message_buffer_ptr = new char[message_obj_size];
      }
    }  // while (*retcode == 0 && resize)
    if (condition_item_count_needed > 0) {
      // The actual diags area resides in the context heap and is inaccessible
      // here in non-priv code. We must unpack the copy which has been
      // packed for us in the buffer pointed to by message_buffer_ptr.
      diagsArea->unpackObj(message_obj_type, message_obj_version, TRUE, message_obj_size, message_buffer_ptr);
    }
    delete[] message_buffer_ptr;
    return (condition_item_count_needed);
  }

  int SQL_EXEC_GetDiagnosticsCondInfo(
      /*IN*/ SQLDIAG_COND_INFO_ITEM * cond_info_items,
      /*IN*/ SQLDESC_ID * cond_num_descriptor,
      /*IN*/ SQLDESC_ID * output_descriptor) {
    int retcode = 0;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    NAHeap heap("NonPriv CLI", NAMemory::DERIVED_FROM_SYS_HEAP, 32 * 1024);

    int msgInLocaleLen = 0;
    NABoolean msgInLocaleLenIsAvailable = FALSE;

    int condition_item_count;
    const int try_count = 16;  // starting size; GetAutoSizeCondInfo may resize the array
    DiagsConditionItem *condition_item_array = new DiagsConditionItem[try_count];
    NAWchar *p = NULL;
    SQLMXLoggingArea::ExperienceLevel emsEventEL = SQLMXLoggingArea::eAdvancedEL;

    ComDiagsArea *diagsArea = ComDiagsArea::allocate(&heap);

    if ((condition_item_count = GetAutoSizedCondInfo(&retcode, cond_info_items, cond_num_descriptor, output_descriptor,
                                                     diagsArea, try_count, &condition_item_array, &emsEventEL)) > 0) {
      for (int i = 0; i < condition_item_count && retcode == 0; i++) {
        ComCondition &condition = (*diagsArea)[condition_item_array[i].condition_index];
        // if (condition.getIso88591MappingCharSet() == CharInfo::UnknownCharSet)
        // {
        //   // store the iso_mapping cqd setting to the ComCondition object
        //   // for later use by its methods to do the charset conversion
        //   condition.setIso88591MappingCharSet(isoMapCS);
        // }
        switch (condition_item_array[i].item_id) {
          case SQLDIAG_RET_SQLSTATE: /* (string ) returned SQLSTATE */
            ComSQLSTATE(condition.getSQLCODE(), condition_item_array[i].var_ptr);
            break;
          case SQLDIAG_CLASS_ORIG: /* (string ) class origin, e.g. ISO 9075 */
            copyResultString(condition_item_array[i].var_ptr, ComClassOrigin(condition.getSQLCODE()),
                             condition_item_array[i].length);
            break;
          case SQLDIAG_SUBCLASS_ORIG: /* (string ) subclass origin, e.g. ISO 9075 */
            copyResultString(condition_item_array[i].var_ptr, ComSubClassOrigin(condition.getSQLCODE()),
                             condition_item_array[i].length);
            break;

          // R2
          case SQLDIAG_MSG_TEXT: /* (string ) message text */

            logAnMXEventForError(condition, emsEventEL);

            msgInLocaleLenIsAvailable = TRUE;
            p = (NAWchar *)(condition.getMessageText(TRUE,              // NABoolean prefixAdded
                                                     CharInfo::UTF8));  // msg in UCS2

            if (p && condition_item_array[i].length > 0) {
              int wMsgLen = condition.getMessageLength();  // msg length in UCS2 characters

              //     	    assert (condition_item_array[i].charset != CharInfo::UnknownCharSet);

              // Do a conversion if the target charset is not UNICODE.
              // If the target charset is UNICODE, just do a str cpy.
              switch (condition_item_array[i].charset) {
                case CharInfo::UNICODE: {
                  // condition_item_array[i].length gives the available
                  // space (in chars) for the msg.
                  int wCharToConvert = MINOF(condition_item_array[i].length, wMsgLen);
                  na_wcsncpy((NAWchar *)condition_item_array[i].var_ptr, (NAWchar *)p, wCharToConvert);
                  msgInLocaleLen = wCharToConvert;
                  if (wMsgLen < condition_item_array[i].length)
                    ((wchar_t *)(condition_item_array[i].var_ptr))[msgInLocaleLen] = 0;
                } break;

                default: {
                  // Convert the message from UCS2 to ISO_MAPPING charset
                  //                               to UTF8 character set

                  {
                    int cnvErrStatus = 0;
                    char *pFirstUntranslatedChar = NULL;

                    cnvErrStatus = SQL_EXEC_UTF16ToLocale(
                        cnv_UTF8,                                 // IN int conv_charset of output
                        ((void *)p),                              // IN void * Input_Buffer_Addr
                        wMsgLen * 2 /* bytes per UCS2 char */,    // IN int Input_Buffer_Octet_Length
                        (void *)condition_item_array[i].var_ptr,  // I/O void * Output_Buffer_Addr
                        condition_item_array[i].length,           // IN int Output_Buffer_Octet_Length
                        (void **)&pFirstUntranslatedChar,         // OUT void * * First_Untranslated_Char_Addr
                        &msgInLocaleLen,                          // OUT int * Output_Data_Octet_Length
                        0,                                        // IN int conv_flags
                        (int)FALSE,                               // IN int add_null_at_end_Flag
                        (int)TRUE,                                // IN int allow_invalids
                        (int *)NULL,                              // OUT int * num_translated_char
                        NULL                                      // IN void * substitution_char_addr
                    );                                            //            use ? as the substitute char
                    switch (cnvErrStatus) {
                      case 0:  // success
                        // append NULL at the end if there is enough room
                        if (condition_item_array[i].length > msgInLocaleLen)
                          ((char *)(condition_item_array[i].var_ptr))[msgInLocaleLen] = '\0';
                        break;
                      case -1:  // CNV_ERR_INVALID_CHAR    Character in input cannot be converted
                                // impossible condition
                                // we enable the "allow invalid characters" option which will
                                // substitute every invalid character with the ? character.
                      case -2:  // CNV_ERR_BUFFER_OVERRUN  No output buffer or not big enough
                                // No problem, a truncated message is better than nothing
                                // The truncated message does not include the NULL terminator
                                // assert (msgInLocaleLen == condition_item_array[i].length);
                      case -3:  // CNV_ERR_NOINPUT         No input buffer or input cnt <= 0
                                // assert (msgInLocaleLen == 0 && (wMsgLen == 0 || NAWstrlen(p) == 0));
                      case -4:  // CNV_ERR_INVALID_CS      Invalid Character Set specified
                      case -5:  // CNV_ERR_INVALID_VERS    Invalid version specified
                      default:
                        // impossible conditions
                        break;
                    }  // switch
                  }
                  if (msgInLocaleLen == 0 && wMsgLen > 0 && condition_item_array[i].length > 80) {  // UTF8
                    char *pMsg = (char *)condition_item_array[i].var_ptr;                           // SJIS
                    const char *initMsg =                                                           // ISO88591
                        "*** ERROR[3066] Unable to convert error message from UTF16 to ";           // NULL
                    // 012345678901234567890123456789012345678901234567890123456789012345678901
                    //          1         3         4         5         6         7         8
                    pMsg[0] = '\0';
                    str_cat(pMsg, initMsg, pMsg);
                    str_cat(pMsg, SQLCHARSETSTRING_UTF8, pMsg);
                    msgInLocaleLen = str_len(pMsg);
                  }  // if (msgInLocaleLen == 0 && wMsgLen > 0 && ... > 80)
                } break;
              }
              // end of switch (condition_item_array[i].charset)

              if (msgInLocaleLen == 0) *(char *)condition_item_array[i].var_ptr = 0;
            } else {
              *(char *)condition_item_array[i].var_ptr = 0;
            }

            break;

          // R2
          // the message length is the same regardless of the charset
          // it is true for R2 and the foreseen post R2.
          case SQLDIAG_MSG_LEN: /* (numeric) message length in characters */
            try {
              tmpSemaphore = getCliSemaphore(threadContext);
              tmpSemaphore->get();
              threadContext->incrNumOfCliCalls();
              retcode = SQLCLI_OutputValueIntoNumericHostvar(GetCliGlobals(), output_descriptor,
                                                             condition_item_array[i].output_entry + 1,
                                                             condition.getMessageLength());
            } catch (...) {
              retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
              if (cliWillThrow()) {
                threadContext->decrNumOfCliCalls();
                tmpSemaphore->release();
                throw;
              }
#endif
            }
            threadContext->decrNumOfCliCalls();
            tmpSemaphore->release();
            retcode = RecordError(NULL, retcode);
            break;

          case SQLDIAG_MSG_OCTET_LEN: /* (numeric) message length in bytes */

            try {
              tmpSemaphore = getCliSemaphore(threadContext);
              tmpSemaphore->get();
              threadContext->incrNumOfCliCalls();
              retcode = SQLCLI_OutputValueIntoNumericHostvar(GetCliGlobals(), output_descriptor,
                                                             condition_item_array[i].output_entry + 1, msgInLocaleLen);
            } catch (...) {
              retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
              if (cliWillThrow()) {
                threadContext->decrNumOfCliCalls();
                tmpSemaphore->release();
                throw;
              }
#endif
            }
            threadContext->decrNumOfCliCalls();
            tmpSemaphore->release();

            if (msgInLocaleLenIsAvailable == FALSE) {
              retcode = -CLI_INTERNAL_ERROR;
            }

            retcode = RecordError(NULL, retcode);

            break;
        }  // switch (condition_item_array[i].item_id)
      }    // for (int i = 0; i < condition_item_count && retcode == 0; i++)
    }      // if (condition_item_count > 0)
    delete[] condition_item_array;

    diagsArea->clear();

    return retcode;
  }

  int SQL_EXEC_GETDIAGNOSTICSCONDINFO(
      /*IN*/ SQLDIAG_COND_INFO_ITEM * cond_info_items,
      /*IN*/ SQLDESC_ID * cond_num_descriptor,
      /*IN*/ SQLDESC_ID * output_descriptor) {
    return SQL_EXEC_GetDiagnosticsCondInfo(cond_info_items, cond_num_descriptor, output_descriptor);
  };

  int SQL_EXEC_GetDiagnosticsCondInfo2(
      /*IN* (SQLDIAG_COND_INFO_ITEM_ID) */ int what_to_get,
      /*IN*/ int conditionNum,
      /*OUT OPTIONAL*/ int *numeric_value,
      /*OUT OPTIONAL*/ char *string_value,
      /*IN OPTIONAL */ int max_string_len,
      /*OUT OPTIONAL*/ int *len_of_item) {
    int retcode = 0;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      switch (what_to_get) {
        case SQLDIAG_RET_SQLSTATE:  /* (string ) returned SQLSTATE */
        case SQLDIAG_CLASS_ORIG:    /* (string ) class origin, e.g. ISO 9075 */
        case SQLDIAG_SUBCLASS_ORIG: /* (string ) subclass origin, e.g. ISO 9075*/
        case SQLDIAG_MSG_TEXT:      /* (string ) message text */
        case SQLDIAG_MSG_LEN:       /* (numeric) message length in characters */
        case SQLDIAG_MSG_OCTET_LEN: /* (numeric) message length in bytes */
        {
          int rc;

          // for these cases, call GetDiagnosticsCondInfo().
          SQLDIAG_COND_INFO_ITEM cond_info_item;
          cond_info_item.item_id = what_to_get;
          cond_info_item.cond_number_desc_entry = 1;

          SQLMODULE_ID module;
          module.module_name = 0;
          module.module_name_len = 0;

          SQLDESC_ID cond_num_descriptor = {1, desc_name, &module, "dummy_input_name", 0, 0, 10};
          int i = conditionNum;
          rc = SQL_EXEC_AllocDesc(&cond_num_descriptor, 1);
          rc = SQL_EXEC_SetDescItem(&cond_num_descriptor, 1, SQLDESC_TYPE_FS, REC_BIN32_SIGNED, 0);
          rc = SQL_EXEC_SetDescPointers(&cond_num_descriptor, 1, 1, 2, &(i), 0);

          SQLDESC_ID output_descriptor = {1, desc_name, &module, "dummy_output_name", 0, 0, 11};
          rc = SQL_EXEC_AllocDesc(&output_descriptor, 1);
          if ((what_to_get == SQLDIAG_MSG_LEN) || (what_to_get == SQLDIAG_MSG_OCTET_LEN)) {
            rc = SQL_EXEC_SetDescItem(&output_descriptor, 1, SQLDESC_TYPE_FS, REC_BIN32_SIGNED, 0);
            rc = SQL_EXEC_SetDescPointers(&output_descriptor, 1, 1, 2, numeric_value, 0);
          } else {
            rc = SQL_EXEC_SetDescItem(&output_descriptor, 1, SQLDESC_TYPE_FS, REC_MIN_V_N_CHAR_H, 0);
            rc = SQL_EXEC_SetDescItem(&output_descriptor, 1, SQLDESC_LENGTH, max_string_len, 0);
            rc = SQL_EXEC_SetDescPointers(&output_descriptor, 1, 1, 2, string_value, 0);
          }

          rc = SQL_EXEC_GetDiagnosticsCondInfo(&cond_info_item, &cond_num_descriptor, &output_descriptor);

          if ((what_to_get == SQLDIAG_RET_SQLSTATE) || (what_to_get == SQLDIAG_CLASS_ORIG) ||
              (what_to_get == SQLDIAG_SUBCLASS_ORIG) || (what_to_get == SQLDIAG_MSG_TEXT)) {
            if (len_of_item) *len_of_item = str_len(string_value);
          }

          rc = SQL_EXEC_DeallocDesc(&cond_num_descriptor);
          rc = SQL_EXEC_DeallocDesc(&output_descriptor);
        } break;

        default: {
          retcode = SQLCLI_GetDiagnosticsCondInfo2(GetCliGlobals(), what_to_get, conditionNum, numeric_value,
                                                   string_value, max_string_len, len_of_item);
        }
      }
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_GETDIAGNOSTICSCONDINFO2(
      /*IN* (SQLDIAG_COND_INFO_ITEM_ID) */ int what_to_get,
      /*IN*/ int conditionNum,
      /*OUT OPTIONAL*/ int *numeric_value,
      /*OUT OPTIONAL*/ char *string_value,
      /*IN OPTIONAL */ int max_string_len,
      /*OUT OPTIONAL*/ int *len_of_item) {
    return SQL_EXEC_GetDiagnosticsCondInfo2(what_to_get, conditionNum, numeric_value, string_value, max_string_len,
                                            len_of_item);
  }

  int SQL_EXEC_GetDiagnosticsCondInfo3(
      /*IN*/ int no_of_condition_items,
      /*IN*/ SQLDIAG_COND_INFO_ITEM_VALUE diag_cond_info_item_values[]) {
    int retcode = 0;
    IpcMessageObjSize message_obj_size_needed = 0;
    IpcMessageObjSize message_obj_size;
    IpcMessageBufferPtr message_buffer_ptr;
    IpcMessageObjType message_obj_type;
    IpcMessageObjVersion message_obj_version;
    ComDiagsArea *diagsArea;
    SQLDIAG_COND_INFO_ITEM_VALUE diagItemValues;
    int max_string_len;
    int what_to_get;
    int conditionNum;
    int *numeric_value;
    int *len_of_item;
    char *string_value;
    NAWchar *p = NULL;
    NABoolean msgInLocaleLenIsAvailable = FALSE;
    int msgInLocaleLen = 0;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetDiagnosticsCondInfo3(GetCliGlobals(), no_of_condition_items, diag_cond_info_item_values,
                                               &message_obj_size_needed);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    retcode = RecordError(NULL, retcode);

    if (message_obj_size_needed > 0) {
      NAHeap heap("NonPriv CLI", NAMemory::DERIVED_FROM_SYS_HEAP, message_obj_size_needed * 2);
      // We multiply by two to account for packing
      diagsArea = ComDiagsArea::allocate(&heap);
      message_buffer_ptr = new char[message_obj_size_needed];
      message_obj_size = message_obj_size_needed;

      SQLMXLoggingArea::ExperienceLevel emsEventEL = SQLMXLoggingArea::eAdvancedEL;
      CLISemaphore *tmpSemaphore = NULL;
      ContextCli *threadContext;

      try {
        tmpSemaphore = getCliSemaphore(threadContext);
        tmpSemaphore->get();
        threadContext->incrNumOfCliCalls();
        retcode = SQLCLI_GetDiagnosticsArea(GetCliGlobals(), message_buffer_ptr, message_obj_size, &message_obj_type,
                                            &message_obj_version);
      } catch (...) {
        retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
        if (cliWillThrow()) {
          delete[] message_buffer_ptr;
          diagsArea->clear();
          diagsArea->deAllocate();
          threadContext->decrNumOfCliCalls();
          tmpSemaphore->release();
          throw;
        }
#endif
      }

      threadContext->decrNumOfCliCalls();
      tmpSemaphore->release();

      retcode = RecordError(NULL, retcode);
      if (retcode < 0) {
        delete[] message_buffer_ptr;
        diagsArea->clear();
        diagsArea->deAllocate();
        return retcode;
      }

      // The actual diags area resides in the context heap and is inaccessible
      // here in non-priv code. We must unpack the copy which has been
      // packed for us in the buffer pointed to by message_buffer_ptr.
      diagsArea->unpackObj(message_obj_type, message_obj_version, TRUE, message_obj_size_needed, message_buffer_ptr);

      for (int i = 0; i < no_of_condition_items && retcode == 0; i++) {
        diagItemValues = diag_cond_info_item_values[i];
        if (diagItemValues.num_val_or_len == NULL) {
          delete[] message_buffer_ptr;
          diagsArea->clear();
          diagsArea->deAllocate();
          return -CLI_INVALID_ATTR_VALUE;
        }
        string_value = diagItemValues.string_val;
        max_string_len = string_value ? *(diagItemValues.num_val_or_len) : 0;
        conditionNum = diagItemValues.item_id_and_cond_number.cond_number_desc_entry;
        ComCondition &condition = diagsArea->operator[](conditionNum);
        what_to_get = diagItemValues.item_id_and_cond_number.item_id;
        numeric_value = diagItemValues.num_val_or_len;
        len_of_item = diagItemValues.num_val_or_len;

        // if (condition.getIso88591MappingCharSet() == CharInfo::UnknownCharSet)
        // {
        //   // store the iso_mapping cqd setting to the ComCondition object
        //   // for later use by its methods to do the charset conversion
        //   condition.setIso88591MappingCharSet(isoMapCS);
        // }

        switch (what_to_get) {
          case SQLDIAG_RET_SQLSTATE: /* (string ) returned SQLSTATE */
          {
            if (condition.getCustomSQLState() == NULL) {
              if (max_string_len < 6) {
                delete[] message_buffer_ptr;
                diagsArea->clear();
                diagsArea->deAllocate();
                return -CLI_INVALID_ATTR_VALUE;
              }
              ComSQLSTATE(condition.getSQLCODE(), string_value);
              if (len_of_item) *len_of_item = 6;
            } else {
              copyResultString(string_value, condition.getCustomSQLState(), max_string_len, len_of_item);
            }
          } break;

            /* GetDiagnosticsCondInfo3 currently returns sqlcode or a part of it for
            these condition items. They do not appear to be fully supported yet. */
          case SQLDIAG_CLASS_ORIG: /* (string ) class origin, e.g. ISO 9075 */
            copyResultString(string_value, condition.getClassOrigin(), max_string_len, len_of_item);
            break;

          case SQLDIAG_SUBCLASS_ORIG: /* (string ) subclass origin, e.g. ISO 9075*/
            copyResultString(string_value, condition.getSubClassOrigin(), max_string_len, len_of_item);
            break;

          // Currently this method assumes that the target is ISO_MAPPING CQD CharSet
          // - For SeaQuest, the target character set is UTF8.
          // This CLI call is currently used only by ODBC/JDBC
          // To make this method support other charsets, we will need to change the signature
          // of this method. This asumption affects the next three cases (SQLDIAG_MSG_TEXT,
          // SQLDIAG_MSG_LEN, and SQLDIAG_MSG_OCTET_LEN)
          case SQLDIAG_MSG_TEXT: /* (string ) message text */
          {
            logAnMXEventForError(condition, emsEventEL);

            msgInLocaleLenIsAvailable = TRUE;
            p = (NAWchar *)(condition.getMessageText(TRUE,              // NABoolean prefixAdded
                                                     CharInfo::UTF8));  // msg in UCS2

            if (p && max_string_len > 0) {
              int wMsgLen = condition.getMessageLength();  // msg length in UCS2 characters

              {
                int cnvErrStatus = 0;
                char *pFirstUntranslatedChar = NULL;

                cnvErrStatus = SQL_EXEC_UTF16ToLocale(
                    cnv_UTF8,
                    ((void *)p),                            // IN void * Input_Buffer_Addr
                    wMsgLen * 2 /* bytes per UCS2 char */,  // IN int Input_Buffer_Octet_Length
                    ((void *)string_value),                 // I/O void * Output_Buffer_Addr
                    max_string_len,                         // IN int Output_Buffer_Octet_Length
                    (void **)&pFirstUntranslatedChar,       // OUT void * * First_Untranslated_Char_Addr
                    &msgInLocaleLen,                        // OUT int Output_Data_Octet_Length
                    0,                                      // IN int conv_flags
                    (int)FALSE,                             // IN int add_null_at_end_Flag
                    (int)TRUE,                              // IN int allow_invalids
                    (int *)NULL,                            // OUT int * num_translated_char
                    NULL                                    // IN void * substitution_char_addr
                );                                          //            use ? as the substitute char
                switch (cnvErrStatus) {
                  case 0:  // success
                    // append NULL at the end if there is enough room
                    if (max_string_len > msgInLocaleLen) string_value[msgInLocaleLen] = '\0';
                    break;
                  case -1:  // CNV_ERR_INVALID_CHAR    Character in input cannot be converted
                            // impossible condition
                            // we enable the "allow invalid characters" option which will
                            // substitute every invalid character with the ? character.
                  case -2:  // CNV_ERR_BUFFER_OVERRUN  No output buffer or not big enough
                            // No problem, a truncated message is better than nothing
                            // The truncated message does not include the NULL terminator
                            // assert (msgInLocaleLen == max_string_len);
                  case -3:  // CNV_ERR_NOINPUT         No input buffer or input cnt <= 0
                            // assert (msgInLocaleLen == 0 && (wMsgLen == 0 || NAWstrlen(p) == 0));
                  case -4:  // CNV_ERR_INVALID_CS      Invalid Character Set specified
                  case -5:  // CNV_ERR_INVALID_VERS    Invalid version specified
                  default:
                    // impossible conditions
                    break;
                }  // switch
              }
              if (msgInLocaleLen == 0 && wMsgLen > 0 && max_string_len > 80) {         // UTF8
                char *pMsg = string_value;                                             // SJIS
                const char *initMsg =                                                  // ISO88591
                    "*** ERROR[3066] Unable to convert error message from UTF16 to ";  // NULL
                // 012345678901234567890123456789012345678901234567890123456789012345678901
                //          1         3         4         5         6         7         88
                pMsg[0] = '\0';
                str_cat(pMsg, initMsg, pMsg);
                str_cat(pMsg, SQLCHARSETSTRING_UTF8, pMsg);
                msgInLocaleLen = str_len(pMsg);
              }  // if (msgInLocaleLen == 0 && wMsgLen > 0 && max_string_len > 80)
            }

            if (msgInLocaleLen == 0) *string_value = 0;
            if (len_of_item) *len_of_item = msgInLocaleLen;
          } break;

          case SQLDIAG_MSG_LEN: /* (numeric) message length in characters */
            *numeric_value = condition.getMessageLength();
            break;

          case SQLDIAG_MSG_OCTET_LEN: /* (numeric) message length in bytes */
            *numeric_value = msgInLocaleLen;
            if (msgInLocaleLenIsAvailable == FALSE) {
              retcode = -CLI_INTERNAL_ERROR;
            }
            break;
        }
      }
      delete[] message_buffer_ptr;
      diagsArea->clear();
      diagsArea->deAllocate();
    }

    return retcode;
  }
  int SQL_EXEC_GETDIAGNOSTICSCONDINFO3(
      /*IN*/ int no_of_condition_items,
      /*IN*/ SQLDIAG_COND_INFO_ITEM_VALUE diag_cond_info_item_values[]) {
    return SQL_EXEC_GetDiagnosticsCondInfo3(no_of_condition_items, diag_cond_info_item_values);
  }

  int SQL_EXEC_GetMainSQLSTATE(
      /*IN*/ SQLSTMT_ID * stmtId,
      /*IN*/ int sqlcode,
      /*OUT*/ char *sqlstate /* assumed to be char[6] */) {
    int retcode = 0;

    CLI_NONPRIV_PROLOGUE(retcode);

    char localSQLSTATE[6] = "\0";

    // Since cliglobals and executor context are not accessible from
    // here, this part needs to be rewritten similar to
    // SQL_EXEC_GetDiagnosticsCondInfo. TBD.
    // For now, just return the regular sqlstate.
    ComSQLSTATE(sqlcode, localSQLSTATE);

    str_cpy_all(sqlstate, localSQLSTATE, 5);

    return retcode;
  }

  int SQL_EXEC_GETMAINSQLSTATE(
      /*IN*/ SQLSTMT_ID * statement_id,
      /*IN*/ int sqlcode,
      /*OUT*/ char *sqlstate /* assumed to be char[6] */) {
    return SQL_EXEC_GetMainSQLSTATE(statement_id, sqlcode, sqlstate);
  }

  int SQL_EXEC_GetCSQLSTATE(/*OUT*/ char *theSQLSTATE /* assumed char[6] */,
                            /*IN*/ int theSQLCODE) {
    int retcode = 0;

    CLI_NONPRIV_PROLOGUE(retcode);

    ComSQLSTATE(theSQLCODE, theSQLSTATE);
    return retcode;
  }

  int SQL_EXEC_GETCSQLSTATE(
      /*OUT*/ char *sqlstate /* assumed to be char[6] */,
      /*IN*/ int sqlcode) {
    return SQL_EXEC_GetCSQLSTATE(sqlstate, sqlcode);
  };

  int SQL_EXEC_GetCobolSQLSTATE(/*OUT*/ char *theSQLSTATE /*assumed char[5]*/,
                                /*IN*/ int theSQLCODE) {
    int retcode = 0;

    char localSQLSTATE[6];

    ComSQLSTATE(theSQLCODE, localSQLSTATE);

    str_cpy_all(theSQLSTATE, localSQLSTATE, 5);

    return retcode;
  }

  int SQL_EXEC_GETCOBOLSQLSTATE(
      /*OUT*/ char *sqlstate /* assumed to be char[5] */,
      /*IN*/ int sqlcode) {
    return SQL_EXEC_GetCobolSQLSTATE(sqlstate, sqlcode);
  };

  int SQL_EXEC_GetSQLSTATE(/*OUT*/ char *SQLSTATE /* assumed to be char[6] */) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    int sqlcode;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetSQLCODE(GetCliGlobals(), &sqlcode);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(NULL, retcode);
    ComSQLSTATE(sqlcode, SQLSTATE);
    return retcode;
  }

  int SQL_EXEC_GETSQLSTATE(
      /*OUT*/ char *sqlstate /* assumed to be char[6] */) {
    return SQL_EXEC_GetSQLSTATE(sqlstate);
  };

  int SQL_EXEC_GetSessionAttr(
      /*IN (SESSIONATTR_TYPE )*/ int attrName,
      /*OUT OPTIONAL*/ int *numeric_value,
      /*OUT OPTIONAL*/ char *string_value,
      /*IN OPTIONAL*/ int max_string_len,
      /*OUT OPTIONAL*/ int *len_of_item) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode =
          SQLCLI_GetSessionAttr(GetCliGlobals(), attrName, numeric_value, string_value, max_string_len, len_of_item);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetAuthID(const char *authName, int &authID)

  {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetAuthID(GetCliGlobals(), authName, authID);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetAuthName_Internal(int auth_id, char *string_value, int max_string_len, int &len_of_item)

  {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetAuthName(GetCliGlobals(), auth_id, string_value, max_string_len, len_of_item);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetDatabaseUserName_Internal(
      /*IN*/ int user_id,
      /*OUT*/ char *string_value,
      /*IN*/ int max_string_len,
      /*OUT OPTIONAL*/ int *len_of_item) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetDatabaseUserName(GetCliGlobals(), user_id, string_value, max_string_len, len_of_item);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetDatabaseUserID_Internal(/*IN*/ char *string_value,
                                          /*OUT*/ int *numeric_value) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetDatabaseUserID(GetCliGlobals(), string_value, numeric_value);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetAuthState(
      /*OUT*/ int &authenticationType,
      /*OUT*/ bool &authorizationEnabled,
      /*OUT*/ bool &authorizationReady,
      /*OUT*/ bool &auditingEnabled)

  {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetAuthState(GetCliGlobals(), authenticationType, authorizationEnabled, authorizationReady,
                                    auditingEnabled);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetUserAttrs(
      /*IN*/ const char *username,
      /*IN*/ const char *tenant_name,
      /*OUT*/ USERS_INFO *users_info,
      /*OUT*/ struct SQLSEC_AuthDetails *auth_details) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetUserAttrs(GetCliGlobals(), username, tenant_name, users_info, auth_details);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_RegisterUser(
      /*IN*/ const char *username,
      /*IN*/ const char *config,
      /*OUT*/ USERS_INFO *users_info,
      /*OUT*/ struct SQLSEC_AuthDetails *auth_details) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_RegisterUser(GetCliGlobals(), username, config, users_info, auth_details);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetAuthErrPwdCnt(
      /*IN*/ int userid,
      /*OUT*/ Int16 &errcnt) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetAuthErrPwdCnt(GetCliGlobals(), userid, errcnt);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_UpdateAuthErrPwdCnt(
      /*IN*/ int userid,
      /*IN*/ Int16 errcnt,
      /*IN*/ bool reset) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_UpdateAuthErrPwdCnt(GetCliGlobals(), userid, errcnt, reset);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_SetSessionAttr_Internal(
      /*IN (SESSIONATTR_TYPE)*/ int attrName,
      /*IN OPTIONAL*/ int numeric_value,
      /*IN OPTIONAL*/ const char *string_value) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetSessionAttr(GetCliGlobals(), attrName, numeric_value, string_value);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetRoleList(int &numEntries, int *&roleIDs, int *&granteeIDs)

  {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetRoleList(GetCliGlobals(), numEntries, roleIDs, granteeIDs);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_ResetRoleList_Internal() {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_ResetRoleList(GetCliGlobals());
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetUniqueQueryIdAttrs(
      /*IN*/ char *uniqueQueryId,
      /*IN*/ int uniqueQueryIdLen,
      /*IN*/ int no_of_attrs,
      /*INOUT*/ UNIQUEQUERYID_ATTR unique_queryid_attrs[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetUniqueQueryIdAttrs(GetCliGlobals(), uniqueQueryId, uniqueQueryIdLen, no_of_attrs,
                                             unique_queryid_attrs);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetStmtAttr(/*IN*/ SQLSTMT_ID * statement_id,
                           /*IN (SQLATTR_TYPE )*/ int attrName,
                           /*OUT OPTIONAL*/ int *numeric_value,
                           /*OUT OPTIONAL*/ char *string_value,
                           /*IN OPTIONAL*/ int max_string_len,
                           /*OUT OPTIONAL*/ int *len_of_item) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetStmtAttr(GetCliGlobals(), statement_id, attrName, numeric_value, string_value, max_string_len,
                                   len_of_item);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }
  int SQL_EXEC_GETSTMTATTR(/*IN*/ SQLSTMT_ID * statement_id,
                           /*IN* (SQLATTR_TYPE) */ int attrName,
                           /*OUT OPTIONAL*/ int *numeric_value,
                           /*OUT OPTIONAL*/ char *string_value,
                           /*IN OPTIONAL*/ int max_string_len,
                           /*OUT OPTIONAL*/ int *len_of_item) {
    return SQL_EXEC_GetStmtAttr(statement_id, attrName, numeric_value, string_value, max_string_len, len_of_item);
  }
  int SQL_EXEC_GetStmtAttrs(/*IN*/ SQLSTMT_ID * statement_id,
                            /*IN*/ int number_of_attrs,
                            /*INOUT*/ SQLSTMT_ATTR attrs[],
                            /*OUT OPTIONAL*/ int *num_returned) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetStmtAttrs(GetCliGlobals(), statement_id, number_of_attrs, attrs, num_returned);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }
  int SQL_EXEC_GETSTMTATTRS(/*IN*/ SQLSTMT_ID * statement_id,
                            /*IN*/ int number_of_attrs,
                            /*INOUT*/ SQLSTMT_ATTR attrs[],
                            /*OUT OPTIONAL*/ int *num_returned) {
    return SQL_EXEC_GetStmtAttrs(statement_id, number_of_attrs, attrs, num_returned);
  }
#ifdef __cplusplus
  extern "C" {
#endif
  int SQL_EXEC_GetStatistics(/*IN OPTIONAL*/ SQLSTMT_ID *statement_id,
                             /*INOUT*/ SQL_QUERY_STATISTICS *query_statistics) {
    return -CLI_INTERNAL_ERROR;
  }
  int SQL_EXEC_GETSTATISTICS(
      /*IN OPTIONAL*/ SQLSTMT_ID *statement_id) {
    return SQL_EXEC_GetStatistics(statement_id, NULL);
  };

#ifdef __cplusplus
  }
#endif /*__cplusplus*/

  int SQL_EXEC_GetCurrentCatalog(
      /*OUT */ char *CatalogName,
      /*IN  */ int CatalogNameMaxLen,
      /*OUT */ int &CatalogNameLen,
      /*OUT */ char *SchemaName,
      /*In  */ int SchemaNameMaxLen,
      /*OUT */ int &SchemaNameLen) {
    int retcode = 0;

    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      SessionDefaults *sessiondefaults = threadContext->getSessionDefaults();
      char *catalog = sessiondefaults->getCatalog();
      char *schema = sessiondefaults->getSchema();
      ;

      if (CatalogName != NULL) {
        if (catalog != NULL && strlen(catalog) > 0) {
          strncpy(CatalogName, catalog, CatalogNameMaxLen);
          CatalogNameLen = strlen(CatalogName);
        } else {
          strncpy(CatalogName, TRAFODION_SYSTEM_CATALOG, CatalogNameMaxLen);
          CatalogNameLen = strlen(CatalogName);
        }
      }

      if (SchemaName != NULL) {
        if (schema != NULL) {
          strncpy(SchemaName, schema, SchemaNameMaxLen);
          SchemaNameLen = strlen(SchemaName);
        } else {
          strncpy(SchemaName, SEABASE_SYSTEM_SCHEMA, SchemaNameMaxLen);
          SchemaNameLen = strlen(SchemaName);
        }
      }

    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw();
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  };

  int SQL_EXEC_Prepare(/*IN*/ SQLSTMT_ID * statement_id,
                       /*IN*/ SQLDESC_ID * sql_source) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_Prepare(GetCliGlobals(), statement_id, sql_source);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_Prepare2(/*IN*/ SQLSTMT_ID * statement_id,
                        /*IN*/ SQLDESC_ID * sql_source,
                        /*INOUT*/ char *gencode_ptr,
                        /*IN*/ int gencode_len,
                        /*INOUT*/ int *ret_gencode_len,
                        /*INOUT*/ SQL_QUERY_COST_INFO *query_cost_info,
                        /*INOUT*/ SQL_QUERY_COMPILER_STATS_INFO *comp_stats_info,
                        /*INOUT*/ char *uniqueStmtId,
                        /*INOUT*/ int *uniqueStmtIdLen,
                        /*IN*/ int flags) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_Prepare2(GetCliGlobals(), statement_id, sql_source, gencode_ptr, gencode_len, ret_gencode_len,
                                query_cost_info, comp_stats_info, uniqueStmtId, uniqueStmtIdLen, flags);

      // -2008 is an internal error - don't attempt AQR in case of internal errors
      if ((retcode < 0) && (retcode != -2008)) {
        retcode = SQLCLI_ProcessRetryQuery(GetCliGlobals(), statement_id, retcode, 1, 0, 0, 0);
      }
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_PREPARE(
      /*IN*/ SQLSTMT_ID * statement_id,
      /*IN*/ SQLDESC_ID * sql_source) {
    return SQL_EXEC_Prepare(statement_id, sql_source);
  };

  int SQL_EXEC_GetExplainData(
      /*IN*/ SQLSTMT_ID * statement_id,
      /*INOUT*/ char *explain_ptr,
      /*IN*/ int explain_len,
      /*INOUT*/ int *ret_explain_len) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetExplainData(GetCliGlobals(), statement_id, explain_ptr, explain_len, ret_explain_len);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_StoreExplainData(
      /*IN*/ long *exec_start_utc_ts,
      /*IN*/ char *query_id,
      /*INOUT*/ char *explain_ptr,
      /*IN*/ int explain_len) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_StoreExplainData(GetCliGlobals(), exec_start_utc_ts, query_id, explain_ptr, explain_len);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    retcode = RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_ResDescName(/*INOUT*/ SQLDESC_ID * statement_id,
                           /*IN OPTIONAL*/ SQLSTMT_ID * from_statement,
                           /* (SQLWHAT_DESC) *IN OPTIONAL*/ int what_desc) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_ResDescName(GetCliGlobals(), statement_id, from_statement, what_desc);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_RESDESCNAME(
      /*INOUT*/ SQLDESC_ID * statement_id,
      /*IN OPTIONAL*/ SQLSTMT_ID * from_statement,
      /*IN OPTIONAL (SQLWHAT_DESC) */ int what_desc) {
    return SQL_EXEC_ResDescName(statement_id, from_statement, what_desc);
  };

  int SQL_EXEC_ResStmtName(/*INOUT*/ SQLSTMT_ID * statement_id) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_ResStmtName(GetCliGlobals(), statement_id);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }

  int SQL_EXEC_RESSTMTNAME(
      /*INOUT*/ SQLSTMT_ID * statement_id) {
    return SQL_EXEC_ResStmtName(statement_id);
  };
  int SQL_EXEC_SetCursorName(/*IN*/ SQLSTMT_ID * statement_id,
                             /*IN*/ SQLSTMT_ID * cursor_name) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetCursorName(GetCliGlobals(), statement_id, cursor_name);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    retcode = RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_SETCURSORNAME(
      /*IN*/ SQLSTMT_ID * statement_id,
      /*IN*/ SQLSTMT_ID * cursor_name) {
    return SQL_EXEC_SetCursorName(statement_id, cursor_name);
  };
  int SQL_EXEC_SetStmtAttr(/*IN*/ SQLSTMT_ID * statement_id,
                           /*IN* (SQLATTR_TYPE) */ int attrName,
                           /*IN OPTIONAL*/ int numeric_value,
                           /*IN OPTIONAL*/ char *string_value) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetStmtAttr(GetCliGlobals(), statement_id, attrName, numeric_value, string_value);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    if ((retcode != 0) && (attrName != SQL_ATTR_COPY_STMT_ID_TO_DIAGS)) RecordError(statement_id, retcode);
    return retcode;
  }
  int SQL_EXEC_SETSTMTATTR(/*IN*/ SQLSTMT_ID * statement_id,
                           /*IN* (SQLATTR_TYPE) */ int attrName,
                           /*IN OPTIONAL*/ int numeric_value,
                           /*IN OPTIONAL*/ char *string_value) {
    return SQL_EXEC_SetStmtAttr(statement_id, attrName, numeric_value, string_value);
  }

  int SQL_EXEC_SetDescEntryCount(/*IN*/ SQLDESC_ID * sql_descriptor,
                                 /*IN*/ SQLDESC_ID * input_descriptor) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetDescEntryCount(GetCliGlobals(), sql_descriptor, input_descriptor);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_SETDESCENTRYCOUNT(
      /*IN*/ SQLDESC_ID * sql_descriptor,
      /*IN*/ SQLDESC_ID * input_descriptor) {
    return SQL_EXEC_SetDescEntryCount(sql_descriptor, input_descriptor);
  };

  int SQL_EXEC_SetDescEntryCountBasic(/*IN*/ SQLDESC_ID * sql_descriptor,
                                      /*IN*/ int num_entries) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetDescEntryCountInt(GetCliGlobals(), sql_descriptor, num_entries);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }
  int SQL_EXEC_SetDescItem(/*IN*/ SQLDESC_ID * sql_descriptor,
                           /*IN*/ int entry,
                           /* (SQLDESC_ITEM_ID) *IN*/ int what_to_set,
                           /*IN OPTIONAL*/ Long numeric_value,
                           /*IN OPTIONAL*/ char *string_value) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetDescItem(GetCliGlobals(), sql_descriptor, entry, what_to_set, numeric_value, string_value);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }
  int SQL_EXEC_SETDESCITEM(
      /*IN*/ SQLDESC_ID * sql_descriptor,
      /*IN*/ int entry,
      /*IN* (SQLDESC_ITEM_ID) */ int what_to_set,
      /*IN OPTIONAL*/ Long numeric_value,
      /*IN OPTIONAL*/ char *string_value) {
    return SQL_EXEC_SetDescItem(sql_descriptor, entry, what_to_set, numeric_value, string_value);
  };
  int SQL_EXEC_SetDescItems(/*IN*/ SQLDESC_ID * sql_descriptor,
                            /*IN*/ SQLDESC_ITEM desc_items[],
                            /*IN*/ SQLDESC_ID * value_num_descriptor,
                            /*IN*/ SQLDESC_ID * input_descriptor) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode =
          SQLCLI_SetDescItems(GetCliGlobals(), sql_descriptor, desc_items, value_num_descriptor, input_descriptor);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }
  int SQL_EXEC_SETDESCITEMS(
      /*IN*/ SQLDESC_ID * sql_descriptor,
      /*IN*/ SQLDESC_ITEM desc_items[],
      /*IN*/ SQLDESC_ID * value_num_descriptor,
      /*IN*/ SQLDESC_ID * input_descriptor) {
    return SQL_EXEC_SetDescItems(sql_descriptor, desc_items, value_num_descriptor, input_descriptor);
  };
  int SQL_EXEC_SetDescItems2(
      /*IN*/ SQLDESC_ID * sql_descriptor,
      /*IN*/ int no_of_desc_items,
      /*IN*/ SQLDESC_ITEM desc_items[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetDescItems2(GetCliGlobals(), sql_descriptor, no_of_desc_items, desc_items);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }
  int SQL_EXEC_SETDESCITEMS2(
      /*IN*/ SQLDESC_ID * sql_descriptor,
      /*IN*/ int no_of_desc_items,
      /*IN*/ SQLDESC_ITEM desc_items[])

  {
    return SQL_EXEC_SetDescItems2(sql_descriptor, no_of_desc_items, desc_items);
  };
  int SQL_EXEC_SetDescPointers(/*IN*/ SQLDESC_ID * sql_descriptor,
                               /*IN*/ int starting_entry,
                               /*IN*/ int num_ptr_pairs,
                               /*IN*/ int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode = SQLCLI_SetDescPointers(GetCliGlobals(), sql_descriptor, starting_entry, num_ptr_pairs, num_ap, ap, 0);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }
  int SQL_EXEC_SETDESCPOINTERS(/*IN*/ SQLDESC_ID * sql_descriptor,
                               /*IN*/ int starting_entry,
                               /*IN*/ int num_ptr_pairs,
                               /*IN*/ SQLCLI_PTR_PAIRS ptr_pairs[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetDescPointers(GetCliGlobals(), sql_descriptor, starting_entry, 0, num_ptr_pairs, VA_LIST_NULL,
                                       ptr_pairs);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }
  int SQL_EXEC_SetRowsetDescPointers(SQLDESC_ID * sql_descriptor, int rowset_size, int *rowset_status_ptr,
                                     int starting_entry, int num_quadruple_fields, int num_ap, ...) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();

      va_list ap;
      va_start(ap, num_ap);

      retcode = SQLCLI_SetRowsetDescPointers(GetCliGlobals(), sql_descriptor, rowset_size, rowset_status_ptr,
                                             starting_entry, num_quadruple_fields, num_ap, ap, 0);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_SETROWSETDESCPOINTERS(SQLDESC_ID * sql_descriptor, int rowset_size, int *rowset_status_ptr,
                                     int starting_entry, int num_quadruple_fields, SQLCLI_QUAD_FIELDS quad_fields[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetRowsetDescPointers(GetCliGlobals(), sql_descriptor, rowset_size, rowset_status_ptr,
                                             starting_entry, num_quadruple_fields, 0, VA_LIST_NULL, quad_fields);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_SwitchContext(/*IN*/ SQLCTX_HANDLE ctxt_handle,
                             /*OUT OPTIONAL*/ SQLCTX_HANDLE * prev_ctxt_handle) {
    return SQL_EXEC_SwitchContext_Internal(ctxt_handle, prev_ctxt_handle, FALSE);
  }
  int SQL_EXEC_SWITCHCONTEXT(
      /*IN*/ SQLCTX_HANDLE context_handle,
      /*OUT OPTIONAL*/ SQLCTX_HANDLE * prev_context_handle) {
    return SQL_EXEC_SwitchContext(context_handle, prev_context_handle);
  };
  int SQL_EXEC_Xact(/*IN* (SQLTRANS_COMMAND) */ int command,
                    /*OUT OPTIONAL*/ SQLDESC_ID *transid_descriptor) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_Xact(GetCliGlobals(), command, transid_descriptor);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }
  int SQL_EXEC_XACT(
      /*IN* (SQLTRANS_COMMAND) */ int command,
      /*OUT OPTIONAL*/ SQLDESC_ID *transid_descriptor) {
    return SQL_EXEC_Xact(command, transid_descriptor);
  };

#if 0
int SQL_EXEC_SetAuthID(
   const char * externalUsername,
   const char * databaseUsername,
   const char * authToken,
   int        authTokenLen,
   int        effectiveUserID,
   int        sessionUserID)
{
   return SQL_EXEC_SetAuthID2(externalUsername,
                              databaseUsername,
                              DB__SYSTEMTENANT, // use ESGYNDB as the default tenant
                              authToken,
                              authTokenLen,
                              effectiveUserID,
                              sessionUserID,
                              SYSTEM_TENANT_ID,NULL,NULL,
                              0); // use ESGYNDB as the default tenant

}
#endif

  int SQL_EXEC_SetAuthID2(const USERS_INFO &usersInfo, const char *authToken, int authTokenLen, const char *slaName,
                          const char *profileName, int resetAttributes)

  {
    int retcode = 0;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode =
          SQLCLI_SetAuthID(GetCliGlobals(), usersInfo, authToken, authTokenLen, slaName, profileName, resetAttributes);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  /* temporary functions -- for use by sqlcat simulator only */

  int SQL_EXEC_AllocDesc(/*INOUT*/ SQLDESC_ID * desc_id,
                         /*IN OPTIONAL*/ int max_entries) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_AllocDescInt(GetCliGlobals(), desc_id, max_entries);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetDescEntryCount(/*IN*/ SQLDESC_ID * sql_descriptor,
                                 /*OUT*/ int *num_entries) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetDescEntryCountInt(GetCliGlobals(), sql_descriptor, num_entries);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_SetDescEntryCount(/*IN*/ SQLDESC_ID * sql_descriptor,
                                 /*IN*/ int num_entries) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetDescEntryCountInt(GetCliGlobals(), sql_descriptor, num_entries);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  // For internal use only -- do not document!
  // This method merges the CLI diags area into the caller's diags area
  int SQL_EXEC_MergeDiagnostics_Internal(/*INOUT*/ ComDiagsArea & newDiags) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_MergeDiagnostics(GetCliGlobals(), newDiags);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  // For internal use only -- do not document!
  // This method returns the CLI diags area in packed format
  int SQL_EXEC_GetPackedDiagnostics_Internal(
      /*OUT*/ char *message_buffer_ptr,
      /*IN*/ int message_obj_size,
      /*OUT*/ int *message_obj_size_needed,
      /*OUT*/ int *message_obj_type,
      /*OUT*/ int *message_obj_version) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetPackedDiagnostics(GetCliGlobals(), message_buffer_ptr, message_obj_size,
                                            message_obj_size_needed, message_obj_type, message_obj_version);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  // For internal use only -- do not document!
  void SQL_EXEC_SetParserFlagsForExSqlComp_Internal(int flagbits) {
    SQL_EXEC_SetParserFlagsForExSqlComp_Internal2(flagbits);
  }

  // For internal use only -- do not document!
  int SQL_EXEC_SetParserFlagsForExSqlComp_Internal2(int flagbits) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetParserFlagsForExSqlComp_Internal(GetCliGlobals(), flagbits);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_SwitchContext_Internal(/*IN*/ SQLCTX_HANDLE context_handle,
                                      /*OUT OPTIONAL*/ SQLCTX_HANDLE * prev_context_handle,
                                      /*IN*/ int allowSwitchBackToDefault) {
    int retcode;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      retcode = SQLCLI_SwitchContext(GetCliGlobals(), context_handle, prev_context_handle, allowSwitchBackToDefault);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        throw;
      }
#endif
    }

    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_AssignParserFlagsForExSqlComp_Internal(int flagbits) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_AssignParserFlagsForExSqlComp_Internal(GetCliGlobals(), flagbits);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetParserFlagsForExSqlComp_Internal(int &flagbits) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetParserFlagsForExSqlComp_Internal(GetCliGlobals(), flagbits);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    return retcode;
  }

  // For internal use only -- do not document!
  void SQL_EXEC_ResetParserFlagsForExSqlComp_Internal(int flagbits) {
    SQL_EXEC_ResetParserFlagsForExSqlComp_Internal2(flagbits);
  }

  // For internal use only -- do not document!
  int SQL_EXEC_ResetParserFlagsForExSqlComp_Internal2(int flagbits) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_ResetParserFlagsForExSqlComp_Internal(GetCliGlobals(), flagbits);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetTotalTcbSpace(char *tdb, char *otherInfo) {
    int retcode = 0;
    retcode = SQLCLI_GetTotalTcbSpace(GetCliGlobals(), tdb, otherInfo);
    return retcode;
  }

  // for now include this and build it here
  // #include "CliDll.cpp"

  int SQL_EXEC_GetCollectStatsType_Internal(
      /*OUT*/ int *collectStatsType,
      /*IN*/ SQLSTMT_ID *statement_id) {
    int retcode = 0;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    retcode = SQLCLI_GetCollectStatsType_Internal(GetCliGlobals(), collectStatsType, statement_id);
    return retcode;
  }
  int SQL_EXEC_BreakEnabled_Internal(
      /*IN*/ UInt32 enabled) {
    int retcode = 0;

    retcode = SQLCLI_BreakEnabled(GetCliGlobals(), enabled);
    return retcode;
  }
  int SQL_EXEC_SPBreakReceived_Internal(
      /*OUT*/ UInt32 * breakRecvd) {
    int retcode = 0;

    *breakRecvd = 0;
    return retcode;
  }

  // For internal use only -- do not document!
  int SQL_EXEC_SetEnviron_Internal(int propagate) {
    int retcode = 0;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetEnviron_Internal(GetCliGlobals(), environ, propagate);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    return retcode;
  }

#ifdef __cplusplus
  extern "C" {
#endif
  // For internal use only -- do not document!
  int SQL_EXEC_SETENVIRON_INTERNAL(int propagate) { return SQL_EXEC_SetEnviron_Internal(propagate); }

  int SQL_EXEC_LocaleToUTF8(
      /*IN*/ int conv_charset,
      /*IN*/ void *Input_Buffer_Addr,
      /*IN*/ int Input_Buffer_Length,
      /*IN/OUT*/ void *Output_Buffer_Addr,
      /*IN*/ int Output_Buffer_Length,
      /*OUT*/ void **First_Untranslated_Char_Addr,
      /*OUT*/ int *Output_Data_Length,
      /*IN*/ int add_null_at_end_Flag,
      /*OUT*/ int *num_translated_char) {
    int retcode = 0;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    retcode = SQLCLI_LocaleToUTF8(GetCliGlobals(), conv_charset, Input_Buffer_Addr, Input_Buffer_Length,
                                  Output_Buffer_Addr, Output_Buffer_Length, First_Untranslated_Char_Addr,
                                  Output_Data_Length, add_null_at_end_Flag, num_translated_char);

    return retcode;
  }

  int SQL_EXEC_UTF8ToLocale(
      /*IN*/ int conv_charset,
      /*IN*/ void *Input_Buffer_Addr,
      /*IN*/ int Input_Buffer_Length,
      /*IN/OUT*/ void *Output_Buffer_Addr,
      /*IN*/ int Output_Buffer_Length,
      /*OUT*/ void **First_Untranslated_Char_Addr,
      /*OUT*/ int *Output_Data_Length,
      /*IN*/ int add_null_at_end_Flag,
      /*IN*/ int allow_invalids,
      /*OUT*/ int *num_translated_char,
      /*IN*/ void *substitution_char_addr) {
    int retcode = 0;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    retcode =
        SQLCLI_UTF8ToLocale(GetCliGlobals(), conv_charset, Input_Buffer_Addr, Input_Buffer_Length, Output_Buffer_Addr,
                            Output_Buffer_Length, First_Untranslated_Char_Addr, Output_Data_Length,
                            add_null_at_end_Flag, allow_invalids, num_translated_char, substitution_char_addr);

    return retcode;
  }

  int SQL_EXEC_LocaleToUTF16(
      /*IN*/ int conv_charset,
      /*IN*/ void *Input_Buffer_Addr,
      /*IN*/ int Input_Buffer_Length,
      /*IN/OUT*/ void *Output_Buffer_Addr,
      /*IN*/ int Output_Buffer_Length,
      /*OUT*/ void **First_Untranslated_Char_Addr,
      /*OUT*/ int *Output_Data_Length,
      /*IN*/ int conv_flags,
      /*IN*/ int add_null_at_end_Flag,
      /*OUT*/ int *num_translated_char) {
    int retcode = 0;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    retcode = SQLCLI_LocaleToUTF16(GetCliGlobals(), conv_charset, Input_Buffer_Addr, Input_Buffer_Length,
                                   Output_Buffer_Addr, Output_Buffer_Length, First_Untranslated_Char_Addr,
                                   Output_Data_Length, conv_flags, add_null_at_end_Flag, num_translated_char);

    return retcode;
  }

  int SQL_EXEC_UTF16ToLocale(
      /*IN*/ int conv_charset,
      /*IN*/ void *Input_Buffer_Addr,
      /*IN*/ int Input_Buffer_Length,
      /*IN/OUT*/ void *Output_Buffer_Addr,
      /*IN*/ int Output_Buffer_Length,
      /*OUT*/ void **First_Untranslated_Char_Addr,
      /*OUT*/ int *Output_Data_Length,
      /*IN*/ int conv_flags,
      /*IN*/ int add_null_at_end_Flag,
      /*IN*/ int allow_invalids,
      /*OUT*/ int *num_translated_char,
      /*IN*/ void *substitution_char_addr) {
    int retcode = 0;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    retcode =
        SQLCLI_UTF16ToLocale(GetCliGlobals(), conv_charset, Input_Buffer_Addr, Input_Buffer_Length, Output_Buffer_Addr,
                             Output_Buffer_Length, First_Untranslated_Char_Addr, Output_Data_Length, conv_flags,
                             add_null_at_end_Flag, allow_invalids, num_translated_char, substitution_char_addr);

    return retcode;
  }

  int SQL_EXEC_SetErrorCodeInRTS(
      /*IN*/ SQLSTMT_ID *statement_id,
      /*IN*/ int sqlErrorCode) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetErrorCodeInRTS(GetCliGlobals(), statement_id, sqlErrorCode);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_SetSecInvalidKeys(
      /* IN */ int numSiKeys,
      /* IN */ SQL_QIKEY siKeys[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetSecInvalidKeys(GetCliGlobals(), numSiKeys, siKeys);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_GetSecInvalidKeys(
      /* IN */ long prevTimestamp,
      /* IN/OUT */ SQL_QIKEY siKeys[],
      /* IN */ int maxNumSiKeys,
      /* IN/OUT */ int *returnedNumSiKeys,
      /* IN/OUT */ long *maxTimestamp) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetSecInvalidKeys(GetCliGlobals(), prevTimestamp, siKeys, maxNumSiKeys, returnedNumSiKeys,
                                         maxTimestamp);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_SetObjectEpochEntry(
      /* IN */ int operation,
      /* IN */ int objectNameLength,
      /* IN */ const char *objectName,
      /* IN */ long redefTime,
      /* IN */ long key,
      /* IN */ UInt32 expectedEpoch,
      /* IN */ UInt32 expectedFlags,
      /* IN */ UInt32 newEpoch,
      /* IN */ UInt32 newFlags,
      /* OUT */ UInt32 *maxExpectedEpochFound,
      /* OUT */ UInt32 *maxExpectedFlagsFound) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SetObjectEpochEntry(GetCliGlobals(), operation, objectNameLength, objectName, redefTime, key,
                                           expectedEpoch, expectedFlags, newEpoch, newFlags, maxExpectedEpochFound,
                                           maxExpectedFlagsFound);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_LockObjectDDL(
      /* IN */ const char *objectName,
      /* IN */ short objectType) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_LockObjectDDL(GetCliGlobals(), objectName, objectType);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_UnLockObjectDDL(
      /* IN */ const char *objectName,
      /* IN */ short objectType) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_UnLockObjectDDL(GetCliGlobals(), objectName, objectType);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_LockObjectDML(
      /* IN */ const char *objectName,
      /* IN */ short objectType) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_LockObjectDML(GetCliGlobals(), objectName, objectType);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_ReleaseAllDDLObjectLocks() {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_ReleaseAllDDLObjectLocks(GetCliGlobals());
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_ReleaseAllDMLObjectLocks() {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_ReleaseAllDMLObjectLocks(GetCliGlobals());
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetStatistics2(
      /* IN */ short statsReqType,
      /* IN */ char *statsReqStr,
      /* IN */ int statsReqStrLen,
      /* IN */ short activeQueryNum,
      /* IN */ short statsMergeType,
      /* OUT */ short *statsCollectType,
      /* IN/OUT */ SQLSTATS_DESC sqlStats_desc[],
      /* IN */ int max_stats_desc,
      /* OUT */ int *no_returned_stats_desc) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetStatistics2(GetCliGlobals(), statsReqType, statsReqStr, statsReqStrLen, activeQueryNum,
                                      statsMergeType, statsCollectType, sqlStats_desc, max_stats_desc,
                                      no_returned_stats_desc);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();

    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_GetStatisticsItems(
      /* IN */ short statsReqType,
      /* IN */ char *queryId,
      /* IN */ int queryIdLen,
      /* IN */ int no_of_stats_items,
      /* IN/OUT */ SQLSTATS_ITEM sqlstats_items[]) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetStatisticsItems(GetCliGlobals(), statsReqType, queryId, queryIdLen, no_of_stats_items,
                                          sqlstats_items);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_RegisterQuery(SQLQUERY_ID *queryId, int fragId, int tdbId, int explainTdbId, short collectStatsType,
                             int instNum, int tdbType, char *tdbName, int tdbNameLen) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_RegisterQuery(GetCliGlobals(), queryId, fragId, tdbId, explainTdbId, collectStatsType, instNum,
                                     tdbType, tdbName, tdbNameLen);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_DeregisterQuery(SQLQUERY_ID *queryId, int fragId) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_DeregisterQuery(GetCliGlobals(), queryId, fragId);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  // This method returns the pointer to the CLI ExStatistics area.
  // The returned pointer is a read only pointer, its contents cannot be
  // modified by the caller.
  int SQL_EXEC_GetStatisticsArea_Internal(
      /* IN */ short statsReqType,
      /* IN */ char *statsReqStr,
      /* IN */ int statsReqStrLen,
      /* IN */ short activeQueryNum,
      /* IN */ short statsMergeType,
      /*INOUT*/ const ExStatisticsArea *&exStatsArea) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetStatisticsArea_Internal(GetCliGlobals(), statsReqType, statsReqStr, statsReqStrLen,
                                                  activeQueryNum, statsMergeType, exStatsArea);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetObjectEpochStats_Internal(
      /* IN */ const char *objectName,
      /* IN */ int objectNameLen,
      /* IN */ short cpu,
      /* IN */ bool locked,
      /*INOUT*/ ExStatisticsArea *&exStatsArea) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode =
          SQLCLI_GetObjectEpochStats_Internal(GetCliGlobals(), objectName, objectNameLen, cpu, locked, exStatsArea);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetObjectLockStats_Internal(
      /* IN */ const char *objectName,
      /* IN */ int objectNameLen,
      /* IN */ short cpu,
      /*INOUT*/ ExStatisticsArea *&exStatsArea) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetObjectLockStats_Internal(GetCliGlobals(), objectName, objectNameLen, cpu, exStatsArea);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_GetChildQueryInfo(
      /*IN*/ SQLSTMT_ID *statement_id,
      /*INOUT*/ char *uniqueQueryId,
      /*IN */ int uniqueQueryIdMaxLen,
      /*INOUT*/ int *uniqueQueryIdLen,
      /*INOUT*/ SQL_QUERY_COST_INFO *query_cost_info,
      /*INOUT*/ SQL_QUERY_COMPILER_STATS_INFO *comp_stats_info) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetChildQueryInfo(GetCliGlobals(), statement_id, uniqueQueryId, uniqueQueryIdMaxLen,
                                         uniqueQueryIdLen, query_cost_info, comp_stats_info);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_SWITCH_TO_COMPILER_TYPE(
      /*IN*/ int cmpCntxtType) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SWITCH_TO_COMPILER_TYPE(GetCliGlobals(), cmpCntxtType);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_SWITCH_TO_COMPILER(
      /*IN*/ void *cmpCntxt) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SWITCH_TO_COMPILER(GetCliGlobals(), cmpCntxt);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_SWITCH_BACK_COMPILER() {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SWITCH_BACK_COMPILER(GetCliGlobals());
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_SEcliInterface(SECliQueryType qType, void **cliInterface, const char *inStrParam1,
                              const char *inStrParam2, int inIntParam1, int inIntParam2, char **outStrParam1,
                              char **outStrParam2, int *outIntParam1) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SEcliInterface(GetCliGlobals(), qType, cliInterface, inStrParam1, inStrParam2, inIntParam1,
                                      inIntParam2, outStrParam1, outStrParam2, outIntParam1);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_SeqGenCliInterface(void **cliInterface, void *seqGenAttrs) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_SeqGenCliInterface(GetCliGlobals(), cliInterface, seqGenAttrs);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_OrderSeqXDCCliInterface(void **cliInterface, void *seqGenAttrs, long endValue) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_OrderSeqXDCCliInterface(GetCliGlobals(), cliInterface, seqGenAttrs, endValue);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }
  int SQL_EXEC_GetRoutine(
      /* IN */ const char *serializedInvocationInfo,
      /* IN */ int invocationInfoLen,
      /* IN */ const char *serializedPlanInfo,
      /* IN */ int planInfoLen,
      /* IN */ int language,
      /* IN */ int paramStyle,
      /* IN */ const char *externalName,
      /* IN */ const char *containerName,
      /* IN */ const char *externalPath,
      /* IN */ const char *librarySqlName,
      /* OUT */ int *handle) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetRoutine(GetCliGlobals(), serializedInvocationInfo, invocationInfoLen, serializedPlanInfo,
                                  planInfoLen, language, paramStyle, externalName, containerName, externalPath,
                                  librarySqlName, handle);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_InvokeRoutine(
      /* IN */ int handle,
      /* IN */ int phaseEnumAsInt,
      /* IN */ const char *serializedInvocationInfo,
      /* IN */ int invocationInfoLen,
      /* OUT */ int *invocationInfoLenOut,
      /* IN */ const char *serializedPlanInfo,
      /* IN */ int planInfoLen,
      /* IN */ int planNum,
      /* OUT */ int *planInfoLenOut,
      /* IN */ char *inputRow,
      /* IN */ int inputRowLen,
      /* OUT */ char *outputRow,
      /* IN */ int outputRowLen) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_InvokeRoutine(GetCliGlobals(), handle, phaseEnumAsInt, serializedInvocationInfo,
                                     invocationInfoLen, invocationInfoLenOut, serializedPlanInfo, planInfoLen, planNum,
                                     planInfoLenOut, inputRow, inputRowLen, outputRow, outputRowLen);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_GetRoutineInvocationInfo(
      /* IN */ int handle,
      /* IN/OUT */ char *serializedInvocationInfo,
      /* IN */ int invocationInfoMaxLen,
      /* OUT */ int *invocationInfoLenOut,
      /* IN/OUT */ char *serializedPlanInfo,
      /* IN */ int planInfoMaxLen,
      /* IN */ int planNum,
      /* OUT */ int *planInfoLenOut) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetRoutineInvocationInfo(GetCliGlobals(), handle, serializedInvocationInfo, invocationInfoMaxLen,
                                                invocationInfoLenOut, serializedPlanInfo, planInfoMaxLen, planNum,
                                                planInfoLenOut);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_PutRoutine(
      /* IN */ int handle) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_PutRoutine(GetCliGlobals(), handle);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_LoadTrafMetadataInCache() {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;
    CLI_NONPRIV_PROLOGUE(retcode);
    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_LoadTrafMetadataInCache(GetCliGlobals());
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }
    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    RecordError(NULL, retcode);
    return retcode;
  }

  void SQL_EXEC_InitGlobals() {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    // CliNonPrivPrologue() only returns 0
    // It generates an assert if unable to set up context.
    retcode = CliNonPrivPrologue();
  }

  int SQL_EXEC_SetClientInfo(
      /*IN*/ const char *clientInfo,
      /*IN*/ int length,
      /*IN OPTIONAL*/ const char *characterset) {
    ContextCli *currContext = GetCliGlobals()->currContext();
    if (length == 0) return 0;
    currContext->setClientInfo(clientInfo, length, characterset);
    return 0;
  }

  char *SQL_EXEC_GetClientInfo() {
    char *clientInfo = NULL;
    ContextCli *currContext = GetCliGlobals()->currContext();
    if (currContext != NULL) clientInfo = currContext->getClientInfo();

    return clientInfo;
  }

  int SQL_EXEC_GetTransactionId(/*OUT*/ long *trans_id) {
    int retcode;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      retcode = SQLCLI_GetTransactionId(GetCliGlobals(), trans_id);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        throw;
      }
#endif
    }
    retcode = RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_GetStatementHeapSize(
      /*IN*/ SQLSTMT_ID *statement_id,
      /*OUT*/ long *heapSize) {
    int retcode;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      retcode = SQLCLI_GetStatementHeapSize(GetCliGlobals(), statement_id, heapSize);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        throw;
      }
#endif
    }
    retcode = RecordError(NULL, retcode);
    return retcode;
  }

  int SQL_EXEC_GetAuthGracePwdCnt(
      /*IN*/ int userid,
      /*OUT*/ Int16 &graceCnt) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_GetAuthGracePwdCnt(GetCliGlobals(), userid, graceCnt);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

  int SQL_EXEC_UpdateAuthGracePwdCnt(
      /*IN*/ int userid,
      /*IN*/ Int16 graceCnt) {
    int retcode;
    CLISemaphore *tmpSemaphore = NULL;
    ContextCli *threadContext;

    CLI_NONPRIV_PROLOGUE(retcode);

    try {
      tmpSemaphore = getCliSemaphore(threadContext);
      tmpSemaphore->get();
      threadContext->incrNumOfCliCalls();
      retcode = SQLCLI_UpdateAuthGracePwdCnt(GetCliGlobals(), userid, graceCnt);
    } catch (...) {
      retcode = -CLI_INTERNAL_ERROR;
#if defined(_THROW_EXCEPTIONS)
      if (cliWillThrow()) {
        threadContext->decrNumOfCliCalls();
        tmpSemaphore->release();
        throw;
      }
#endif
    }

    threadContext->decrNumOfCliCalls();
    tmpSemaphore->release();
    return retcode;
  }

#ifdef __cplusplus
  }
#endif
