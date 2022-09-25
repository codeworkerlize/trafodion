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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>


#include "CommonLogger.h"

// General Seaquest includes
#include "SCMVersHelp.h"

// seabed includes
#include "seabed/logalt.h"
#include "seabed/ms.h"
#include "seabed/pctl.h"
#include "seabed/pevents.h"
#include "seabed/trace.h"
#include "seabed/thread.h"
#include "common/sq_common.h"
#include "common/evl_sqlog_eventnum.h"

static int sv_my_nid;
static int sv_my_pid;

extern void ms_getenv_int (const char *pp_key, int *pp_val);
extern void ms_getenv_bool(const char *pp_key, bool *pp_val);

// Version
DEFINE_EXTERN_COMP_DOVERS(traf_notify)

static std::string TRAF_NOTIFY = "TRAF_NOTIFY";

#define TN_LOG_MSG_SIZE 4096

int tn_send_mail(short pv_mail_severity_level,
		 char *pv_severity_string,
		 char *p_msg)
{
  char fname[512];
  static bool sb_first = true;
  static bool sb_mail_enable=false;
  static int  sv_mail_severity_level=3;
  static char sv_mail_subject_prefix[1024];
  static char sv_mail_to[1024];

  if (sb_first) {
    sb_first = false;

    ms_getenv_bool("NOTIFY_MAIL_ENABLE", &sb_mail_enable);
    ms_getenv_int ("NOTIFY_MAIL_SEVERITY_LEVEL", &sv_mail_severity_level);
  
    memset(sv_mail_subject_prefix, 0, sizeof(sv_mail_subject_prefix));
    char *lv_mail_subject_prefix = getenv("NOTIFY_MAIL_SUBJECT_PREFIX");
    if (lv_mail_subject_prefix) {
      strcpy(sv_mail_subject_prefix, lv_mail_subject_prefix);
    }
    else {
      strcpy(sv_mail_subject_prefix, "Esgyn Enterprise 1.0 Notification");
    }

    memset(sv_mail_to, 0, sizeof(sv_mail_to));
    char *lv_mail_to = getenv("NOTIFY_MAIL_TO");
    if (lv_mail_to) {
      strcpy(sv_mail_to, lv_mail_to);
    }
    else {
      strcpy(sv_mail_to, "narendra.goyal@esgyn.com");
    }

  }

  if (! sb_mail_enable) {
    return 5;
  }

  if (pv_mail_severity_level > sv_mail_severity_level) {
    return 4;
  }

  memset(fname, 0, 512);
  sprintf(fname, "%s/sendmail.txt", getenv("TRAF_LOG"));

  FILE *fp = fopen(fname, "w");
  if (fp == NULL) {
    printf("Error %d while opening the file:%s", errno, fname);
    return errno;
  }

  fprintf(fp, "%s", p_msg);
  fclose(fp);
    
  char lv_mail_cmd[1024];
  memset(lv_mail_cmd, 0, 1024);
  sprintf(lv_mail_cmd, "mail -s \"%s:%s\" %s < %s", 
	  sv_mail_subject_prefix,
	  pv_severity_string,
	  sv_mail_to,
	  fname);
    
  int lv_system_ret_code = system(lv_mail_cmd);
  printf("Exit code: %d:Mail command: %s\n",
	 lv_system_ret_code,
	 lv_mail_cmd);
  fflush(stdout);

  return lv_system_ret_code;
}   

int tn_log_stdout(int event_id, 
		  posix_sqlog_severity_t severity, 
		  char *temp_string,
		  int pv_event_node_id,
		  char *str1
		  )

{
  time_t    current_time;
  char      timestamp[50];

  char      my_name[MS_MON_MAX_PROCESS_NAME];
  int       my_nid,my_pid;
  int       error;
  char      lv_msg[TN_LOG_MSG_SIZE];

  current_time = time(NULL);
  ctime_r(&current_time,timestamp);
  timestamp[strlen(timestamp)-1] = '\0';
    
  memset(lv_msg, 0, TN_LOG_MSG_SIZE);
  sprintf(lv_msg, "%s:", timestamp);

  char lv_severity_string[32];
  memset(lv_severity_string, 0, 32);
  switch (severity)
    {
    case SQ_LOG_EMERG: 
      strcpy(lv_severity_string, "EMERGENCY"); 
      break;
    case SQ_LOG_ALERT: 
      strcpy(lv_severity_string, "ALERT"); 
      break;
    case SQ_LOG_CRIT: 
      strcpy(lv_severity_string, "CRITICAL"); 
      break;
    case SQ_LOG_ERR: 
      strcpy(lv_severity_string, "ERROR"); 
      break;
    case SQ_LOG_WARNING: 
      strcpy(lv_severity_string, "WARNING"); 
      break;
    case SQ_LOG_NOTICE: 
      strcpy(lv_severity_string, "NOTICE"); 
      break;
    case SQ_LOG_INFO: 
      strcpy(lv_severity_string, "INFO"); 
      break;
    case SQ_LOG_DEBUG: 
      strcpy(lv_severity_string, "DEBUG"); 
      break;
    default:
      strcpy(lv_severity_string, "UNKNOWN");
    }
  strcat(lv_msg, lv_severity_string);

  sprintf(lv_msg, "%s:Event %s", lv_msg, temp_string);
  if (pv_event_node_id != 0)
    sprintf(lv_msg, "%s:NodeId %u",lv_msg, pv_event_node_id);

  my_nid = my_pid = -1;
  strcpy(my_name, "UNKNOWN");
  error = msg_mon_get_my_process_name( my_name, sizeof(my_name) );
  if (!error)
    {
      error = msg_mon_get_process_info( my_name, &my_nid, &my_pid );
    }
  sprintf(lv_msg, "%s:Reported by (%s,%u,%u) ",lv_msg, my_name,my_nid,my_pid);
  strcat(lv_msg, "\n");

  printf("%s", lv_msg);

  error = tn_send_mail(severity, lv_severity_string, lv_msg);

  return error;
} 

int tn_log_event(int event_id, 
                 posix_sqlog_severity_t severity, 
                 const char *temp_string,
                 int pv_event_node_id,
		 char *str1 = NULL)

{
    int rc = 0;
    char la_buf[1024];
    strncpy (la_buf, temp_string, 1024 - 1);
    tn_log_stdout(event_id,
		  severity, 
		  la_buf, 
		  pv_event_node_id, 
		  str1);

   return rc;
}

// ---------------------------------------------------------------------------
// tn_process_node_down_msg
// ---------------------------------------------------------------------------
void tn_process_node_down_msg(int pv_nid)
{
  trace_printf("tn_process_node_down_msg ENTRY, nid %d\n", pv_nid);
  tn_log_event(DTM_NODEDOWN,
	       SQ_LOG_CRIT, 
	       "NODEDOWN", 
	       pv_nid);


  trace_printf("tn_process_node_down_msg EXIT nid %d\n", pv_nid);

} //tn_process_node_down_msg

void tn_process_monitor_msg(BMS_SRE *pp_sre, char *pp_buf, bool *pp_done)
{
  MS_Mon_Msg  lv_msg;

  if (pp_buf == NULL)
    {
      tn_log_event(DTM_INVALID_PROC_MON_MSG, 
		   SQ_LOG_CRIT,
		   "DTM_INVALID_PROC_MON_MSG",
		   -1, 
		   NULL);
      trace_printf("tn_process_monitor_msg ENTER, data null, exiting \n");

      return;
    }
    
  memcpy (&lv_msg, pp_buf, sizeof (MS_Mon_Msg));

  trace_printf("tn_process_monitor_msg ENTRY, type=%d\n", lv_msg.type);

  XMSG_REPLY_(pp_sre->sre_msgId,       /*msgid*/
	      NULL,           /*replyctrl*/
	      0,      /*replyctrlsize*/
	      NULL,           /*replydata*/
	      0,          /*replydatasize*/
	      0,              /*errorclass*/
	      NULL);          /*newphandle*/
    
  switch (lv_msg.type) 
    {
    case MS_MsgType_Shutdown:
      {
	trace_printf("tn_process_monitor_msg Shutdown notice, level %d.\n", lv_msg.u.shutdown.level);
	*pp_done = true;
	tn_log_event(DTM_SHUTDOWN_NOTICE, 
		     SQ_LOG_INFO, 
		     "TRAFODION_SHUTDOWN",
		     lv_msg.u.shutdown.level);
      
	if (lv_msg.u.shutdown.level == MS_Mon_ShutdownLevel_Immediate) {
	  ;
	}
	msg_mon_process_shutdown();

	break;
      }  // MS_MsgType_Shutdown

    case MS_MsgType_NodeDown:
      {
	trace_printf("tn_process_monitor_msg NodeDown notice for nid %d\n", lv_msg.u.down.nid);

	tn_process_node_down_msg(lv_msg.u.death.nid);

	break;
      }
    case MS_MsgType_NodeUp:
      {
	trace_printf("tn_process_monitor_msg NodeUp notice for nid %d\n", lv_msg.u.up.nid);
	tn_log_event(DTM_NODEUP,
		     SQ_LOG_INFO,
		     "NODEUP", 
		     lv_msg.u.up.nid);
	break;
      }
#if 0
    case MS_MsgType_NodePrepare:
      {
	trace_printf("tn_process_monitor_msg NodePrepare notice for nid %d\n", lv_msg.u.prepare.nid);
	tn_log_event(DTM_NODEPREPARE, 
		     SQ_LOG_INFO,
		     "NODEPREPARE", 
		     lv_msg.u.prepare.nid);
	break;
      }

    case MS_MsgType_TmRestarted:
      {
	trace_printf("tn_process_monitor_msg TMRestarted notice for nid %d\n", lv_msg.u.tmrestarted.nid);

        tn_log_event(DTM_TMRESTARTED, 
		     SQ_LOG_INFO,
		     "TMRESTARTED", 
		     lv_msg.u.tmrestarted.nid);

        break;
      }
#endif
    case MS_MsgType_ProcessDeath:
      {
	trace_printf("tn_process_monitor_msg Process Death notice for %s\n", lv_msg.u.death.process_name);

	if (lv_msg.u.death.type == MS_ProcessType_DTM) {
	  tn_log_event(DTM_PROCDEATH_DTM, 
		       SQ_LOG_INFO, 
		       "DTM_PROCDEATH_DTM", 
		       -1,
		       lv_msg.u.death.process_name);
	  trace_printf("tn_process_monitor_msg death notice for DTM%d\n", lv_msg.u.death.nid);
	  break;
	}
	break;
      }
    case MS_MsgType_Event:
//    case MS_MsgType_UnsolicitedMessage:
    default:
      {
	break;
      }
    };
   
  trace_printf("tn_process_monitor_msg EXIT\n");
}

// -----------------------------------------------------------------------
// tn_process_msg
// Purpose - process messages incoming to the TM
// -----------------------------------------------------------------------
void tn_process_msg(BMS_SRE *pp_sre, bool *pp_done) 
{
    short                  lv_ret;
    char                   la_recv_buffer[8192];

    trace_printf("tn_process_msg ENTRY\n");

    lv_ret = BMSG_READDATA_(pp_sre->sre_msgId,           // msgid
                            la_recv_buffer,              // reqdata
                            pp_sre->sre_reqDataSize);    // bytecount

    if (lv_ret != 0)
    {
      return;
    }
    
    if (pp_sre->sre_flags & XSRE_MON) 
    {
      tn_process_monitor_msg(pp_sre, la_recv_buffer, pp_done);
        return;
    }

    trace_printf("tn_process_msg EXIT\n");
}

// ----------------------------------------------------------------
// main method
// ----------------------------------------------------------------
int main(int argc, char *argv[]) 
{
    BMS_SRE   lv_sre;
    bool      lv_done = false;
    int       lv_lerr;

    CALL_COMP_DOVERS(traf_notify, argc, argv);
    
    // get our pid info and initialize
    msg_init(&argc, &argv);

    // get our pid info and initialize
    msg_mon_get_my_info2(&sv_my_nid, // mon node-id
                         &sv_my_pid, // mon process-id
                         NULL,       // mon name
                         0,          // mon name-len
                         NULL,       // mon process-type
                         NULL,       // mon zone-id
                         NULL,       // os process-id
                         NULL,       // os thread-id
                         NULL);      // component-id

    msg_mon_process_startup(true); // server?
    msg_debug_hook ("tn.hook", "tn.hook");
    msg_mon_enable_mon_messages (1);

    while (!lv_done) {
        do {
            lv_lerr = XWAIT(LREQ, -1);
            lv_lerr = BMSG_LISTEN_((short *) &lv_sre, // sre
                                   0,                 // listenopts
                                   0);                // listenertag
        } while (lv_lerr == XSRETYPE_NOWORK);

	tn_process_msg(&lv_sre, &lv_done);
    }

    msg_mon_process_shutdown();
}
