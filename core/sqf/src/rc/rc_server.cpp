//------------------------------------------------------------------
//
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

#include <assert.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "seabed/fserr.h"
#include "seabed/ms.h"
#include "seabed/pctl.h"
#include "seabed/pevents.h"
#include "seabed/thread.h"
#include "seabed/trace.h"

#include "ml.h"
#include "rc_chkfe.h"
#include "rc_ms.h"
#include "rc_msfsutil.h"
#include "rc_util.h"
#include "rc_utilp.h"

// Version
#include "SCMVersHelp.h"
DEFINE_EXTERN_COMP_DOVERS(rc_server)

int   gv_my_nid     = -1;
int   gv_my_zid     = -1;
int   gv_my_pid     = -1;
long  gv_my_tid     = -1;
int   gv_my_mon_pid = -1;
char  gv_my_name[MS_MON_MAX_PROCESS_NAME];
bool  debug        = false;
int   dsize        = MAX_BUF;
bool  event        = false;
int   inxc         = 1;
int   inxs         = 1;
int   maxs         = 1;
bool  quiet        = false;
int   sre_count    = 0;
int   gv_verbose   = VERBOSE_LEVEL_NONE;

char  gv_my_node_name[MS_MON_MAX_PROCESSOR_NAME];

SB_Ts_Queue            free_q((char *) "freeQ");
SB_Thread::Mutex       mutex;
char                   my_name[BUFSIZ];
TPT_DECL2             (phandle,MAX_THR_S);
SB_Sig_Queue           work_q((char *) "workQ", false);

typedef struct  {
    SB_ML_Type     link;
    bool           fin;
    BMS_SRE        sre;
} Test_SRE;
enum { MAX_SRES = MAX_THR_S * 3 };
Test_SRE sres[MAX_SRES];

void get_my_info(const char *pv_where)
{
  char lv_time[100];
  msg_mon_get_my_info(&gv_my_nid,       // mon node-id
                      &gv_my_mon_pid,   // mon process-id
                      gv_my_name,       // mon name
                      MS_MON_MAX_PROCESS_NAME,       // mon name-len
                      NULL,             // mon process-type
                      &gv_my_zid,       // mon zone-id
                      &gv_my_pid,       // os process-id
                      &gv_my_tid        // os thread-id
                      );

  if (gv_verbose >= VERBOSE_LEVEL_LOW)
    printf("%s, %s, nid: %d, pid: %d, name: %s\n",
           util_printf_time(lv_time),
           pv_where,
           gv_my_nid,
           gv_my_pid,
           gv_my_name
           );
  MS_Mon_Zone_Info_Type lv_zinfo;
  int ferr = msg_mon_get_zone_info_detail(gv_my_nid, gv_my_zid, &lv_zinfo);
  if (ferr == 0) {
    if (gv_verbose >= VERBOSE_LEVEL_LOW)
      printf("%s, %s, nid: %d, zid: %d, #entries: %d, node_name: %s\n", 
             util_printf_time(lv_time),
             pv_where,
             gv_my_nid, 
             gv_my_zid,
             lv_zinfo.num_returned,
             lv_zinfo.node[0].node_name
             );
    memset(gv_my_node_name, 0, MS_MON_MAX_PROCESSOR_NAME);
    strncpy(gv_my_node_name, lv_zinfo.node[0].node_name, MS_MON_MAX_PROCESSOR_NAME-1);
  }
  else {
    util_gethostname(gv_my_node_name, MS_MON_MAX_PROCESSOR_NAME);
  }

}

// Execute a 'system' command and get the output 
long rc_exec_pipe_cmd(const char* pp_cmd, char *pp_result, unsigned long pv_max_output_len) {
  char *lp_curr = 0;
  unsigned long lv_curr_read_len = 0;
  unsigned long lv_read_len = 0;
  int lv_done = 0;

  FILE* lp_pipe = popen(pp_cmd, "r");
  if (!lp_pipe) return -1;

  lp_curr = &pp_result[0];
  
  while ((lv_done == 0) && (!feof(lp_pipe)) ) {
    if (fgets(lp_curr, 128, lp_pipe) != NULL) {
      lv_curr_read_len = strlen(lp_curr);
      lv_read_len += lv_curr_read_len;
      lp_curr += lv_curr_read_len;
      if (lv_read_len + 128 > pv_max_output_len) {
        lv_done = 1;
      }
    }
  }

  pclose(lp_pipe);

  return lv_read_len;
}

void server(int whoami, Test_SRE *sre) {

    char lv_time[100];

    int             ferr;
    Util_AA<char>   recv_buffer(MAX_BUF);
    Util_AA<short>  recv_buffer2(MAX_BUF);

    ferr = BMSG_READCTRL_(sre->sre.sre_msgId,     // msgid
                          &recv_buffer2,          // reqctrl
                          1);                     // bytecount
    util_check("BMSG_READCTRL_", ferr);
    ferr = BMSG_READDATA_(sre->sre.sre_msgId,     // msgid
                          &recv_buffer,           // reqdata
                          MAX_BUF);               // bytecount
    util_check("BMSG_READDATA_", ferr);
    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM)
      printf("%s, s-%d, cmd received: %s\n", 
             util_printf_time(lv_time),
             whoami, 
             &recv_buffer);

    char lv_sed_string[512];
    lv_sed_string[0] = 0;
    sprintf(lv_sed_string, " 2>&1 | sed -e \"s/^/%s: /\"", gv_my_node_name);
   
   char lv_cmd[MAX_COMMAND_REQUEST_LEN + sizeof(lv_sed_string) + 128];
   lv_cmd[0] = 0;
   sprintf(lv_cmd, "function func_run_command { %s ; } ; func_run_command %s",
           &recv_buffer,
           lv_sed_string);

   (&recv_buffer)[0] = 0;
   rc_exec_pipe_cmd(lv_cmd, 
                    &recv_buffer,
                    MAX_BUF);

   if (gv_verbose >= VERBOSE_LEVEL_HIGH)
     printf("%s, s-%d, reply: %s\n", 
            util_printf_time(lv_time),
            whoami, 
            &recv_buffer);

   BMSG_REPLY_(sre->sre.sre_msgId,          // msgid
               NULL,                        // replyctrl
               0,                           // replyctrlsize
               &recv_buffer,                // replydata
               (int) strlen(&recv_buffer),  // replydatasize
               0,                           // errorclass
               NULL);                       // newphandle
}

void *server_thr(void *arg) {

    char lv_time[100];

    Test_SRE *srep;
    int       whoami = inxs++;

    arg = arg; // touch
    for (;;) {
        srep = (Test_SRE *) work_q.remove();
        if (gv_verbose >= VERBOSE_LEVEL_HIGH)
            printf("%s, s-%d, have work, fin=%d\n", 
                   util_printf_time(lv_time),
                   whoami, 
                   srep->fin);
        if (srep->fin)
            break;
        server(whoami, srep);
        free_q.add(&srep->link);
    }
    return NULL;
}

SB_Thread::Thread *thrs[MAX_THR_S];

int main(int argc, char *argv[]) {
  int   ferr;
  int   inx;
  int   lerr;
  void *res;
  int   status;
  TAD   zargs[] = {
    { "-debug",       TA_Bool, TA_NOMAX,    &debug        },
    { "-dsize",       TA_Int,  MAX_BUF,     &dsize        },
    { "-maxs",        TA_Int,  MAX_THR_S,   &maxs         },
    { "-quiet",       TA_Bool, TA_NOMAX,    &quiet        },
    { "-v",           TA_Int,  TA_NOMAX,    &gv_verbose      },
    { "",             TA_End,  TA_NOMAX,    NULL          }
  };

  char lv_time[100];

  CALL_COMP_DOVERS(rc_server, argc, argv);

  msfs_util_init(&argc, &argv, msg_debug_hook);
  arg_proc_args(zargs, false, argc, argv);
  ms_getenv_int((const char *)"RC_SERVER_VERBOSE_LEVEL", &gv_verbose);
  printf("%s, s-main, started rc_server (verbosity level: %d) \n", 
         util_printf_time(lv_time),
         gv_verbose);

  // setup threads
  for (inx = 0; inx < maxs; inx++) {
    char lname[10];
    sprintf(lname, "s%d", inx);
    thrs[inx] = new SB_Thread::Thread(server_thr, lname);
  }

  util_test_start(false);
  ferr = msg_mon_process_startup(true); // system messages
  TEST_CHK_FEOK(ferr);

  get_my_info("s-main");
  util_gethostname(my_name, sizeof(my_name));

  for (inx = 0; inx < MAX_SRES; inx++)
    free_q.add(&sres[inx].link);
  for (inx = 0; inx < maxs; inx++)
    thrs[inx]->start();
  Test_SRE *sre = NULL;
  inx = 0;
  bool lv_done = false;
  while ( ! lv_done ) {
    lerr = XWAIT(LREQ, 1000);
    TEST_CHK_WAITIGNORE(lerr);
    if (gv_verbose >= VERBOSE_LEVEL_HIGH)
      printf("%s, s-main, after wait - received some request. \n", 
             util_printf_time(lv_time)
             );
    do {
      if (sre == NULL) {
        sre = (Test_SRE *) free_q.remove();
        assert(sre != NULL);
      }
      sre->fin = false;
      lerr = BMSG_LISTEN_((short *) &sre->sre, // sre
                          0,                   // listenopts
                          0);                  // listenertag
      if (lerr == BSRETYPE_IREQ) {
        assert(sre->sre.sre_msgId > 0);
        if (gv_verbose >= VERBOSE_LEVEL_HIGH)
          printf("%s, s-main, queue work inx=%d\n", 
                 util_printf_time(lv_time),
                 inx);
        work_q.add(&sre->link);
        sre = NULL;
        inx++;
      }
    } while (lerr == BSRETYPE_IREQ);

    if (gv_verbose >= VERBOSE_LEVEL_HIGH)
      printf("%s, s-main, out of the inner work loop \n",
             util_printf_time(lv_time)
             );
  }

  for (inx = 0; inx < maxs; inx++) {
    Test_SRE *quit_sre = (Test_SRE *) free_q.remove();
    quit_sre->fin = true;
    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM)
      printf("%s, s-main, fin inx=%d\n", 
             util_printf_time(lv_time),
             inx);
    work_q.add(&quit_sre->link);
    util_time_sleep_ms(1);
  }

  for (inx = 0; inx < maxs; inx++) {
    status = thrs[inx]->join(&res);
    if (status != 0) {
      if (gv_verbose >= VERBOSE_LEVEL_MEDIUM)
        printf("%s, s-main, error performing a join with server thread: %d\n", 
               util_printf_time(lv_time),
               inx);
    }
    else {
      if (gv_verbose >= VERBOSE_LEVEL_MEDIUM) 
        printf("%s, s-main, joined with server %d\n", 
               util_printf_time(lv_time),
               inx);
    }
  }

  ferr = msg_mon_process_shutdown();
  TEST_CHK_FEOK(ferr);
  util_test_finish(false);
  return 0;
}
