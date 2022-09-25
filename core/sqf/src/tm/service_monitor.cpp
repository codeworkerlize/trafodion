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
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/shm.h>
#include <sys/time.h>

#include "SCMVersHelp.h"

#include "seabed/fserr.h"
#include "seabed/ms.h"
#include "seabed/pctl.h"
#include "seabed/pevents.h"

char           ga_name[BUFSIZ];
bool           gv_shook   = false;
bool           gv_verbose = false;
long           gv_sleeptime = 3000; // in 10 ms units. So, this is 30 seconds.
char           gv_file_name_base[512];
char           gv_attached_process_name[512];


DEFINE_EXTERN_COMP_DOVERS(service_monitor)

// forwards
void do_reply(BMS_SRE *pp_sre, char *pp_reply, int pv_len, short pv_ec);

//
// initialize
//
void do_init(int pv_argc, char **ppp_argv) {
    char *lp_arg;
    int   lv_arg;
    bool  lv_attach;
    int   lv_ferr;

    lv_attach = false;
    memset(gv_file_name_base, 0, sizeof(gv_file_name_base));
    strcpy(gv_file_name_base, "service_monitor.cmd");
    memset(gv_attached_process_name, 0, sizeof(gv_attached_process_name));
    strcpy(gv_attached_process_name, "SRVMON0");

    for (lv_arg = 1; lv_arg < pv_argc; lv_arg++) {
        lp_arg = ppp_argv[lv_arg];
        if (strcmp(lp_arg, "-attach") == 0)
            lv_attach = true;
        else if (strcmp(lp_arg, "-shook") == 0)
            gv_shook = true;
        else if (strcmp(lp_arg, "-v") == 0)
            gv_verbose = true;
        else if (strcmp(lp_arg, "-t") == 0) {
	  if (lv_arg < (pv_argc - 1)) {
	    lp_arg = ppp_argv[++lv_arg];
	    errno = 0;
	    long lv_sleeptime = 0;
	    lv_sleeptime = strtol(lp_arg, (char **) NULL, 10);
	    if ((errno == 0) && 
		(lv_sleeptime > 0) && 
		(lv_sleeptime < 86401)) {
	      gv_sleeptime = lv_sleeptime * 100;
	    }
	  }
	}
	else if (strcmp(lp_arg, "-f") == 0) {
	  if (lv_arg < (pv_argc - 1)) {
	    lp_arg = ppp_argv[++lv_arg];
	    strcpy(gv_file_name_base, lp_arg);
	  }
	}
	else if (strcmp(lp_arg, "-n") == 0) {
	  if (lv_arg < (pv_argc - 1)) {
	    lp_arg = ppp_argv[++lv_arg];
	    strcpy(gv_attached_process_name, lp_arg);
	  }
	}
    }

    if (lv_attach) {
      char lv_process_name[512];
      sprintf(lv_process_name, "$%s", gv_attached_process_name);
      lv_ferr = msg_init_attach(&pv_argc, &ppp_argv, false, (char *) lv_process_name);
    }
    else
      lv_ferr = msg_init(&pv_argc, &ppp_argv);

    assert(lv_ferr == XZFIL_ERR_OK);

    if (gv_shook)
        msg_debug_hook("s", "s");
}

//
// process monitor message.
//
// if shutdown, set done
//
void do_mon_msg(BMS_SRE *pp_sre, bool *pp_done) {
    int        lv_ferr;
    MS_Mon_Msg lv_mon_msg;

    lv_ferr = BMSG_READDATA_(pp_sre->sre_msgId,         // msgid
                             (char *) &lv_mon_msg,      // reqdata
                             (int) sizeof(lv_mon_msg)); // bytecount
    assert(lv_ferr == XZFIL_ERR_OK);
    if (lv_mon_msg.type == MS_MsgType_Shutdown)
        *pp_done = true;
    if (gv_verbose)
        printf("srv: received mon message\n");
    do_reply(pp_sre, NULL, 0, 0);
}

//
// do reply
//
void do_reply(BMS_SRE *pp_sre, char *pp_reply, int pv_len, short pv_ec) {
    if (gv_verbose)
        printf("srv: reply, len=%d, ec=%d\n", pv_len, pv_ec);
    BMSG_REPLY_(pp_sre->sre_msgId,   // msgid
                NULL,                // replyctrl
                0,                   // replyctrlsize
                pp_reply,            // replydata
                pv_len,              // replydatasize
                pv_ec,               // errorclass
                NULL);               // newphandle
}

void do_cmd(const char *pv_cmd_file)
{
  FILE *fp = 0;

  errno = 0;
  fp = fopen(pv_cmd_file, "r");
  if (fp == NULL) {
    return;
  }
  else {
    fclose(fp);
    system(pv_cmd_file);
  }

}

//
// server main
//
int main(int pv_argc, char *pa_argv[]) {
    bool     lv_done;
    int      lv_ferr;
    int      lv_lerr;
    BMS_SRE  lv_sre;

    CALL_COMP_DOVERS(service_monitor, pv_argc, pa_argv);

    do_init(pv_argc, pa_argv);

    lv_ferr = msg_mon_process_startup(true); // system messages
    assert(lv_ferr == XZFIL_ERR_OK);
    msg_mon_enable_mon_messages(true);
    lv_ferr = msg_mon_get_my_process_name(ga_name, sizeof(ga_name));
    assert(lv_ferr == XZFIL_ERR_OK);
    char cmd_file[512];
    memset(cmd_file, 0, 512);
    sprintf(cmd_file, "%s/sql/scripts/%s", 
	    getenv("TRAF_HOME"),
	    gv_file_name_base);

    lv_done = false;
    while (!lv_done) {
        do {
            lv_lerr = XWAIT(LREQ, gv_sleeptime);
            lv_lerr = BMSG_LISTEN_((short *) &lv_sre, // sre
                                   0,                 // listenopts
                                   0);                // listenertag
	    do_cmd(cmd_file);
        } while (lv_lerr == XSRETYPE_NOWORK);
        if (lv_sre.sre_flags & XSRE_MON) {
            do_mon_msg(&lv_sre, &lv_done);
        }
    }

    if (gv_verbose)
        printf("server %s shutting down\n", ga_name);
    lv_ferr = msg_mon_process_shutdown();
    assert(lv_ferr == XZFIL_ERR_OK);

    return 0;
}
