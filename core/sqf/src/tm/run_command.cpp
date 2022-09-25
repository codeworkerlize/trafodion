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
char           gv_cmd[16000];

DEFINE_EXTERN_COMP_DOVERS(run_command)

//
// initialize
//
void do_init(int pv_argc, char **ppp_argv) {
    char *lp_arg;
    int   lv_arg;
    bool  lv_attach;
    int   lv_ferr;
    bool  lv_cmd_reading_mode = false;

    lv_attach = false;
    memset(gv_file_name_base, 0, sizeof(gv_file_name_base));
    strcpy(gv_file_name_base, "service_monitor.cmd");
    memset(gv_attached_process_name, 0, sizeof(gv_attached_process_name));
    strcpy(gv_attached_process_name, "RUNCMD0");

    for (lv_arg = 1; lv_arg < pv_argc; lv_arg++) {
      if (lv_cmd_reading_mode) {
	strcat(gv_cmd, ppp_argv[lv_arg]);
	strcat(gv_cmd, " ");
	continue;
      }

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
	else if (strcmp(lp_arg, "-c") == 0) {
	  lv_cmd_reading_mode = true;
	  gv_cmd[0] = 0;
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

    if (lv_ferr != XZFIL_ERR_OK) {
      exit(1);
    }

    if (gv_shook)
        msg_debug_hook("s", "s");
}

//
// server main
//
int main(int pv_argc, char *pa_argv[]) {
    int      lv_ferr;
    char     lv_node_name[MS_MON_MAX_PROCESSOR_NAME];

    CALL_COMP_DOVERS(run_command, pv_argc, pa_argv);

    do_init(pv_argc, pa_argv);

    lv_ferr = msg_mon_process_startup(true); // system messages
    if (lv_ferr != XZFIL_ERR_OK) {
      exit(1);
    }
    msg_mon_enable_mon_messages(true);

    char lv_sed_string[512];
    if (strlen(gv_cmd) > 1) {
      int lv_my_nid, lv_my_zid,lv_my_pid=0;
      msg_mon_get_my_info(&lv_my_nid, // mon node-id
			  &lv_my_pid, // mon process-id
			  NULL,       // mon name
			  0,       // mon name-len
			  NULL,       // mon process-type
			  &lv_my_zid,       // mon zone-id
			  NULL,       // os process-id
			  NULL       // os thread-id
			  );
      MS_Mon_Zone_Info_Type lv_zinfo;
      int ferr = msg_mon_get_zone_info_detail(lv_my_nid, lv_my_zid, &lv_zinfo);
      if (ferr == 0) {
	/*	
		printf("nid: %d, zid: %d, #entries: %d, max: %d, struct size: %ld \n", lv_my_nid, lv_my_zid,
		lv_zinfo.num_returned,
		MS_MON_MAX_NODE_LIST,
		sizeof(lv_zinfo));
		printf("node name: %s \n", lv_zinfo.node[0].node_name);
	*/
	memset(lv_node_name, 0, MS_MON_MAX_PROCESSOR_NAME);
	strncpy(lv_node_name, lv_zinfo.node[0].node_name, MS_MON_MAX_PROCESSOR_NAME-1);
      }
      else {
	gethostname(lv_node_name, MS_MON_MAX_PROCESSOR_NAME);
      }
      
      lv_sed_string[0] = 0;
      sprintf(lv_sed_string, " 2>&1 | sed -e \"s/^/%s: /\"", lv_node_name);

      char lv_cmd[16384];
      lv_cmd[0] = 0;
      sprintf(lv_cmd, "function run_command_%d { %s ; } ; run_command_%d %s",
	      getpid(), 
	      gv_cmd,
	      getpid(),
	      lv_sed_string);
      //printf("cmd: %s \n", lv_cmd);

      system(lv_cmd);
      fflush(stdout);
    }

    msg_mon_process_shutdown();

    return 0;
}
