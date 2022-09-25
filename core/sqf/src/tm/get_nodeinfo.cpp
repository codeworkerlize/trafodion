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
#include "trafconf/trafconfig.h"

bool    gv_shook      = false;
bool    gv_verbose    = false;
bool    gv_all_nodes  = true;
char    gv_node_name[512];
DEFINE_EXTERN_COMP_DOVERS(get_nodeinfo)

//
// initialize
//
void do_init(int pv_argc, char **ppp_argv) {
    char *lp_arg;
    int   lv_arg;
    bool  lv_attach;
    int   lv_ferr;

    lv_attach = false;

    for (lv_arg = 1; lv_arg < pv_argc; lv_arg++) {
        lp_arg = ppp_argv[lv_arg];
        if (strcmp(lp_arg, "-attach") == 0)
            lv_attach = true;
        else if (strcmp(lp_arg, "-shook") == 0)
            gv_shook = true;
        else if (strcmp(lp_arg, "-n") == 0) {
	  if (lv_arg < (pv_argc - 1)) {
	    lp_arg = ppp_argv[++lv_arg];
	    gv_node_name[0] = 0;
	    strncpy(gv_node_name, lp_arg, sizeof(gv_node_name) -1);
	    gv_all_nodes = false;
	  }
	}
        else if (strcmp(lp_arg, "-v") == 0)
            gv_verbose = true;
    }

    if (lv_attach) {
      lv_ferr = msg_init_attach(&pv_argc, &ppp_argv, false, (char *) "");
    }
    else {
      lv_ferr = msg_init(&pv_argc, &ppp_argv);
    }

    assert(lv_ferr == XZFIL_ERR_OK);

    if (gv_shook)
        msg_debug_hook("s", "s");
}

//
// server main
//
int main(int pv_argc, char *pa_argv[]) {
    int      lv_ferr;

    CALL_COMP_DOVERS(get_nodeinfo, pv_argc, pa_argv);
    
    try {
    do_init(pv_argc, pa_argv);

    lv_ferr = msg_mon_process_startup(true); // system messages
    }
    catch (...) {
      fprintf(stderr, "Error while initializing the messaging system. Please ensure that the Trafodion env is up. Exiting...\n");
      fflush(stderr);
      exit(1);
    }
      
    assert(lv_ferr == XZFIL_ERR_OK);
    msg_mon_enable_mon_messages(true);

    int lv_num_allocated = TC_NODES_MAX;
    int lv_num_nodes = 0;
    MS_Mon_Zone_Info_Entry_Type *p_zi = 0;
    p_zi = (MS_Mon_Zone_Info_Entry_Type *) calloc(lv_num_allocated, 
						  sizeof(MS_Mon_Zone_Info_Entry_Type));
    if (p_zi == 0) {
      exit(1);
    }

    int lv_ret = msg_mon_get_zone_info(&lv_num_nodes,
				       lv_num_allocated,
				       p_zi);
    if (lv_ret != 0) {
      fprintf(stdout, "%s: Error %d returned... exitting\n", pa_argv[0], lv_ret);
      fflush(stdout);
      exit(1);
    }
    
    if (gv_verbose) {
      fprintf(stdout, "num nodes: %d \n", lv_num_nodes);
      fflush(stdout);
    }
      
    for (int i = 0; i < lv_num_nodes; i++) {
      if (gv_verbose) {
	fprintf(stdout, "%d %s\n", p_zi[i].zid, p_zi[i].node_name);
	fflush(stdout);
      }
      else {
	if (!gv_all_nodes) {
	  if (strcmp(p_zi[i].node_name, gv_node_name) != 0) {
	    continue;
	  }
	  else {
	    if (p_zi[i].pstate != MS_Mon_State_Up) {
	      exit(-1);
	    }
	  }
	}
	fprintf(stdout, "%d ", p_zi[i].zid);
	fflush(stdout);
      }
    }
    if ((lv_num_nodes > 0) && (!gv_verbose)) {
      printf("\n");
      fflush(stdout);
    }

    lv_ferr = msg_mon_process_shutdown();
    assert(lv_ferr == XZFIL_ERR_OK);

    return 0;
}
