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
DEFINE_EXTERN_COMP_DOVERS(rc_client)

#include "trafconf/trafconfig.h"

int   dsize        = MAX_BUF;
int   inxc         = 1;
int   gv_default_maxc = 128;                   // number of client threads
int   gv_maxc         = -1;             
int   gv_maxsp        = 1;
char *name         = NULL;
char  *gv_cmd_array[1];
char *gv_cmd       = NULL;
int   gv_verbose      = VERBOSE_LEVEL_NONE;
bool  gv_all_nodes = false;
char *gv_node_name_list[2000];
int   gv_node_name2id_list[2000];

int gv_retcode = 0;

MS_Mon_Zone_Info_Entry_Type *gp_zi = 0;
int *gp_node_id = 0;

char                   my_name[BUFSIZ];
TPT_DECL2             (gv_phandle,MAX_THR_S);

typedef struct  {
    SB_ML_Type     link;
    bool           fin;
    BMS_SRE        sre;
} Test_SRE;
enum { MAX_SRES = MAX_THR_S * 3 };
Test_SRE sres[MAX_SRES];

SB_Thread::Thread *thrc[MAX_THR_C];

void rc_open_server(int pv_inx)
{
  int  lv_oid;
  char lv_lname[10];
  int  lv_ferr;
  
  sprintf(lv_lname, "$RCSVR%d", pv_inx);

  if (gv_verbose >= VERBOSE_LEVEL_MEDIUM) 
    printf("opening the process: %s \n", lv_lname);

  lv_ferr = msg_mon_open_process(lv_lname,
                                 TPT_REF2(gv_phandle,pv_inx),
                                 &lv_oid);
  if (lv_ferr != 0) {
    if (gv_verbose >= VERBOSE_LEVEL_LOW) 
      printf("Error connecting to %s\n", lv_lname);
  }
}

void rc_close_server(int pv_inx)
{
  if (TPT_REF2_NULL(gv_phandle,pv_inx)) {
    return;
  }

  msg_mon_close_process(TPT_REF2(gv_phandle,pv_inx));
}

// setup the number of client threads
void rc_setup_maxc() {

    if (gv_maxc == -1) {
      gv_maxc = gv_default_maxc;
    }

    if (gv_maxc > gv_maxsp) {
      gv_maxc = gv_maxsp;
    }

    if (gv_maxc > MAX_THR_C) {
      gv_maxc = MAX_THR_C;
    }

    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM) 
      printf("maxc=%d, gv_maxsp=%d\n", gv_maxc, gv_maxsp);
}

void rc_endgame() {

  int lv_inx;
  int lv_status;
  void *res;

  for (lv_inx = 0; lv_inx < gv_maxc; lv_inx++) {
    lv_status = thrc[lv_inx]->join(&res);
    if (lv_status != 0) {
      if (gv_verbose >= VERBOSE_LEVEL_MEDIUM) {
        printf("Error: %d trying to join with the client thread: %inx \n", 
               lv_status,
               lv_inx
               );
      }
    }

    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM)
      printf("joined with client %d\n", lv_inx);

  }

  for (lv_inx = 0; lv_inx < gv_maxsp; lv_inx++) {
    rc_close_server(lv_inx);
  }

  msg_mon_process_shutdown();

}


void rc_getcluster_info() {

    int lv_num_allocated = TC_NODES_MAX;
    int lv_num_nodes = 0;

    gp_zi = (MS_Mon_Zone_Info_Entry_Type *) calloc(lv_num_allocated, 
						  sizeof(MS_Mon_Zone_Info_Entry_Type));
    if (gp_zi == 0) {
      fprintf(stderr, "Unable to allocate memory to get cluster info... exitting\n");
      fflush(stderr);
      exit(1);
    }

    int lv_ret = msg_mon_get_zone_info(&lv_num_nodes,
				       lv_num_allocated,
				       gp_zi);
    if (lv_ret != 0) {
      fprintf(stderr, "Error %d returned... exitting\n", lv_ret);
      fflush(stderr);
      exit(1);
    }

    gp_node_id = (int *) calloc(lv_num_nodes, sizeof(int));
    if (gp_node_id == 0) {
      fprintf(stderr, "Unable to allocate memory to store cluster info... exitting\n");
      fflush(stderr);
      exit(1);
    }

    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM) {
      fprintf(stdout, "num nodes: %d \n", lv_num_nodes);
      fflush(stdout);
    }

    gv_maxsp = lv_num_nodes;

    for (int i = 0; i < (int) (sizeof(gv_node_name2id_list)/sizeof(int)) ; i++) {
      gv_node_name2id_list[i] = -1;
    }

    int lv_node_id_index = 0;
    for (int i = 0; i < lv_num_nodes; i++) {
      gp_node_id[i] = -1;

      if (gv_verbose >= VERBOSE_LEVEL_MEDIUM) {
	fprintf(stdout, "%d %s\n", gp_zi[i].zid, gp_zi[i].node_name);
	fflush(stdout);
      }

      // if not all nodes (via the argument -a), then node names
      // have to be provided via the argument -w.
      if (!gv_all_nodes) {
        int lv_i = 0;
        bool lv_found = false;
        while (gv_node_name_list[lv_i] != NULL) {
          if (strcmp(gp_zi[i].node_name, gv_node_name_list[lv_i]) == 0) {
	    if (gp_zi[i].pstate != MS_Mon_State_Up) {
	      printf("%s: ssh exited with exit code 255. EsgynDB not up.\n",
		     gv_node_name_list[lv_i]
		     );
	      gv_retcode = 255;

	    }
            gv_node_name2id_list[lv_i] = gp_zi[i].zid;
            lv_found = true;
            break;
          }
          lv_i++;
        }
        if (! lv_found) {
          continue;
        }
      }
      gp_node_id[lv_node_id_index++] = gp_zi[i].zid;
    }

    if (!gv_all_nodes) {
      for(int lv_i = 0; (gv_node_name_list[lv_i] != NULL); lv_i++) {
        if (gv_node_name2id_list[lv_i] == -1) {
          printf("%s: Could not resolve hostname %s: Name or service not known. Not found in EsgynDB configuration.\n",
                 gv_node_name_list[lv_i],
                 gv_node_name_list[lv_i]
                 );
	      gv_retcode = 255;
        }
      }
    }

    gv_maxsp = lv_node_id_index;

    // print the node id's being used
    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM) {
      printf("Node id's being used: \n");
      for (int i = 0; i < lv_node_id_index; i++) {
        printf("%d ", gp_node_id[i]);
      }
      printf("\n");
    }

    return;
}

void *rc_client_thr(void *arg) {
  int             ferr;
  int             inx = 0;
  int             msgid;
  Util_AA<char>   recv_buffer(MAX_BUF);
  Util_AA<short>  recv_buffer3(MAX_BUF);
  RT              results;
  Util_AA<char>   send_buffer(MAX_BUF);
  Util_AA<short>  send_buffer2(MAX_BUF);
  int             srv;
  int             whoami = inxc++;

  if (gv_cmd == 0) {
    return NULL;
  }

  SB_Thread::Thread *lp_arg = (SB_Thread::Thread *) arg;
  char *lv_thr_name = 0;
  int lv_thr_id = -1;
  if (lp_arg != 0) {
    lv_thr_name = lp_arg->get_name();
    lv_thr_id = atoi(lv_thr_name);
    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM)
      printf("c-%d client_thr_name: %s, client_thr_id: %d\n", whoami, lv_thr_name, lv_thr_id);
  }

  for (int lv_index = lv_thr_id; lv_index < gv_maxsp; lv_index += gv_maxc) {
    srv = gp_node_id[lv_index];
    if (srv < 0) {
      continue;
    }

    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM) 
      printf("client_thr, lv_index: %d, srv: %d, maxc: %d\n", lv_index, srv, gv_maxc);

    rc_open_server(srv);

    if (TPT_REF2_NULL(gv_phandle,srv)) {
      continue;
    }
    
    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM)
      printf("c-%d, inx=%d, srv: %d\n", whoami, inx, srv);

    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM)
      printf("c-%d: sending to srv: %d\n", whoami, srv);

    strcpy(send_buffer.ip_v, gv_cmd);
    memset(&recv_buffer, 0, MAX_BUF);
    ferr = BMSG_LINK_(TPT_REF2(gv_phandle,srv),       // phandle
                      &msgid,                      // msgid
                      &send_buffer2,               // reqctrl
                      (ushort) (inx & 1),          // reqctrlsize
                      &recv_buffer3,               // replyctrl
                      (ushort) 1,                  // replyctrlmax
                      &send_buffer,                // reqdata
                      dsize,                       // reqdatasize
                      &recv_buffer,                // replydata
                      MAX_BUF,                     // replydatamax
                      0,                           // linkertag
                      0,                           // pri
                      0,                           // xmitclass
                      0);                          // linkopts

    if (ferr != XZFIL_ERR_OK) {
      gv_retcode = 1;
      return NULL;
    }

    ferr = BMSG_BREAK_(msgid, results.u.s, TPT_REF2(gv_phandle,srv));
    if (ferr != XZFIL_ERR_OK) {
      gv_retcode = 1;
      return NULL;
    }

    if (gv_verbose >= VERBOSE_LEVEL_LOW) {
      printf("%d:%d:", lv_index, srv);
    }

    // Check if the last character in the received buffer is a new line character.
    // If it's not, then insert a new line in the output. 
    // Without this, the last line in the output from the current node and the first line from the 
    // next node (if any), gets printed on the same line.
    char *lv_recv_buf = &recv_buffer;
    if (results.u.t.data_size > 0) {
      if (lv_recv_buf[results.u.t.data_size - 1] == '\n') {
        printf("%s", &recv_buffer);
      }
      else {
      printf("%s\n", &recv_buffer);
      }    
    }  
    
    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM)
        printf("%s: received\n", name);
  }

  return NULL;
}


int main(int argc, char *argv[]) {

    int   ferr;
    int   inx;
    TAD   zargs[] = {
      { "-a",           TA_Bool, TA_NOMAX,    &gv_all_nodes },
      { "-cmd",         TA_Str,  MAX_COMMAND_REQUEST_LEN,    &gv_cmd       },
      { "-dsize",       TA_Int,  MAX_BUF,     &dsize        },
      { "-maxc",        TA_Int,  MAX_THR_C,   &gv_maxc         },
      { "-maxsp",       TA_Int,  TA_NOMAX,    &gv_maxsp     },
      { "-name",        TA_Str,  TA_NOMAX,    &name         },
      { "-v",           TA_Int,  TA_NOMAX,    &gv_verbose      },
      { "-w",           TA_StrArray,  128,    gv_node_name_list },
      { "",             TA_Str,  MAX_COMMAND_REQUEST_LEN,    &gv_cmd_array[0]       },
      { "",             TA_End,  TA_NOMAX,    NULL          }
    };

    CALL_COMP_DOVERS(rc_client, argc, argv);

    try {
      msfs_util_init_attach(&argc, &argv, msg_debug_hook, false, (char *) "");
    }
    catch (...) {
      fprintf(stderr, "Error in attaching to the monitor. Please ensure that the Trafodion env is up. Exitting...\n");
      fflush(stderr);
      exit(1);
    }

    gv_cmd_array[0] = (char *) calloc(MAX_COMMAND_REQUEST_LEN+1, sizeof(char));
    gv_cmd = gv_cmd_array[0];
    arg_proc_args(zargs, false, argc, argv);

    // just retain the name before the '.' (if any)
    int i = 0;
    char *lp_dot = 0;
    while (gv_node_name_list[i] != NULL) {
      lp_dot = 0;
      lp_dot = strchr(gv_node_name_list[i], '.');
      if (lp_dot) {
	*lp_dot = 0;
      }
      i++;
    }

    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM) {
      i = 0;
      while (gv_node_name_list[i] != NULL) {
        printf("node_name: %s\n", gv_node_name_list[i]);
        i++;
      }
    }

    if (gv_verbose >= VERBOSE_LEVEL_MEDIUM)
      printf("cmd: %s\n", gv_cmd);

    try {
      util_test_start(true);
      ferr = msg_mon_process_startup(false); // system messages
      if (ferr != 0) {
        printf("Error %d while starting the process. Exitting..\n", ferr);
        exit(1);
      }
    }
    catch (...) {
      fprintf(stderr, "Please ensure that the Trafodion env is up. Exitting...\n");
      fflush(stderr);
      exit(1);
    }

    rc_getcluster_info();

    rc_setup_maxc();

    // setup threads
    for (inx = 0; inx < gv_maxc; inx++) {
      char lname[10];
      sprintf(lname, "%d", inx);
      thrc[inx] = new SB_Thread::Thread(rc_client_thr, lname);
    }
    
    util_gethostname(my_name, sizeof(my_name));

    for (inx = 0; inx < gv_maxc; inx++)
      thrc[inx]->start();

    rc_endgame();

    util_test_finish(true);

    return gv_retcode;
}
