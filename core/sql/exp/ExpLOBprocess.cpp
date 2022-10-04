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
 * File:         ExpLOBprocess.cpp
 * Description:  class to store and retrieve LOB info from mxlobsrvr process.
 *               
 *               
 * Created:      10/29/2012
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
/*** Note *** This file is currently compiled and creates the mxlobsrvr executable. But the functions in this file are not active or used at this point. Code 
    maybe added in the near future to offload any tasks like garbage collection, to this process .Hence we are retainign this file as part of the mxlobsrvr 
infrastructure .If any functionas are added and need to be executed in the 
mxlobsrvr process, the sqstart/sqstop need to modified to call lobstop and 
lostart**/

/****************************************************************************/
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <malloc.h>
#include <string>
#include <errno.h>
#include <sys/file.h>

#include <iostream>
 
#include <errno.h>
#include <fcntl.h>       // for nonblocking
#include <netdb.h>
#include <pthread.h>
#include <signal.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <zlib.h>        // ZLIB compression library
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <sys/epoll.h>
#include <sys/socket.h>  // basic socket definitions
#include <sys/stat.h>
#include <sys/time.h>
#include <sys/types.h>   // basic system data types
#include <sys/uio.h>
#include <sys/wait.h>

#include <guardian/kphandlz.h>


#include <seabed/ms.h>
#include <seabed/fs.h>
#include <seabed/pctl.h>
#include <seabed/pevents.h>
#include <seabed/fserr.h>

#include "common/ComRtUtils.h"
//#include "ExeReplInterface.h"
#include "cli/Globals.h"
#include "common/NAExit.h"
#include "ex_ex.h"                  // ex_assert
#include "common/SCMVersHelp.h"

#define SQ_USE_LOB_PROCESS 1
#include "qmscommon/QRLogger.h"

extern int ms_transid_reg(MS_Mon_Transid_Type, MS_Mon_Transseq_Type, SB_Int64_Type, SB_Int64_Type);
extern void ms_transid_clear(MS_Mon_Transid_Type, MS_Mon_Transseq_Type);
extern "C" short GETTRANSID(short *transid);
extern "C" short JOINTRANSACTION(Int64 transid);
extern "C" short SUSPENDTRANSACTION(short *transid);
#define TRANSID_IS_VALID(idin) (idin.id[0] != 0)

using namespace std;

// Session State values
enum {
      IDLE_STATE      =  0,
      WAITING         =  1,
      DATA_PENDING    =  2,
      WRITE_PENDING   =  3,
      SEND_PENDING    =  4,
      SENDING_DATA    =  5,
      RQST_PENDING    =  6,
      REPLY_PENDING   =  7,
      WAITING_REPLY   =  8,
      END_SESSION     =  9,
      COMM_RESET      = 10,
      READ_POSTED     = 11,
// TCP/IP State Machine states
      LISTENER_INIT_STATE         = 20,
      LISTENER_SOCKOPT_STATE      = 21,
      LISTENER_ACCEPT_STATE       = 22,
      LISTENER_SHUTDOWN_STATE     = 23,
      LISTENER_CLOSE_STATE        = 24,

      SESSION_INIT_STATE          = 30,
      SESSION_CONNECT_STATE       = 31,
      SESSION_CONNECT_CHECK_STATE = 32,
      SESSION_SOCKOPT1_STATE      = 33,
      SESSION_SOCKOPT2_STATE      = 34,
      SESSION_RECV_STATE          = 35,
      SESSION_SEND_STATE          = 36,
      SESSION_SHUTDOWN_STATE      = 37,
      SESSION_CLOSE_STATE         = 38
};

// AWAITIO classes
enum {Class_0         = 0,
      Class_1         = 1,
      Class_2         = 2,
      Class_3         = 3,
      Class_4         = 4,
      Class_5         = 5,
      Class_6         = 6,
      Class_7         = 7};

void process_msg(BMS_SRE *sre)
{
    // do work here
    return;
}

class rcv_struct
{
public:
  union {
    unsigned char  *databuf;
    Lng32            *datalen;
  };
  Lng32               buflen;
  short             file;
  short             state;
};

#pragma fieldalign platform awaitio_tag_struct
class awaitio_tag_struct
{
public:
  union {
    SB_Tag_Type     Tag;
    struct {
      ULng32  Class:4;          // I/O class
      ULng32  State:12;         // State of this I/O operation.
      ULng32  Index:16;         // Index into control block table.
    };
  };
};

class awaitio_struct
{
public:
  Lng32               Iocount;
  short             Error;
  short             File;
};

enum {IDLE_TIMEOUT    = 30000,      // 5 minute wait before stopping cli process.
      MAX_RETRIES     = 3,          // Maximum number of retries before waiting.
      NUM_OPENS       = 64,         // Initial number of entries in the OCB table.
      RECV_BUFSIZE    = 56 * 1024,  // Size of $RECEIVE I/O buffer.
      RETRY_TIMEOUT   = 3000,       // 30 second wait before retrying.
      SOCK_BUFSIZE    = 56 * 1024,  // Size of socket I/O buffers.
      STARTUP_TIMEOUT = 500};       // CLI Invokers will die if an open message is not
                                    // received before a 5 sec startup timer expires.
rcv_struct          rcv;
// Mutexes and thread variables
pthread_mutex_t     cnfg_mutex;
pthread_mutex_t     g_mutex;
pthread_attr_t      thr_attr;
Lng32                 total_dynamic_memory;
CliGlobals         *cliGlobals    = NULL;
char               *myProgramName = NULL;
xzsys_ddl_smsg_def *sysmsg        = NULL;
FS_Receiveinfo_Type rcvinfo;

//*****************************************************************************
//*****************************************************************************
static void sigterm_handler(Lng32 signo)
{
    printf("sigterm received\n");
}

void LOB_process_stop(short flag)
{
  if (rcv.state == READ_POSTED)
    BCANCEL(rcv.file);
  else
    NAExit(flag);
}  // BDR_process_stop

//*****************************************************************************

static void *Calloc(size_t memsize)
{
  void     *ptr;
  Lng32       retval;

  retval = pthread_mutex_lock(&g_mutex);
  ex_assert(retval == 0, "Calloc 1");
  ptr = calloc(1, memsize);
  ex_assert(ptr != NULL, "Calloc 2");
  total_dynamic_memory += memsize;
  retval = pthread_mutex_unlock(&g_mutex);
  ex_assert(retval == 0, "Calloc 3");

  return(ptr);
}  // Calloc

void post_receive_read(void)
{
  _bcc_status         cc_status;
  awaitio_tag_struct  tag;

  if (rcv.state == END_SESSION)
    LOB_process_stop(0);

  if (rcv.databuf == NULL)
  {
    rcv.buflen  = RECV_BUFSIZE + 2*sizeof(int);
    rcv.databuf = (unsigned char *)Calloc(rcv.buflen);
    sysmsg = (xzsys_ddl_smsg_def *)&rcv.datalen[0];
  }

  tag.Tag   = 0;
  tag.Class = Class_7;

  cc_status = BREADUPDATEX(rcv.file, (char *)&rcv.datalen[0], RECV_BUFSIZE,
                           NULL, tag.Tag);
  ex_assert(_status_eq(cc_status), "post_receive_read 1");
  rcv.state = READ_POSTED;

  return;
}  // post_receive_read

short process_open(void)
{
  Lng32         retval;
  short       retvals = XZFIL_ERR_OK;

  return retvals;
}  // process_open

//*****************************************************************************
//*****************************************************************************
short process_close(void)
{
  Lng32               retval;
  short             retvals = XZFIL_ERR_OK;

  return retvals;
}  // process_close


void process_mon_msg(MS_Mon_Msg *msg) {
    printf("server received monitor msg, type=%d\n", msg->type);

    switch (msg->type) {
    case MS_MsgType_Change:
        printf("  type=%d, group=%s, key=%s, value=%s\n",
               msg->u.change.type,
               msg->u.change.group,
               msg->u.change.key,
               msg->u.change.value);
        break;
    case MS_MsgType_Close:
        printf("  nid=%d, pid=%d, process=%s, aborted=%d\n",
               msg->u.close.nid,
               msg->u.close.pid,
               msg->u.close.process_name,
               msg->u.close.aborted);
        break;
    case MS_MsgType_Event:
        break;
    case MS_MsgType_NodeDown:
        printf("  nid=%d, node=%s\n",
               msg->u.down.nid,
               msg->u.down.node_name);
        break;
    case MS_MsgType_NodeUp:
        printf("  nid=%d, node=%s\n",
               msg->u.up.nid,
               msg->u.up.node_name);
        break;
    case MS_MsgType_Open:
        printf("  nid=%d, pid=%d, process=%s, death=%d\n",
               msg->u.open.nid,
               msg->u.open.pid,
               msg->u.open.process_name,
               msg->u.open.death_notification);
        break;
    case MS_MsgType_ProcessCreated:
        printf("  nid=%d, pid=%d, tag=0x%llx, process=%s, ferr=%d\n",
               msg->u.process_created.nid,
               msg->u.process_created.pid,
               msg->u.process_created.tag,
               msg->u.process_created.process_name,
               msg->u.process_created.ferr);
        break;
    case MS_MsgType_ProcessDeath:
        printf("  nid=%d, pid=%d, aborted=%d, process=%s\n",
               msg->u.death.nid,
               msg->u.death.pid,
               msg->u.death.aborted,
               msg->u.death.process_name);
        break;
    case MS_MsgType_Service:
        break;
    case MS_MsgType_Shutdown:
        printf("  nid=%d, pid=%d, level=%d\n",
               msg->u.shutdown.nid,
               msg->u.shutdown.pid,
               msg->u.shutdown.level);
        break;
    default:
        break;
    }
}



