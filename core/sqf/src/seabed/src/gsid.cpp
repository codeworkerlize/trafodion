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

#include <string>
#include <assert.h>
#include <ctype.h>
#include <pthread.h>
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

#include "idtmsrv.h"
#include "mstrace.h"

static bool            gv_inited  = false;
static pthread_mutex_t gv_mutex   = PTHREAD_MUTEX_INITIALIZER;
static int             gv_oid     = -1;
static SB_Phandle_Type gv_gsid_phandle;

#include "gsidclicom.h"


//
// initialize.
//
// if first call, attach and startup.
// open server.
//
// return file error.
//
static int do_seqid_init() {
    const char   *WHERE = "do_seqid_init";
    int   lv_ferr;
    int   lv_perr;

    if (gv_ms_trace_mon)
        trace_where_printf(WHERE, "ENTER\n");

    lv_perr = pthread_mutex_lock(&gv_mutex);
    assert(lv_perr == 0);
    if (gv_inited)
        lv_ferr = XZFIL_ERR_OK;
    else {
        if (gv_oid < 0) {
            lv_ferr = do_seqid_cli_open(&gv_gsid_phandle, &gv_oid);
            if (gv_ms_trace_mon)
                trace_where_printf(WHERE, "open-err=%d\n", lv_ferr);
            if (lv_perr == 0)
                gv_inited = true;
        }
    }
    lv_perr = pthread_mutex_unlock(&gv_mutex);
    assert(lv_perr == 0);

    if (gv_ms_trace_mon)
       trace_where_printf(WHERE, "EXIT ret=%d\n", lv_ferr);

    return lv_ferr;
}

//
// initialize.
//
// if first call, attach and startup.
// open server.
//
// return file error.
//
int msg_gsid_close_process(bool ingoreBoundsErr) {
    const char   *WHERE = "msg_gsid_close_process";
    int   lv_ferr;
    int   lv_perr;

    if (gv_ms_trace_mon)
        trace_where_printf(WHERE, "ENTER\n");

    lv_perr = pthread_mutex_lock(&gv_mutex);
    assert(lv_perr == 0);
    if (gv_inited) {
        lv_ferr = msg_mon_close_process(&gv_gsid_phandle);
        if (!(ingoreBoundsErr == true && lv_ferr == XZFIL_ERR_BOUNDSERR))
          assert(lv_ferr == XZFIL_ERR_OK);
        lv_ferr = XZFIL_ERR_OK;
    }
    lv_perr = pthread_mutex_unlock(&gv_mutex);
    assert(lv_perr == 0);

    if (gv_ms_trace_mon)
       trace_where_printf(WHERE, "EXIT ret=%d\n", lv_ferr);

    return lv_ferr;
}

//
// initialize.
// call do_seqid_cli_id() and set pp_id to returned id from do_seqid_cli_id()
//
// return file error
//
int msg_seqid_get_id(long *pp_id, int pv_timeout)
{
    const char   *WHERE = "msg_seqid_get_id";
    int      lv_ferr;
    long     lv_id;

    if (pv_timeout == XOMITINT)
        pv_timeout = -1;

    if (gv_ms_trace_mon)
        trace_where_printf(WHERE, "ENTER timeout=%d\n", pv_timeout);

    lv_ferr = do_seqid_init();
    lv_id = 0;
    if (lv_ferr == XZFIL_ERR_OK) {
        lv_ferr = do_seqid_cli_id(&gv_gsid_phandle, pv_timeout, &lv_id);
        if (lv_ferr == XZFIL_ERR_OK) {
            assert(lv_id != 0);
            *pp_id = lv_id;
        }
    }

    if (gv_ms_trace_mon)
        trace_where_printf(WHERE, "EXIT ret=%d, id=0x%lx\n", lv_ferr, lv_id);

    return lv_ferr;
}

