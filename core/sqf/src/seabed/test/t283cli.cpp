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
#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include <sys/time.h>

#include "seabed/fserr.h"
#include "seabed/ms.h"
#include "seabed/pctl.h"
#include "seabed/pevents.h"

#include "tchkfe.h"
#include "tms.h"
#include "tmsfsutil.h"
#include "tutil.h"
#include "tutilp.h"

#include "t284.h"

bool chook   = false;
bool client  = false;
int  loop    = 10;
bool verbose = false;


//
// initialize
//
void do_init(int argc, char **argv) {
    int   arg;
    char *argp;
    int   ferr;

    ferr = msg_init(&argc, &argv);
    assert(ferr == XZFIL_ERR_OK);

    for (arg = 1; arg < argc; arg++) {
        argp = argv[arg];
        if (strcmp(argp, "-chook") == 0)
            chook = true;
        else if (strcmp(argp, "-client") == 0) {
            client = true;
        } else if (strcmp(argp, "-inst") == 0) {
            arg++;
        } else if (strcmp(argp, "-loop") == 0) {
            if ((arg + 1) < argc) {
               arg++;
               argp = argv[arg];
               loop = atoi(argp);
            } else {
                printf("-loop expecting <loop>\n");
                exit(1);
            }
        } else if (strcmp(argp, "-maxcp") == 0) {
            arg++;
        } else if (strcmp(argp, "-maxsp") == 0) {
            arg++;
        } else if (strcmp(argp, "-name") == 0) {
            arg++;
        } else if (strcmp(argp, "-v") == 0) {
            verbose = true;
        } else {
            printf("unknown argument=%s\n", argp);
            exit(1);
        }
    }

    if (chook)
        msg_debug_hook("c", "c");

    ferr = msg_mon_process_startup(false);  // system messages?
    assert(ferr == XZFIL_ERR_OK);
}

//
// client main
//
int main(int argc, char *argv[]) {
    enum {           TO = 1000 };
    int              ferr;
    long             id;
    int              inx;
    long             t_elapsed;
    struct timeval   t_start;
    struct timeval   t_stop;

    util_test_start(client);
    do_init(argc, argv);

    gettimeofday(&t_start, NULL);
    for (inx = 0; inx < loop; inx++) {
        ferr = msg_seqid_get_id(&id, TO);
        if (verbose)
            printf("cli: id=0x%lx, ferr=%d\n", id, ferr);
        assert(ferr == XZFIL_ERR_OK);
    }
    gettimeofday(&t_stop, NULL);
    t_elapsed = (t_stop.tv_sec * 1000000 + t_stop.tv_usec) -
                (t_start.tv_sec * 1000000 + t_start.tv_usec);
    printf("elapsed time (gettimeofday us)=%ld, ops/sec=%f\n",
           t_elapsed, (double) loop / ((double) t_elapsed / 1000000));

    ferr = msg_mon_process_shutdown();
    TEST_CHK_FEOK(ferr);
    util_test_finish(client);
    return 0;
}
