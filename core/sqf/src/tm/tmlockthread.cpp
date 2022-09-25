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

#include <sys/types.h>
#include <stdlib.h>
#include <string.h>

#include "seabed/ms.h"
#include "seabed/thread.h"

// Define XATM_LIB here because currently this code is in the XATM Library
#define XATM_LIB

#include "tminfo.h"
#include "seabed/trace.h"
#include "tmlogging.h"
#include "tmtime.h"
#include "tmtimer.h"
#include "tmlockthread.h"
#include "tmglob.h"

//----------------------------------------------------------------------------
// CTmLockThread Constructor
// Constructs a CTmLockThread object.
//----------------------------------------------------------------------------
CTmLockThread::CTmLockThread(SB_Thread::Sthr::Function pv_fun, int64 pv_num, const char *pp_name)
   :CTmThread(pv_fun, pv_num, pp_name)
{
   TMTrace(2, ("CTmLockThread::CTmLockThread : ENTRY.\n"));
  
   start(); //Start the lock thread

   TMTrace(2, ("CTmLockThread::CTmLockThread : EXIT.\n"));
} //CTmThreadExample::CTmThreadExample


//----------------------------------------------------------------------------
// CTmLockThread Destructor
//----------------------------------------------------------------------------
CTmLockThread::~CTmLockThread()
{
   TMTrace(2, ("CTmLockThread::~CTmLockThread : EXIT\n"));
}


// --------------------------------------------------------------
// CTmLockThread::eventQ_push
// Purpose - push a new event to the threads event queue.
// These are pushed in FIFO order.
// --------------------------------------------------------------
void CTmLockThread::eventQ_push(CTmEvent * pp_event)
{ 
    CTmEvent *lp_event = (CTmEvent *) pp_event; 
    if (lp_event == NULL)
    {
        tm_log_event(DTM_TMTIMER_BAD_EVENT, SQ_LOG_CRIT, "DTM_TMTIMER_BAD_EVENT");
        TMTrace(1, ("CTmLockThread::eventQ_push - LockThread request to be queued is NULL\n"));
        abort ();
    }

   eventQ()->push(lp_event);

   TMTrace(2, ("CTmLockThread::eventQ_push : signaling lock thread, event %p, "
            "request %d.\n",
            (void *) lp_event,
            lp_event->requestType()));
   
   eventQ_CV()->signal(true /*lock*/); 
} //CTmLockThread::eventQ_push


// --------------------------------------------------------------
// CTmLockThread::eventQ_pop
// Purpose - pop an event from the end of the queue.  Events are
// always processed in FIFO order.
// --------------------------------------------------------------
CTmEvent * CTmLockThread::eventQ_pop()
{
   CTmEvent *lp_event = NULL;

   TMTrace(2, ("CTmLockThread::eventQ_pop ENTRY.\n"));

   // Wait forever for a signal from eventQ_push
   eventQ_CV()->wait(true /*lock*/);

   if (!eventQ()->empty())
   {
      // New event arrived
      lp_event = (CTmEvent *) eventQ()->pop_end();
   }

   TMTrace(2, ("CTmLockThread::eventQ_pop EXIT : Returning event %p, "
            "request %d.\n",
            (void *) lp_event, 
            ((lp_event)?lp_event->requestType():0)));
   return lp_event;
} // CTmLockThread::eventQ_pop

//----------------------------------------------------------------------------
// lockThread_main
// Purpose : Main for lock thread
//----------------------------------------------------------------------------
void * lockThread_main(void *arg)
{
   CTmTimerEvent    *lp_event;
   CTmLockThread *lp_lockTh;
   bool              lv_exit = false;
   CTmTxMessage *lp_msg;
   
   arg = arg;

   TMTrace(2, ("lockThread_main : ENTRY.\n"));
   while (gv_tm_info.tmLock() == NULL)
   {
      SB_Thread::Sthr::usleep(10);
   }
   
   // Now we should be able to set a pointer to the tmLock object because it exits
   // I've just used a global here for simplicity.
   lp_lockTh = gv_tm_info.tmLock();

   if (!lp_lockTh)
      abort();
   
   TMTrace(2, ("lockThread_main : Thread %s(%p) State Up.\n",
      lp_lockTh->get_name(), (void *) lp_lockTh));

   while (!lv_exit)
   {
      lp_event = (CTmTimerEvent *) lp_lockTh->eventQ_pop();

      if (lp_event)
      {
 	lp_msg = (CTmTxMessage *) lp_event;

	switch (lp_event->requestType())
	{
	    case TM_MSG_TYPE_UNLOCKTM:                       // LDTM
	    case (TM_MSG_TYPE_UNLOCKTM + TM_TM_MSG_OFFSET):  // non LDTM
	    {
	      TMTrace(3, ("lockThread_main : TM_MSG_UNLOCKTM received\n"));
	      gv_tm_info.unlockTm(lp_msg);
	      delete lp_event;
	      lp_event = NULL;
	      break;
	    }
	    case TM_MSG_TYPE_LOCKTM:                          // LDTM
	    case (TM_MSG_TYPE_LOCKTM + TM_TM_MSG_OFFSET) :    // non LDTM
	    {
	      TMTrace(3, ("lockThread_main : TM_MSG_LOCKTM received\n"));
	      gv_tm_info.lockTm(lp_msg);
	      delete lp_event;
	      lp_event = NULL;
	      break;
	    }
	    case TM_MSG_TYPE_SDNTRAN:                       // LDTM
	    case (TM_MSG_TYPE_SDNTRAN + TM_TM_MSG_OFFSET):  // non LDTM
	    {
	      TMTrace(3, ("lockThread_main : TM_MSG_TYPE_SDNTRAN received\n"));
	      gv_tm_info.sdnTrans(lp_msg);
	      delete lp_event;
	      lp_event = NULL;
	      break;
	    }
	    default:
	    {
	      TMTrace(3, ("lockThread_main : UNKNOWN command type\n"));
	      break;
	    }	      
	 }
      }
   } //while

   TMTrace(2, ("lockThread_main : EXIT.\n"));

   lp_lockTh->stop();
   return NULL;
} //lockThread_main
