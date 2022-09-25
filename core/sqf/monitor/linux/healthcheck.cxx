///////////////////////////////////////////////////////////////////////////////
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
//
///////////////////////////////////////////////////////////////////////////////

using namespace std;

#include <limits.h>
#include <stdio.h>
#include <unistd.h>
#include <assert.h>
#include <sys/epoll.h>
#include <errno.h>
#include <stdlib.h>
#include <string.h>
#include <sys/time.h>
#include <fcntl.h>
#include <signal.h>
#include <sys/stat.h>
#include <sys/resource.h>
#include <signal.h>

#include "healthcheck.h"
#include "monlogging.h"
#include "montrace.h"
#include "monitor.h"
#include "seabed/trace.h"
#include "clusterconf.h"
#include "lnode.h"
#include "pnode.h"
#include "mlio.h"
#include "reqqueue.h"
#include "process.h"
#include "redirector.h"
#include "replicate.h"
#include "zclient.h"

extern CReqQueue ReqQueue;
extern CMonitor *Monitor;
extern CNodeContainer *Nodes;
extern CNode *MyNode;
#ifndef NAMESERVER_PROCESS
extern CRedirector Redirector;
#endif
extern CHealthCheck HealthCheck;
extern CReplicate Replicator;
extern CZClient    *ZClient;
extern int MyPNID;
extern bool IsRealCluster;
extern bool ZClientEnabled;
extern char MasterMonitorName[MAX_PROCESS_PATH];

const char *StateString( STATE state);

const char *HealthCheckStateString( HealthCheckStates state)
{
    const char *str;
    
    switch( state )
    {
        case HC_AVAILABLE:
            str = "HC_AVAILABLE";
            break;
        case HC_UPDATE_SMSERVICE:
            str = "HC_UPDATE_SMSERVICE";
            break;
        case HC_UPDATE_WATCHDOG:
            str = "HC_UPDATE_WATCHDOG";
            break;
        case MON_READY:
            str = "MON_READY";
            break;
        case MON_SHUT_DOWN:
            str = "MON_SHUT_DOWN";
            break;
        case MON_NODE_QUIESCE:
            str = "MON_NODE_QUIESCE";
            break;
        case MON_SCHED_NODE_DOWN:
            str = "MON_SCHED_NODE_DOWN";
            break;
        case MON_NODE_DOWN:
            str = "MON_NODE_DOWN";
            break;
        case MON_STOP_WATCHDOG:
            str = "MON_STOP_WATCHDOG";
            break;
        case MON_START_WATCHDOG:
            str = "MON_START_WATCHDOG";
            break;
        case MON_EXIT_PRIMITIVES:
            str = "MON_EXIT_PRIMITIVES";
            break;
        case MON_CHECK_LICENSE:
            str = "MON_CHECK_LICENSE";
            break;
        case HC_EXIT:
            str = "HC_EXIT";
            break;
        default:
            str = "HealthCheckState - Undefined";
            break;
    }

    return( str );
}


// constructor
CHealthCheck::CHealthCheck()
    : thread_id_(0)
{
    const char method_name[] = "CHealthCheck::CHealthCheck";
    TRACE_ENTRY;

    clock_gettime(CLOCK_REALTIME, &currTime_);
    lastReqCheckTime_ = currTime_;
    lastSyncCheckTime_ = currTime_;
    nextHealthLogTime_ = currTime_;
    quiesceStartTime_.tv_sec = 0;
    quiesceStartTime_.tv_nsec = 0;
    nonresponsiveTime_.tv_sec = 0;
    nonresponsiveTime_.tv_nsec = 0;
#ifndef NAMESERVER_PROCESS
    licenseCheckTime_.tv_sec = 0;
    licenseCheckTime_.tv_nsec = 0;
    licenseExpireTime_.tv_sec = 0;
    licenseExpireTime_.tv_nsec = 0;
    nodeFailedTime_.tv_sec = 0;
    nodeFailedTime_.tv_nsec = 0;
#endif

    lastIdtmCheckTime_ = currTime_;
    lastIdtmRefresh_ = 0;
    lastDTMRefresh_ = 0;
    state_ = HC_AVAILABLE; // default state
    param1_ = 0;
    watchdogProcess_ = NULL;
    smserviceProcess_ = NULL;

    initializeVars();

    monSyncTimeout_ = -1;
    char *monSyncTimeoutC = getenv("SQ_MON_SYNC_TIMEOUT");
    if ( monSyncTimeoutC ) 
    {
       monSyncTimeout_ = atoi(monSyncTimeoutC);
    }

    checkReqResponsive_ = false;
    char *checkReqResponsiveC = getenv("SQ_MON_REQ_RESPONSIVE");
    if (checkReqResponsiveC && atoi(checkReqResponsiveC) == 1)
    {
       checkReqResponsive_ = true;
    }

    enableMonDebugging_ = false;
    char *enableDebuggingC = getenv("SQ_MON_ENABLE_DEBUG");
    if (enableDebuggingC && atoi(enableDebuggingC) == 1)
    {
       enableMonDebugging_ = true;
    }

    quiesceTimeoutSec_     = CHealthCheck::QUIESCE_TIMEOUT_DEFAULT;
    char *quiesceTimeoutC = getenv("SQ_QUIESCE_WAIT_TIME");
    if (quiesceTimeoutC)
    {
        quiesceTimeoutSec_     = atoi(quiesceTimeoutC);
    }

    healthLoggingFrequency_    = CHealthCheck::HEALTH_LOGGING_FREQUENCY_DEFAULT;
    char *healthLoggingFrequencyC = getenv("SQ_HEALTH_LOGGING_FREQUENCY");
    if (healthLoggingFrequencyC)
    {
        healthLoggingFrequency_     = atoi(healthLoggingFrequencyC);
        if (healthLoggingFrequency_ < CHealthCheck::HEALTH_LOGGING_FREQUENCY_MIN)
        {
            healthLoggingFrequency_ = CHealthCheck::HEALTH_LOGGING_FREQUENCY_MIN;
        }
    }
    nextHealthLogTime_.tv_sec += healthLoggingFrequency_;

    idtmCheckInterval_ = CHealthCheck::IDTM_CHECK_INTERVAL;
    char *idtmCheckInt = getenv("IDTM_CHECK_INTERVAL");
    if (idtmCheckInt)
    {
      idtmCheckInterval_ = atoi(idtmCheckInt);
    }

#ifndef NAMESERVER_PROCESS
    licenseFile_ = NULL;
    licenseValidateSuccess_ = false;
#endif

#ifdef NAMESERVER_PROCESS
    cpuSchedulingDataEnabled_ = false;
#else
    cpuSchedulingDataEnabled_ = true;
    char *env;
    env = getenv("SQ_CPUSCHEDULINGDATA_ENABLED");
    if ( env && isdigit(*env) )
    {
        cpuSchedulingDataEnabled_ = atoi(env);
    }
#endif

    if (trace_settings & TRACE_HEALTH)
        trace_printf("%s@%d quiesceTimeoutSec_ = %d, syncTimeoutSec_ = %d, workerTimeoutSec_ = %d, idtmCheckInterval_ = %d\n", method_name, __LINE__, quiesceTimeoutSec_, SYNC_MAX_RESPONSIVE, CReqQueue::REQ_MAX_RESPONSIVE, idtmCheckInterval_);

    TRACE_EXIT;
}

void CHealthCheck::initializeVars()
{
    const char method_name[] = "CHealthCheck::initializeVars";
    TRACE_ENTRY;

    quiesceCountingDown_ = false;
    nodeDownScheduled_ = false;
    wakeupTimeSaved_ = 0;
    refreshCounter_ = 0;

    TRACE_EXIT;
}

// destructor
CHealthCheck::~CHealthCheck()
{
}

const char *CHealthCheck::getStateStr(HealthCheckStates state)
{
    const char *ret;
    switch (state)
    {
    case HC_AVAILABLE:
        ret = "HC_AVAILABLE";
        break;
    case HC_UPDATE_SMSERVICE:
        ret = "HC_UPDATE_SMSERVICE";
        break;
    case HC_UPDATE_WATCHDOG:
        ret = "HC_UPDATE_WATCHDOG";
        break;
    case MON_READY:
        ret = "MON_READY";
        break;
    case MON_SHUT_DOWN:
        ret = "MON_SHUT_DOWN";
        break;
    case MON_NODE_QUIESCE:
        ret = "MON_NODE_QUIESCE";
        break;
    case MON_SCHED_NODE_DOWN:
        ret = "MON_SCHED_NODE_DOWN";
        break;
    case MON_NODE_DOWN:
        ret = "MON_NODE_DOWN";
        break;
    case MON_STOP_WATCHDOG:
        ret = "MON_STOP_WATCHDOG";
        break;
    case MON_START_WATCHDOG:
        ret = "MON_START_WATCHDOG";
        break;
    case MON_EXIT_PRIMITIVES:
        ret = "MON_EXIT_PRIMITIVES";
        break;
    case HC_EXIT:
        ret = "HC_EXIT";
        break;
    default:
        ret = "?";
        break;
    }
    return ret;
}

#ifdef NAMESERVER_PROCESS
void CHealthCheck::stopNameServer()
{
    const char method_name[] = "CHealthCheck::stopNameServer";
    TRACE_ENTRY;

    if (trace_settings & TRACE_HEALTH)
        trace_printf("%s@%d stopping.\n", method_name, __LINE__);
    char buf[MON_STRING_BUF_SIZE];
    sprintf(buf, "[%s], stopping.\n", method_name);
    mon_log_write(MON_HEALTHCHECK_STOP_NS_1, SQ_LOG_CRIT, buf);

    mon_failure_exit();
}
#endif

// Main body:
// The health check thread waits on two conditions. A signal that is set by other monitor
// threads after updating the state, and a timer event that pops when the timer expires. 
// It goes back to the timed wait after processing the events. 
void CHealthCheck::healthCheckThread()
{
    const char method_name[] = "CHealthCheck::healthCheckThread";
    TRACE_ENTRY;

    HealthCheckStates state;
#ifndef NAMESERVER_PROCESS
    int myPid = getpid();
#endif
    struct timespec ts;

    if (trace_settings & TRACE_HEALTH)
        trace_printf("%s@%d health check thread starting\n", method_name, __LINE__);

    setTimeToWakeUp(ts);

#ifndef NAMESERVER_PROCESS
    bool isInternal = checkLicenseInternal();
#endif

    if (trace_settings & (TRACE_HEALTH | TRACE_INIT | TRACE_RECOVERY))
    {
        trace_printf( "%s@%d quiesceTimeoutSec_ = %d, syncTimeoutSec_ = %d, "
                      "workerTimeoutSec_ = %d, healthLoggingFrequency_= %d (secs)\n"
                    , method_name, __LINE__
                    , quiesceTimeoutSec_
                    , monSyncTimeout_
                    , CReqQueue::REQ_MAX_RESPONSIVE
                    , healthLoggingFrequency_ );
    }

    // Wait for event or timer to expire
    while (!terminateMonitor)
    {
        healthCheckLock_.lock();

        healthCheckLock_.timedWait(&ts);

        clock_gettime(CLOCK_REALTIME, &currTime_);

        // if woken late by 3 seconds, log it.
        if ( ((currTime_.tv_sec - wakeupTimeSaved_) > 3) && watchdogProcess_)
        {
            mem_log_write(MON_HEALTHCHECK_WAKEUP_2, (int)(currTime_.tv_sec - wakeupTimeSaved_));
        }

#ifndef NAMESERVER_PROCESS
        // Check if we should validate license, but not on a virtual cluster
        if (!isInternal)
        {
            timeToVerifyLicense(currTime_, myPid);
        }
#endif

#ifndef NAMESERVER_PROCESS
#ifdef EXCHANGE_CPU_SCHEDULING_DATA
        timeToLogHealth( currTime_ );

        if (cpuSchedulingDataEnabled_)
        {
            // Replicate this host's CPU scheduling data to other nodes
            CReplSchedData *repl = new CReplSchedData();
            Replicator.addItem(repl);
        }
#endif
#endif

        state = state_;
#if 0
        if ( trace_settings & TRACE_HEALTH )
            trace_printf("%s@%d State: %s\n", method_name, __LINE__, HealthCheckStateString(state));
#endif

        switch(state)
        {
            case MON_START_WATCHDOG:
                // Start the watchdog timer. After this, WDT refresh becomes mandatory.
                sendEventToWatchDog(Watchdog_Start);
                state_ = HC_AVAILABLE;
                break;

            case MON_STOP_WATCHDOG:
                // Stop the watchdog timer. After this, WDT refresh is not needed.
                sendEventToWatchDog(Watchdog_Stop);
                state_ = HC_AVAILABLE;
                break;

            case MON_EXIT_PRIMITIVES:
                // Exit the critical primitive processes.
                sendEventToSMService(SMS_Exit); // SMS process will exit
                sendEventToWatchDog(Watchdog_Exit); // WDT process will exit
                state_ = HC_AVAILABLE;
                break;

            case MON_NODE_QUIESCE:
                // Monitor the quiescing work that has already been started.
                startQuiesce();
                state_ = HC_AVAILABLE;
                break;

            case MON_SCHED_NODE_DOWN:
                // schedule node down req on worker thread
                scheduleNodeDown();
                state_ = HC_AVAILABLE;
                break; 

            case MON_NODE_DOWN:
#ifdef NAMESERVER_PROCESS
                stopNameServer();
#endif
                if( getenv("SQ_VIRTUAL_NODES") )
                {
                    // In a virtual cluster the monitor continues to run
                    // just tell the watchdog process to exit normally
                    sendEventToWatchDog(Watchdog_Exit);
                    state_ = HC_AVAILABLE;
                }
                else
                {
                    // Bring down the node by expiring the watchdog process
                    sendEventToWatchDog(Watchdog_Expire);
                    // wait forever
                    for (;;)
                      sleep(10000); 
                }
                break;

            case MON_SHUT_DOWN:
#ifdef NAMESERVER_PROCESS
                stopNameServer();
#endif
                if( getenv("SQ_VIRTUAL_NODES") )
                {
                    // In a virtual cluster the monitor continues to run
                    // just tell the watchdog process to exit normally
                    sendEventToWatchDog(Watchdog_Exit);
                }
                else
                {
                    if ( watchdogProcess_ )
                    {
                        // Bring down the node by shutting down the watchdog process
                        sendEventToWatchDog(Watchdog_Shutdown);
                        // wait forever
                        for (;;)
                          sleep(10000);
                    }
                    else
                    {
                        // we must be a spare as we don't have WDT process
                        assert(MyNode->IsSpareNode());
                        // set the state to shutdown so that IamAlive can drive the exit.
                        MyNode->SetState( State_Shutdown );
                    }
                }
                state_ = HC_AVAILABLE;
                break;

            case HC_AVAILABLE:
                // no event to work on. Process the timer pop.
                processTimerEvent();
                break;

            case HC_EXIT:
                // health check thread should exit.
                terminateMonitor = true;
                break;

            case HC_UPDATE_SMSERVICE:
                // update watchdog process object.
                updateSMServiceProcess();
                state_ = HC_AVAILABLE;
                break;

            case HC_UPDATE_WATCHDOG:
                // update watchdog process object.
                updateWatchdogProcess();
                state_ = HC_AVAILABLE;
                break;
#ifndef NAMESERVER_PROCESS
            case MON_CHECK_LICENSE:
              // verify license
              verifyLicense();
              state_ = HC_AVAILABLE;
              break;
#endif
            default:
                mem_log_write(MON_HEALTHCHECK_BAD_STATE, state_);
                state_ = MON_NODE_DOWN; // something is seriously wrong, bring down the node.
                break;
        }

        setTimeToWakeUp(ts);

        healthCheckLock_.unlock();

        if (terminateMonitor) {
            if (trace_settings & (TRACE_INIT | TRACE_RECOVERY))
            {
                trace_printf("%s@%d - monitor is to be terminated!\n", __FUNCTION__, __LINE__);
            }
        }
    }

    if (trace_settings & TRACE_HEALTH)
        trace_printf("%s@%d health check thread %lx exiting\n", method_name,
                     __LINE__, pthread_self());

    pthread_exit(0);

    TRACE_EXIT;
}

// Sigusr2 is used to block request thread when WDT process is ready 
// to kill all processes, unmount and finally kill the monitor process. 
// Request thread needs to be blocked to prevent sending process death messages
// until the node is completely down.
static void sigusr2SignalHandler(int , siginfo_t *, void *)
{
    const char method_name[] = "CHealthCheck::sigusr2SignalHandler";
    TRACE_ENTRY;

    ReqQueue.enqueuePostQuiesceReq(); // this will block the worker thread.

    if (trace_settings & TRACE_HEALTH)
        trace_printf("%s@%d sigusr2 signal triggered, work queue is now blocked.\n", 
                      method_name, __LINE__);

    if ( ZClientEnabled )
    {
        ZClient->RunningZNodeDelete( MyNode->GetName() );
        ZClient->MasterZNodeDelete( MyNode->GetName() );
    }

    char buf[MON_STRING_BUF_SIZE];
    sprintf(buf, "[CHealthCheck::sigusr2SignalHandler], work queue to now blocked.\n");
    mon_log_write(MON_HEALTHCHECK_Q_BLOCK, SQ_LOG_CRIT, buf);

    TRACE_EXIT;
}

static void *healthCheck(void *arg)
{
    const char method_name[] = "CHealthCheck::healthCheck";
    TRACE_ENTRY;

    // Parameter passed to the thread is an instance of the CHealthCheck object
    CHealthCheck *healthCheck = (CHealthCheck *) arg;

    // Set sigaction such that SIGUSR2 signal is caught.  We use this
    // to detect that WDT process wants us to block req worker thread.
    struct sigaction act;
    act.sa_sigaction = sigusr2SignalHandler;
    act.sa_flags = SA_SIGINFO;
    sigemptyset (&act.sa_mask);
    sigaddset (&act.sa_mask, SIGUSR2);
    sigaction (SIGUSR2, &act, NULL);

    // Mask all allowed signals 
    sigset_t  mask;
    sigfillset(&mask);
    sigdelset(&mask, SIGUSR2);
    int rc = pthread_sigmask(SIG_SETMASK, &mask, NULL);
    if (rc != 0)
    {
        char buf[MON_STRING_BUF_SIZE];
        sprintf(buf, "[%s], pthread_sigmask error=%d\n", method_name, rc);
        mon_log_write(MON_HEALTHCHECK_HC_1, SQ_LOG_ERR, buf);
    }

    // Enter thread processing loop
    healthCheck->healthCheckThread();

    TRACE_EXIT;
    return NULL;
}

void CHealthCheck::start()
{
    const char method_name[] = "CHealthCheck::start";
    TRACE_ENTRY;

    int rc = pthread_create(&thread_id_, NULL, healthCheck, this);
    if (rc != 0)
    {
        char buf[MON_STRING_BUF_SIZE];
        sprintf(buf, "[%s], thread create error=%d\n", method_name, rc);
        mon_log_write(MON_HEALTHCHECK_START_1, SQ_LOG_ERR, buf);
    }

    TRACE_EXIT;
}


void CHealthCheck::shutdownWork(void)
{
    const char method_name[] = "CHealthCheck::shutdownWork";
    TRACE_ENTRY;

    // this will wake up healthCheck thread and make it to exit,
    // which sets terminateMonitor to true
    setState(HC_EXIT);

    if (trace_settings & TRACE_HEALTH)
        trace_printf("%s@%d waiting for health check thread=%lx to exit.\n",
                         method_name, __LINE__, HealthCheck.tid());

    // Wait for healthCheck thread to exit
    pthread_join(HealthCheck.tid(), NULL);

    TRACE_EXIT;
}

// Sets the new state for health check thread to work on.
// Since multiple monitor threads can set the state, check if the state is available before setting one.
// This prevents the race conditions and ensures that HC thread completes working on previous state before
// anyone can set a new one.
void CHealthCheck::setState(HealthCheckStates st, long long param1 /* optional */)
{
    const char method_name[] = "CHealthCheck::setState";
    TRACE_ENTRY;

    bool done = false;

    while (!done)
    {
        healthCheckLock_.lock();
        if (state_ == HC_AVAILABLE) 
        {
            state_ = st;
            param1_ = param1;
            healthCheckLock_.wakeOne();  
            healthCheckLock_.unlock();
            done = true;
        }
        else
        {   // allow health check thread to complete previous event
            healthCheckLock_.unlock();
            usleep(100 * 1000); // wait for 100ms and try again
        }
    }

    TRACE_EXIT; 
}

// Send an event to the SMService process
void CHealthCheck::sendEventToSMService(SMServiceEvent_t event)
{
    const char method_name[] = "CHealthCheck::sendEventToSMService";
    TRACE_ENTRY;

    if ( smserviceProcess_ ) 
    {
        if (trace_settings & (TRACE_INIT | TRACE_RECOVERY | TRACE_REQUEST | TRACE_SYNC))
        {
            trace_printf( "%s@%d - Sending event=%d\n"
                        , method_name, __LINE__, event );
        }
#ifndef NAMESERVER_PROCESS
        smserviceProcess_->GenerateEvent( event, 0, NULL );
#endif
    }

    TRACE_EXIT;
}

// Send an event to the watch dog process
void CHealthCheck::sendEventToWatchDog(WatchdogEvent_t event)
{
    const char method_name[] = "CHealthCheck::sendEventToWatchDog";
    TRACE_ENTRY;

    if ( watchdogProcess_ ) 
    {
        if (trace_settings & (TRACE_INIT | TRACE_RECOVERY | TRACE_REQUEST | TRACE_SYNC))
        {
            trace_printf( "%s@%d - Sending event=%d\n"
                        , method_name, __LINE__, event );
        }
#ifndef NAMESERVER_PROCESS
        watchdogProcess_->GenerateEvent( event, 0, NULL );
#endif
    }

    TRACE_EXIT;
}

// Process any timer pop event such as regular watchdog refresh. 
void CHealthCheck::processTimerEvent()
{
    const char method_name[] = "CHealthCheck::processTimerEvent";
    TRACE_ENTRY;
#if 0
    if (trace_settings & TRACE_HEALTH)
    {
        trace_printf("%s@%d Timer event: wakeup=%lld, current=%ld\n"
                    , method_name, __LINE__, wakeupTimeSaved_, currTime_.tv_sec );
    }
#endif
    // check if request queue is responsive, once every REQ_MAX_RESPONSIVE secs
    if ( (currTime_.tv_sec - lastReqCheckTime_.tv_sec) > CReqQueue::REQ_MAX_RESPONSIVE )
    {
        if ( !ReqQueue.responsive(currTime_) ) 
        {
            if (trace_settings & TRACE_HEALTH)
                trace_printf("%s@%d Request worker thread not responsive.\n", method_name, __LINE__); 

            mem_log_write(MON_HEALTHCHECK_TEVENT_1);

            if (checkReqResponsive_ && !enableMonDebugging_ )
            {
                // schedule a node down request because if the lengthy request completes/aborts, 
                // the worker thread should remain blocked.
                scheduleNodeDown(); 

                // the node down request may not get scheduled to run since the worker thread is blocked.
                // force immediate node down.
                state_ = MON_NODE_DOWN;
            } 
        }
        lastReqCheckTime_ = currTime_;
    }

    // check if sync thread is responsive, once every SYNC_MAX_RESPONSIVE + 1 secs
    if ( (currTime_.tv_sec - lastSyncCheckTime_.tv_sec) > (SYNC_MAX_RESPONSIVE) )
    {
        if ( !Monitor->responsive() )
        {
            if (nonresponsiveTime_.tv_sec == 0)
            {
                nonresponsiveTime_ = currTime_;
            }
            
            if ( (monSyncTimeout_ != -1) && !enableMonDebugging_)
            {
                if ( (currTime_.tv_sec - nonresponsiveTime_.tv_sec) > monSyncTimeout_ )
                {   
                    char buf[MON_STRING_BUF_SIZE];
                    sprintf( buf
                           , "[%s], Sync Thread Timeout detected (timeout=%d). "
                             "Scheduling all nodes down!\n"
                           , method_name, monSyncTimeout_);
                    mon_log_write(MON_HEALTHCHECK_TEVENT_4, SQ_LOG_CRIT, buf);

                    if ( ZClientEnabled )
                    {
                        ZClient->RunningZNodesDelete();
                    }
                    else
                    {
                        CNode  *node = Nodes->GetFirstNode();
                        while (node)
                        {
                            ReqQueue.enqueueDownReq( node->GetPNid() );
                        }
                    }
                    nonresponsiveTime_.tv_sec = 0;
                    nonresponsiveTime_.tv_nsec = 0;
                }
            }
        }
        else
        {
            nonresponsiveTime_.tv_sec = 0;
            nonresponsiveTime_.tv_nsec = 0;
        }

        lastSyncCheckTime_ = currTime_;
    }

    // check if quiescing has timed out, once quiescing has begun.
    if (  (quiesceCountingDown_ && 
          (currTime_.tv_sec - quiesceStartTime_.tv_sec) > quiesceTimeoutSec_)
       ) 
    {
        if (trace_settings & TRACE_HEALTH)
            trace_printf("%s@%d Quiesce timeout, node going down\n", method_name, __LINE__);

        char buf[MON_STRING_BUF_SIZE];
        sprintf(buf, "[%s], Quiesce timeout, node going down\n", method_name);
        mon_log_write(MON_HEALTHCHECK_TEVENT_3, SQ_LOG_WARNING, buf);

        scheduleNodeDown(); // bring the node down
    }

#ifndef NAMESERVER_PROCESS
    // refresh WDT 
    if ( SQ_theLocalIOToClient )
    {
        SQ_theLocalIOToClient->refreshWDT(++refreshCounter_);
#if 0
        if (trace_settings & TRACE_HEALTH)
            trace_printf("%s@%d Watchdog timer refreshed (%d)\n"
                        , method_name, __LINE__, refreshCounter_ );
#endif
    }

    if (currTime_.tv_sec - lastIdtmCheckTime_.tv_sec > idtmCheckInterval_)
    {
      IdtmRefreshCheck();
      DTMRefreshCheck();
      lastIdtmCheckTime_ = currTime_; 
    }
#endif

    TRACE_EXIT;
}

// sets the time when health check thread should wake up on timer pop events.
void CHealthCheck::setTimeToWakeUp( struct timespec &ts)
{
    const char method_name[] = "CHealthCheck::setTimeToWakeUp";
    TRACE_ENTRY;

    clock_gettime(CLOCK_REALTIME, &ts);

    // if this thread has been running for more than 3 seconds since it last woke up, log it.
    if ( ((ts.tv_sec - currTime_.tv_sec) > 3) && (watchdogProcess_) )
    {
       mem_log_write(MON_HEALTHCHECK_WAKEUP_1, ts.tv_sec - currTime_.tv_sec);
    }

    ts.tv_sec += 1; // wake up every second

    wakeupTimeSaved_ = ts.tv_sec;
#if 0
    if (trace_settings & TRACE_HEALTH)
    {
        trace_printf("%s@%d Set Timer event: wakeup=%lld\n"
                    , method_name, __LINE__, wakeupTimeSaved_ );
    }
#endif
    TRACE_EXIT;
}

void CHealthCheck::updateSMServiceProcess()
{
    const char method_name[] = "CHealthCheck::updateSMServiceProcess";
    TRACE_ENTRY;

    smserviceProcess_ = (CProcess *)param1_; 

    TRACE_EXIT;
}

// updates watchdog process object. Client would ask us to set this only after the watchdog process
// is in UP state. param1 has already been populated with the watchdog process object ptr.
void CHealthCheck::updateWatchdogProcess()
{
    const char method_name[] = "CHealthCheck::updateWatchdogProcess";
    TRACE_ENTRY;

    watchdogProcess_ = (CProcess *)param1_; 

    TRACE_EXIT;
}

void CHealthCheck::startQuiesce()
{
    const char method_name[] = "CHealthCheck::startQueisce";
    TRACE_ENTRY;

    ReqQueue.enqueueQuiesceReq();

#ifndef NAMESERVER_PROCESS
    if (MyNode->getNumQuiesceExitPids() > 0) // count down quiesce only if there are pids on exit list.
    {
        clock_gettime(CLOCK_REALTIME, &quiesceStartTime_);
        quiesceCountingDown_ = true;
    
        if (trace_settings & TRACE_HEALTH)
            trace_printf("%s@%d Quiesce Wait Time = %d secs\n", method_name, __LINE__, quiesceTimeoutSec_); 

        if (trace_settings & TRACE_HEALTH)
            trace_printf("%s@%d QuiesceExitPids before wait = %d\n", method_name,
                         __LINE__, MyNode->getNumQuiesceExitPids());
    }

    char buf[MON_STRING_BUF_SIZE];
    sprintf(buf, "[%s], Quiesce req queued. Send pids = %d, Exit pids = %d\n", 
            method_name, MyNode->getNumQuiesceSendPids(), MyNode->getNumQuiesceExitPids());
    mon_log_write(MON_HEALTHCHECK_QUIESCE_1, SQ_LOG_WARNING, buf);
#endif

    TRACE_EXIT;
}

// Schedule a post quiescing work on worker thread to complete the rest of the node down flow.
// It is important to schedule this on worker thread so that it can remain blocked and not process
// any requests (including process exits) while WDT process is killing the processes. 
void CHealthCheck::scheduleNodeDown()
{
    const char method_name[] = "CHealthCheck::scheduleNodeDown";
    TRACE_ENTRY;

    if (!nodeDownScheduled_)
    {
        if (quiesceCountingDown_)
        {
#ifndef NAMESERVER_PROCESS
            if (trace_settings & TRACE_HEALTH)
                trace_printf("%s@%d After wait, QuiesceSendPids = %d, QuiesceExitPids = %d\n", method_name,
                             __LINE__, MyNode->getNumQuiesceSendPids(), MyNode->getNumQuiesceExitPids());
#endif
            quiesceCountingDown_ = false;
        }
        
        ReqQueue.enqueuePostQuiesceReq();
        nodeDownScheduled_ = true;

#ifndef NAMESERVER_PROCESS
        char buf[MON_STRING_BUF_SIZE];
        sprintf(buf, "[%s], Final node down req scheduled. QuiesceSendPids = %d, QuiesceExitPids = %d\n", 
                method_name, MyNode->getNumQuiesceSendPids(), MyNode->getNumQuiesceExitPids());
        mon_log_write(MON_HEALTHCHECK_SCH_1, SQ_LOG_WARNING, buf);
#endif
    }

    TRACE_EXIT; 
}

// Determine if it is time to log a health message and then do it if needed
void CHealthCheck::timeToLogHealth(struct timespec &currentTime)
{
    const char method_name[] = "CHealthCheck::timeToLogHealth";
    TRACE_ENTRY;

    const char message_tag[] = "Health Status";

#if 0
    if (trace_settings & TRACE_HEALTH)
    {
        trace_printf("%s@%d Timer: nextHealthLogTime=%ld, current=%ld\n"
                    , method_name, __LINE__, nextHealthLogTime_.tv_sec, currTime_.tv_sec );
    }
#endif

    if (currentTime.tv_sec >= nextHealthLogTime_.tv_sec)
    { // Time to log health status
        int readyCount = 0;
        int upCount = Nodes->GetPNodesUpCount(readyCount);
    
        char buf[MON_STRING_BUF_SIZE];
        sprintf(buf, "[%s] - Node: %s, pnid=%d, state=%s, master=%s\n"
               , message_tag
               , MyNode->GetName(), MyPNID, StateString(MyNode->GetState()), MasterMonitorName );
        mon_log_write(MON_HEALTHCHECK_TIMETOLOGHEALTH, SQ_LOG_INFO, buf);
        sprintf(buf, "[%s] - Cluster: node count=%d (State Up - count=%d, Txn Services Ready - count=%d)\n"
               , message_tag
               , Nodes->GetPNodesCount(), upCount, readyCount );
        mon_log_write(MON_HEALTHCHECK_TIMETOLOGHEALTH, SQ_LOG_INFO, buf);
        
        nextHealthLogTime_.tv_sec += healthLoggingFrequency_;
    }
    TRACE_EXIT; 
}

void CHealthCheck::triggerTimeToLogHealth( void )
{
    const char method_name[] = "CHealthCheck::triggerTimeToLogHealth";
    TRACE_ENTRY;
    if (trace_settings & (TRACE_INIT | TRACE_RECOVERY | TRACE_HEALTH))
    {
        trace_printf( "%s@%d - Trigger Health Status logging!\n"
                    , method_name, __LINE__);
    }
    clock_gettime(CLOCK_REALTIME, &currTime_);
    nextHealthLogTime_ = currTime_;
    TRACE_EXIT; 
}

void CHealthCheck::IdtmRefreshCheck()
{
    const char method_name[] = "CHealthCheck::IdtmRefreshCheck";
    TRACE_ENTRY;

#ifndef NAMESERVER_PROCESS
    CProcess *idtm = Nodes->GetProcessByName("$TSID0");
    if (idtm && MyNode->IsMyNode(idtm->GetNid()) &&
        idtm->GetState() == State_Up)
    {
      if (SQ_theLocalIOToClient)
      {
        unsigned long latest = SQ_theLocalIOToClient->getLastIdtmRefresh();
        int pid = idtm->GetPid();
        if (latest == lastIdtmRefresh_)
        {
          char buf[MON_STRING_BUF_SIZE];
          sprintf(buf, "Idtm did not refresh, the process may be stopped, lastIdtmRefresh_ = %lu\n",
                       lastIdtmRefresh_);
          mon_log_write(MON_HEALTHCHECK_TIMETOLOGHEALTH, SQ_LOG_INFO, buf);
          if (pid != -1)
            kill(pid, SIGCONT);
        }
        else
        {
          lastIdtmRefresh_ = latest;
          if (trace_settings & (TRACE_INIT | TRACE_RECOVERY | TRACE_REQUEST | TRACE_SYNC))
            trace_printf("%s@%d - Idtm intervally refresh.\n", method_name, __LINE__);
        }
      }
    }
#endif
    TRACE_EXIT;
}

void CHealthCheck::DTMRefreshCheck()
{
    const char method_name[] = "CHealthCheck::DTMRefreshCheck";
    TRACE_ENTRY;

#ifndef NAMESERVER_PROCESS
    CProcess *tm = MyNode->GetProcessByType(ProcessType_DTM);
    if (tm)
    {
      if (SQ_theLocalIOToClient)
      {
        unsigned long latest = SQ_theLocalIOToClient->getLastDTMRefresh();
        int pid = tm->GetPid();
        if (latest == lastDTMRefresh_)
        {
          char buf[MON_STRING_BUF_SIZE];
          sprintf(buf, "TM did not refresh, the process may be stopped, lastDTMRefresh_ = %lu\n",
                       lastDTMRefresh_);
          mon_log_write(MON_HEALTHCHECK_TIMETOLOGHEALTH, SQ_LOG_INFO, buf);
          if (pid != -1)
            kill(pid, SIGCONT);
        }
        else
        {
          lastDTMRefresh_ = latest;
          if (trace_settings & (TRACE_INIT | TRACE_RECOVERY | TRACE_REQUEST | TRACE_SYNC))
            trace_printf("%s@%d - TM intervally refresh.\n", method_name, __LINE__);
        }
      }
    }
#endif
    TRACE_EXIT;
}

#ifndef NAMESERVER_PROCESS
// Determine if it is time to validate the license and then do it if needed
void CHealthCheck::timeToVerifyLicense(struct timespec &currentTime, int myPid)
{
    const char method_name[] = "CHealthCheck::timeToVerifyLicense";
    TRACE_ENTRY;

    myPid = myPid; // Satisfy compiler
    
    // if it time to check the license...
    if (currentTime.tv_sec >= licenseCheckTime_.tv_sec)
    {
       if (licenseFile_)
          delete licenseFile_;
       licenseFile_ = new CLicenseCommon();
        
       if (trace_settings & TRACE_HEALTH)
            trace_printf("%s@%d timeToVerifyLicense : TRUE\n", method_name, __LINE__); 
       if (licenseFile_->getLicenseReady())
       {      
          /* TRKCReplLicense *replL = new CReplLicense(myPid, MyPNID, -1, licenseFile_->getLicense());
          Replicator.addItem(replL);
          */
          licenseExpireTime_.tv_sec = (time_t)((time_t)licenseFile_->getExpireDays()*(time_t)LICENSE_ONE_DAY);
          verifyLicense();
       }
       // TRK Placeholder in case we want to bring this back
     /*  else if (licenseCheckTime_.tv_sec == 0) // first time up, bring it down
       {
         if (trace_settings & TRACE_HEALTH)
            trace_printf("%s@%d timeToVerifyLicense, Invalid or Missing License, shutting down cluster\n", method_name, __LINE__); 
         
          char buf[MON_STRING_BUF_SIZE];
          sprintf(buf, "[%s], CHealthCheck Invalid or Missing License\n", method_name);
          mon_log_write(MON_HEALTHCHECK_LICENSE_INVALID, SQ_LOG_ERR, buf);
            
          CReplShutdown *repl = new CReplShutdown(ShutdownLevel_Abrupt);
          Replicator.addItem(repl);
       }*/
       else
       {
          if (trace_settings & TRACE_HEALTH)
              trace_printf("%s@%d timeToVerifyLicense, Invalid or Missing License\n", method_name, __LINE__);
 
          // Within warning period
          if ((currentTime.tv_sec - licenseFile_->getStartTime()) < licenseFile_->getGracePeriod())
          {
              char buf[MON_STRING_BUF_SIZE];
              sprintf(buf, "[%s], CHealthCheck Invalid, Missing or Temporary License was found.  Attention needed.\n", method_name);
              mon_log_write(MON_HEALTHCHECK_LICENSE_INVALID, SQ_LOG_ERR, buf);
          
              licenseCheckTime_.tv_sec=currTime_.tv_sec + LICENSE_ONE_DAY; 
          }
          // Customer most likely never got a license from Esgyn
          else
          {
              if (trace_settings & TRACE_HEALTH)
                  trace_printf("%s@%d timeToVerifyLicense, Invalid or Missing License, shutting down cluster\n", method_name, __LINE__); 
         
              char buf[MON_STRING_BUF_SIZE];
              sprintf(buf, "[%s], CHealthCheck Invalid or Missing License, , shutting down cluster\n", method_name);
              mon_log_write(MON_HEALTHCHECK_LICENSE_INVALID, SQ_LOG_ERR, buf);
            
              CReplShutdown *repl = new CReplShutdown(ShutdownLevel_Abrupt);
              Replicator.addItem(repl);
          }
       }
    }
    TRACE_EXIT; 
}

void CHealthCheck::setLicenseInfo(int expireTimeInDays, bool success)
{
    const char method_name[] = "CHealthCheck::setLicenseExpireTime";
    TRACE_ENTRY;
    licenseValidateSuccess_ = success;
    
    if (licenseValidateSuccess_)
    {
        // Set Expire Time, but change it into seconds, from Days
        licenseExpireTime_.tv_sec = (time_t)((time_t)expireTimeInDays*(time_t)LICENSE_ONE_DAY);
    }
    // This is the case of a mismatch on one of the nodes
    else
    {
        char buf[MON_STRING_BUF_SIZE];
        sprintf(buf, "[%s], CHealthCheck Invalid License\n", method_name);
        mon_log_write(MON_HEALTHCHECK_LICENSE_INVALID, SQ_LOG_ERR, buf);
    }
    TRACE_EXIT;
}

void CHealthCheck::verifyLicense()
{
    const char method_name[] = "CHealthCheck::verifyLicense";
    TRACE_ENTRY;
    bool stopCluster = false;
    bool licenseExpired = false;
    bool oldVersion = false;
    bool verificationSuccess = true;

    // shouldn't happen, but just in case - read it in again, at this point, it is valid
      /*TRK  if (licenseFile_ == NULL)
    {      
      licenseFile_ = new CLicenseCommon();
    }
    
    // This happens if there is a missing or mismatched license.  We cannot decide what to do since we don't
    // know the validify of a given license.  For R.1 - just warn
     if (!licenseValidateSuccess_)
    {
        if (trace_settings & TRACE_HEALTH)
             trace_printf("%s@%d verifyLicense, Invalid or Missing License\n", method_name, __LINE__); 
         
        char buf[MON_STRING_BUF_SIZE];
        sprintf(buf, "[%s], CHealthCheck Invalid or Missing License\n", method_name);
        mon_log_write(MON_HEALTHCHECK_LICENSE_INVALID, SQ_LOG_ERR, buf);
          
        licenseCheckTime_.tv_sec=currTime_.tv_sec + LICENSE_ONE_DAY;  
        TRACE_EXIT;
        return;
    }
    */
    //only care about seconds    
    clock_gettime(CLOCK_REALTIME, &currTime_);

        // Check version first
    if (licenseFile_->getVersion() < LICENSE_LOWEST_SUPPORTED_VERSION)
    {
        stopCluster = true;
        oldVersion = true;
        
        if (trace_settings & TRACE_HEALTH)
           trace_printf("%s@%d License Version unsupported\n", method_name, __LINE__); 
        
        char buf[MON_STRING_BUF_SIZE];
        sprintf(buf, "[%s], CHealthCheckm License Version unsupported\n", method_name);
        mon_log_write(MON_HEALTHCHECK_LICENSE_NODES, SQ_LOG_ERR, buf); 

        verificationSuccess = false;
    }
    else if (licenseFile_->doesLicenseExpire())
    {
       time_t timeLeft; 
       timeLeft = licenseExpireTime_.tv_sec - currTime_.tv_sec;
      
      // If it is within our window to start warning....
      // If the license is expired....
      
      if (timeLeft < licenseFile_->getSecsToStartWarning())
      {
          if (timeLeft < 0)
          {
	      licenseExpired = true;
              verificationSuccess = false;
	  
              switch (licenseFile_->getType())
              {
                 case TYPE_DEMO :
                 case TYPE_POC :
                 {
                     stopCluster = true;
                    break;
                 }
                case TYPE_PRODUCT:
                default:
                {
                    if (trace_settings & TRACE_HEALTH)
                    trace_printf("%s@%d verifyLicense, License expired\n", method_name, __LINE__); 
        
                    char buf[MON_STRING_BUF_SIZE];
                    sprintf(buf, "[%s], CHealthCheck LICENSE EXPIRED\n", method_name);
                    mon_log_write(MON_HEALTHCHECK_LICENSE_EXPIRE, SQ_LOG_ERR, buf); 
          
                    // Continue warning every day
                    licenseCheckTime_.tv_sec=currTime_.tv_sec + LICENSE_ONE_DAY; 
          
                    struct timespec timePassed;
                    timePassed.tv_sec = currTime_.tv_sec - licenseExpireTime_.tv_sec;
             
                    // if we start to warn 2 months in advance, we will give them a 2 month grace period.
                    // if we start to warn 7 days in advance, we will give them a 7 day grace period, etc...
                    if (timePassed.tv_sec >= licenseFile_->getSecsToStartWarning())
                    {
                      switch (licenseFile_->getAction())
                      {
                        case HC_STOP_INSTANCE:
                        {
                            stopCluster = true;
                            break;
                        }
                        case HC_KEEP_WARNING:
                        default :
                        { // do nothing  
                            break;
                        }
                 
                      }
                    }  
                    break;
                 }
              } //switch
          } //expired
          else
          {
              if (trace_settings & TRACE_HEALTH)
                  trace_printf("%s@%d verifyLicense, License expiring soon\n", method_name, __LINE__); 
        
              char buf[MON_STRING_BUF_SIZE];
              sprintf(buf, "[%s], CHealthCheck LICENSE EXPIRING\n", method_name);
              mon_log_write(MON_HEALTHCHECK_LICENSE_WARN, SQ_LOG_WARNING, buf); 
          }
       }
    }
    
    // Check to make sure the nodes still match up right, no need to check if license is expired
    if ((!licenseExpired) && (!oldVersion) && checkLicenseExceededNodes())
    {  
       if (trace_settings & TRACE_HEALTH)
                trace_printf("%s@%d checkLicenseExceededNodes : Number of Nodes in License Exceeded\n", method_name, __LINE__); 
        
       char buf[MON_STRING_BUF_SIZE];
       sprintf(buf, "[%s], CHealthCheck Number of Nodes in License Exceeded\n", method_name);
       mon_log_write(MON_HEALTHCHECK_LICENSE_NODES, SQ_LOG_ERR, buf); 

       if (nodeFailedTime_.tv_sec == 0)
       {
           nodeFailedTime_.tv_sec = currTime_.tv_sec;
       }

       // If we are within our warning window, just warn
       if ((currTime_.tv_sec - nodeFailedTime_.tv_sec) >= licenseFile_->getSecsToStartWarning())
       {
           switch (licenseFile_->getType())
           {
              case TYPE_DEMO :
              case TYPE_POC :
              {
                   stopCluster = true;
                   break;
               }
               case TYPE_PRODUCT:
               default:
               {
                   switch (licenseFile_->getAction())
                   {
                       case HC_STOP_INSTANCE:
                       {
                           stopCluster = true;
                           break;
                       }
                       case HC_KEEP_WARNING:
                       default :
                       { 
                           break;
                       }        
                   } // getAction
               }
           }  // getType
       }
    }  
   
    // set this before checking stopCluster just in case the cluster doesn't stop - we want
    // this set properly
    licenseCheckTime_.tv_sec=currTime_.tv_sec + LICENSE_ONE_DAY;  
    
    // IF the action to take upon license issue is to stop the cluster, then do so now
    if (stopCluster)
    {            
         char buf[MON_STRING_BUF_SIZE];
         if (licenseExpired)
         {
             sprintf(buf, "[%s],  CHealthCheck shutting down cluster due to expired license\n", method_name);
         }
         else if (oldVersion)
         {
             sprintf(buf, "[%s],  CHealthCheck shutting down cluster due to unsupported license version\n", method_name);
         }
         else 
         {
             sprintf(buf, "[%s],  CHealthCheck shutting down cluster due to invalid number of nodes\n", method_name);
         }
         
         mon_log_write(MON_HEALTHCHECK_LICENSE_SHUT_DOWN, SQ_LOG_ERR, buf); 
         
         CReplShutdown *repl = new CReplShutdown(ShutdownLevel_Abrupt);
          Replicator.addItem(repl);
    }
    // We only want to allow them up upgrade features if the license is valid (expiration, nodes)
    else if (verificationSuccess == true)
    {
        // send system message about feature list. 
        struct message_def *msg;
        msg = new struct message_def;
        msg->type = MsgType_Feature;
        msg->noreply = true;
        msg->u.request.type = ReqType_Notice;
        msg->u.request.u.feature.feature_set = licenseFile_->getFeatures(); 
        MyNode->Bcast(msg);
    }

    
    // Done with the file for now, read it in whenever we need to next.
   if (licenseFile_)
    {
      delete licenseFile_;
      licenseFile_ = NULL;
    }
    
    TRACE_EXIT;
}

bool CHealthCheck::checkLicenseExceededNodes()
{
    const char method_name[] = "CHealthCheck::checkLicenseExceededNodes";
    TRACE_ENTRY;
    bool nodesExceeded = false;

    if (Monitor->GetConfigPNodesCount() > licenseFile_->getNumNodes())
    {
       nodesExceeded = true;
    }
    
    TRACE_EXIT;
    return nodesExceeded;
}

bool CHealthCheck::checkLicenseInternal()
{      
    const char method_name[] = "CHealthCheck::checkLicenseInternal";
    TRACE_ENTRY;
    CLicenseCommon licenseFile;
    bool isInternal = false;

    // If the license is not ready, that means there is an issue - so let's make it fail quick elsewhere by returning false
    if ((!IsRealCluster) || (licenseFile.getLicenseReady() && licenseFile.isInternal()))
    {  
        isInternal = true;
	//Send out the feature list a single time for internal clusters.
	struct message_def *msg;
        msg = new struct message_def;
        msg->type = MsgType_Feature;
        msg->noreply = true;
        msg->u.request.type = ReqType_Notice;
	// Set all the features if nothing is set.  Otherwise, use as is in case someone 
	// is testing feature enablement 

	if (licenseFile.getFeatures() == 0)
	{
	    msg->u.request.u.feature.feature_set = LICENSE_MAX_FEATURES; // 0x00000111
	}
	else
	{
            msg->u.request.u.feature.feature_set = licenseFile.getFeatures(); 
	}
        MyNode->Bcast(msg);
    }
    else 
    {
        isInternal = false;
    }

    TRACE_EXIT;
    return isInternal;
}

short CHealthCheck::getLicenseFeature()
{
    const char method_name[] = "CHealthCheck::getLicenseFeature";
    TRACE_ENTRY;
    short features = 0;
    if (licenseFile_ == NULL)
    {
        licenseFile_ = new CLicenseCommon();  // no need to delete, will happen later
    }
    if (licenseFile_ != NULL)
      {
	//features = licenseFile_->getFeatures();
	if (!IsRealCluster) 
	  {
	    if (licenseFile_->getFeatures() == 0)
	      {
		features = LICENSE_MAX_FEATURES; // 0x00000111
	      }
	    else
	      {
		features = licenseFile_->getFeatures(); 
	      }
	  }
	else
	  {
	    features = licenseFile_->getFeatures(); 
	  }

      }
    
    TRACE_EXIT;
    
    return features;         
}
#endif
