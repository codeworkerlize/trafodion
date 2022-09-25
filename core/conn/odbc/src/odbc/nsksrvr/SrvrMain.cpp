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
********************************************************************/
#include "packet.pb.h"
#include <dlfcn.h>

#include <platform_ndcs.h>
#include <platform_utils.h>
#include "Global.h"
#include "tdm_odbcSrvrMsg.h"
#include "commonFunctions.h"
#include "signal.h"
#include "ODBCMXTraceMsgs.h"
#include <errno.h>
#include "Transport.h"
#include "Listener_srvr.h"
#include "ResStatisticsSession.h"
#include "ResStatisticsStatement.h"

// Version helpers
#include "ndcsversion.h"

#include <sys/socket.h>
#include <netdb.h>
#include <string.h>
#include <ifaddrs.h>


#include <new>
#include <stdio.h>

#include <list>

//#include "CommonLogger.h"
#include "CommonLogger.h"
#include "zookeeper/zookeeper.h"
#include "PubInterface.h"
#include<iostream>
#include<fstream>
#include<string>
#include<queue>
#include <arpa/inet.h>
#include <sys/socket.h>
#include "Mds.h"
#include "ComMemLog.h"

zhandle_t *zh;

clientid_t myid;
pthread_cond_t cond;
pthread_mutex_t smlock;


int shutdownThisThing;

stringstream availSrvrNode;
string dcsRegisteredNode;
string availSrvrData;
string regSrvrData;
char regZnodeName[1024];
char hostname[256];
char instanceId[8];
char childId[8];
char zkHost[1024];
int myNid;
int myPid;
string myProcName;
int sdconn;
char zkRootNode[256];
int clientConnTimeOut;
int zkSessionTimeout;
short stopOnDisconnect;
char trafIPAddr[20];
int aggrInterval;
int statisticsCacheSize;
int queryPubThreshold;
statistics_type statisticsPubType;
bool bStatisticsEnabled;
bool stInternalQueriesInRepo;
long maxHeapPctExit;
long initSessMemSize;
int portMapToSecs = -1;
int portBindToSecs = -1;
bool bPlanEnabled = false;
bool bPublishStatsToTSDB = false;
char tsdurl[256];
int exitSessionsCount = -1;
int exitLiveTime = -1;
int wmsTimeout = -1;
bool buseSSLEnabled = false;
bool keepaliveStatus = false;
int keepaliveIdletime;
int keepaliveIntervaltime;
int keepaliveRetrycount;
bool goingWMSStatus = true;
int shmLimit = 70;
long epoch = -1;
void watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx);
bool verifyPortAvailable(const char * idForPort, int portNumber);
BOOL getInitParamSrvr(int argc, char *argv[], SRVR_INIT_PARAM_Def &initParam, char* strName, char* strValue);
long  getEpoch(zhandle_t *zh);
int mdsEnable = 0;
int sessionDebug = 0;
int preTablesEnable;
int encryptBase64Enable = 0;
int mdsLimitMs = 0;
int cpuLimitPercentWhileStart = 0;
void log4cplusInitialize(int myNid, int myPid);
void registeredNodeWatcher(zhandle_t *zh, int type, int state, const char *path, void* watcherCtx);
bool updateZKState(DCS_SERVER_STATE currState, DCS_SERVER_STATE newState, char *currData = NULL, int currDataLen = 0);
void valueWatcher(zhandle_t *handle, int type, int state, const char *path, void *watcherCtx);
string tmpNode = "/trafodion/1/producer";
void sig_usr(int signo)
{
    if(signo == SIGUSR1)
    {
        MXO_WARN("mxosrvr will exit after next endtran");
        srvrGlobal->exitAfterEndTran = true;
    }
}

//only support positive number
BOOL getNumberTemp( char* strValue, int& nValue )
{
	if( strspn( strValue, "0123456789" ) != strlen( strValue) )
	   return FALSE;
	nValue = atol(strValue);
	return TRUE;
}

void initTcpProcessNameTemp(char *tcpName )
{
	strcpy(tcpName, DEFAULT_TCP_PROCESS);
}

static void free_String_vector(struct String_vector *v)
{
    if (v->data)
    {
        for (int32_t i=0; i < v->count; i++)
        {
            free(v->data[i]);
        }
        free(v->data);
        v->data = NULL;
        v->count = 0;
    }
}

void SQL_EXECDIRECT(SRVR_INIT_PARAM_Def* initParam);
short SQL_EXECDIRECT_FETCH(SRVR_INIT_PARAM_Def* initParam);
int SQL_CANCEL_OPERATION(Int64 transid);

/******************* SLICE Header Fragment *******************/
#include  <stdio.h>

char errStrBuf1[141];
char errStrBuf2[141];
char errStrBuf3[141];
char errStrBuf4[141];
char errStrBuf5[141];

char *sCapsuleName = "ODBC/MX Server";
extern SRVR_GLOBAL_Def *srvrGlobal;
extern ResStatisticsSession    *resStatSession;
extern ResStatisticsStatement  *resStatStatement;

dlHandle hzCmdDll = 0;

char PrimarySystemNm[128];
char PrimaryCatalog[MAX_SQL_IDENTIFIER_LEN+1];
char errorMsg[256];
void* ssn;
char tempSqlString[1500];
const char* tempSqlStringPtr ;
IDL_short tempSqlStmtType;
bool informas = true;
bool sqlflag = false;
int isRebalanced = 0;
int clearSLA = 0;
int toRestart = 0;
int toDisable = 0;
int inDcsStopMode = 0; // 1 means dcsstop command has been executed already,and mxosrvr is in dcsstop mode 
int toRestore = 0;
int  terminateAndClose = 0;
int enterMxoTimestamp[2] = {0,0};
int enterEngineTimestamp[2] = {0,0};
int leaveEngineTimestamp[2] = {0,0};
int leaveMxoTimestamp[2] = {0,0};
char dcsStartTimeFile[1024]={0};
extern "C" void
ImplInit (
	const CEE_handle_def  *objectHandle,
	const char            *initParam,
	long                  initParamLen,
	CEE_status            *returnSts,
	CEE_tag_def           *objTag,
	CEE_handle_def        *implementationHandle);

CEE_status runCEE(char* TcpProcessName, long portNumber, int TransportTrace)
{
	return (GTransport.m_listener->runProgram(TcpProcessName, portNumber, TransportTrace));
}

void mxosrvr_atexit_function(void)
{
  file_mon_process_shutdown();
}
int sqlcount = 0;
int totalruntime = 0;
int totalconnections = 0;
CMdsDeamon Mds;
CEE_handle_def mdsTimerHandle;
void __cdecl MdsTimerExpired(CEE_tag_def pstmt)
{
	Mds.IntervalMsg();
	if (CEE_HANDLE_IS_NIL(&mdsTimerHandle) == IDL_FALSE)
	{
		CEE_TIMER_DESTROY(&mdsTimerHandle);
		CEE_HANDLE_SET_NIL(&mdsTimerHandle);
	}
	CEE_TIMER_CREATE2(30,0,MdsTimerExpired,(CEE_tag_def)NULL,&mdsTimerHandle,srvrGlobal->receiveThrId);

}


int main(int argc, char *argv[], char *envp[])
{
	INITSRVRTRC

	CEE_status				sts = CEE_SUCCESS;
	SRVR_INIT_PARAM_Def		initParam;
	DWORD					processId;
	char					tmpString[128];
	char					tmpString2[32];
	char					tmpString3[512];
	CEECFG_Transport		transport;
	CEECFG_TcpPortNumber	portNumber;
	BOOL					retcode;
	IDL_OBJECT_def			srvrObjRef;
	CEECFG_TcpProcessName	TcpProcessName;
	int						TransportTrace = 0;

	CALL_COMP_DOVERS(ndcs,argc,argv);

try
{

	regZnodeName[0] = '\x0';
	zkHost[0] = '\x0';
	zkRootNode[0] = '\x0';
        tsdurl[0] = '\x0';

	// Initialize seabed
	int	sbResult;
	char buffer[FILENAME_MAX] = {0};
	bzero(buffer, sizeof(buffer));

	sbResult = file_init_attach(&argc, &argv, true, buffer);
	if(sbResult != XZFIL_ERR_OK){
		exit(3);
	}
	msg_debug_hook("mxosrvr", "mxosrvr.hook");
	sbResult = file_mon_process_startup(true);
	if(sbResult != XZFIL_ERR_OK){
		exit(3);
	}
	msg_mon_enable_mon_messages(true);
}
catch(SB_Fatal_Excep sbfe)
{
	exit(3);
}

    

	sigset_t newset, oldset;
	sigemptyset(&newset);
	sigaddset(&newset,SIGQUIT);
	sigaddset(&newset,SIGTERM);
	sigprocmask(SIG_BLOCK,&newset,&oldset);

	processId = GetCurrentProcessId();

	 retcode = getInitParamSrvr(argc, argv, initParam, tmpString, tmpString3);
	retcode = TRUE;

	mxosrvr_init_seabed_trace_dll();
	atexit(mxosrvr_atexit_function);

	// +++ Todo: Duplicating calls here. Should try to persist in srvrGlobal
	MS_Mon_Process_Info_Type  proc_info;
	msg_mon_get_process_info_detail(NULL, &proc_info);
	myNid = proc_info.nid;
	myPid = proc_info.pid;
	myProcName = proc_info.process_name;


/*	char logNameSuffix[32];
        const char *lv_configFileName = "log4cxx.trafodion.sql.config";
        bool singleSqlLogFile = TRUE;
        if (getenv("TRAF_MULTIPLE_SQL_LOG_FILE"))
           singleSqlLogFile = FALSE;
        if (singleSqlLogFile) {
           lv_configFileName = "log4cxx.trafodion.sql.config";
	   CommonLogger::instance().initLog4cxx(lv_configFileName);
        }
        else 
        {
	   sprintf( logNameSuffix, "_%d_%d.log", myNid, myPid );
           lv_configFileName = "log4cxx.trafodion.masterexe.config";
	   CommonLogger::instance().initLog4cxx(lv_configFileName, logNameSuffix);
        }*/

    log4cplusInitialize(myNid, myPid);
   
    if(mdsEnable == 1){
        Mds.DeamonStart();
        Mds.SetTimeLimit(mdsLimitMs);
		sleep(3);
	}

    if(signal(SIGUSR1, sig_usr) == SIG_ERR)
    {
        MXO_WARN("can not catch SIGUSR1");
    }

    int ml_status = ComMemLog::instance().initialize((long)processId, false);
    if (ml_status != 0) {
        MXO_WARN("init memlog failed, errno=%d", ml_status);
    }

    if(retcode == FALSE )
   {
//LCOV_EXCL_START
      SendEventMsg(  MSG_SET_SRVR_CONTEXT_FAILED,
                           EVENTLOG_ERROR_TYPE,
                           processId,
                           ODBCMX_SERVER,
                           srvrObjRef,
                           2,
                           tmpString,
                           tmpString3);
      exit(0);
//LCOV_EXCL_STOP
   }

   GTransport.initialize();
   if(GTransport.error != 0 )
   {
//LCOV_EXCL_START
      SendEventMsg(  MSG_SET_SRVR_CONTEXT_FAILED,
                           EVENTLOG_ERROR_TYPE,
                           processId,
                           ODBCMX_SERVER,
                           srvrObjRef,
                           1,
                           GTransport.error_message);
      exit(0);
//LCOV_EXCL_STOP
   }
   chdir(GTransport.myPathname);

   initParam.srvrType = CORE_SRVR;

//LCOV_EXCL_START
   if (initParam.debugFlag & SRVR_DEBUG_BREAK)
   {
        volatile int done = 0;
        while (!done) {
          sleep(10);
        }
   }
//LCOV_EXCL_STOP

	char zkErrStr[2048];
        stringstream zk_ip_port;
        if( zkHost[0] == '\x0' && regZnodeName[0] == '\x0' )
	{
		sprintf(zkErrStr, "***** Cannot get Zookeeper properties or registered znode info from startup params");
		SendEventMsg(  MSG_SET_SRVR_CONTEXT_FAILED,
						   EVENTLOG_ERROR_TYPE,
						   processId,
						   ODBCMX_SERVER,
						   srvrObjRef,
						   1,
						   zkErrStr);
	}
	else
	{
		zk_ip_port << zkHost;
		sprintf(zkErrStr, "zk_ip_port is: %s", zk_ip_port.str().c_str());
		SendEventMsg(MSG_SERVER_TRACE_INFO, EVENTLOG_INFORMATION_TYPE,
					processId, ODBCMX_SERVER,
					srvrObjRef, 1, zkErrStr);
	}
	if (initParam.debugFlag & SRVR_DEBUG_BREAK)
		zkSessionTimeout = 600;

	zoo_deterministic_conn_order(0); // non-deterministic order for client connections
	zh = zookeeper_init(zk_ip_port.str().c_str(), watcher, zkSessionTimeout * 1000, &myid, 0, 0);
	if (zh == 0){
		sprintf(zkErrStr, "***** zookeeper_init() failed for host:port %s",zk_ip_port.str().c_str());
		SendEventMsg(  MSG_SET_SRVR_CONTEXT_FAILED,
						   EVENTLOG_ERROR_TYPE,
						   processId,
						   ODBCMX_SERVER,
						   srvrObjRef,
						   1,
						   zkErrStr);
	}
    struct timeval now;    
	struct timespec outtime;     
	gettimeofday(&now, NULL);    
	outtime.tv_sec = now.tv_sec + 10;
	outtime.tv_nsec = now.tv_usec * 1000;
	pthread_mutex_lock(&smlock);
	int condRet = pthread_cond_timedwait(&cond, &smlock, &outtime);
//	pthread_cond_wait(&cond,&smlock,);
	pthread_mutex_unlock(&smlock);
	if(condRet == ETIMEDOUT) {
		sprintf(zkErrStr, "***** zookeeper_init() failed for host:port %s tiemout for 10s and exit",zk_ip_port.str().c_str());
		SendEventMsg(  MSG_SET_SRVR_CONTEXT_FAILED,
				EVENTLOG_ERROR_TYPE,
				processId,
				ODBCMX_SERVER,
				srvrObjRef,
				1,
				zkErrStr);
		exit(0);
	}




	bool found = false;
	int rc;
	stringstream ss;
	ss.str("");
	ss << zkRootNode << "/dcs/master";
	string dcsMaster(ss.str());
	Stat stat;
	int startPortNum = 0, portRangeNum;
	char masterHostName[MAX_HOST_NAME_LEN];
	char startPort[12], portRange[12], masterTS[24];
	struct String_vector children;
	children.count = 0;
	children.data = NULL;

	// Get the instance ID from registered node
	char *tkn;
	char tmpStr[256];
	strcpy( tmpStr,  regZnodeName );
	tkn = strtok(tmpStr, ":" );			
	if(tkn!=NULL)
		strcpy(hostname,tkn);
	tkn = strtok(NULL, ":" );
	if( tkn != NULL )
		strcpy( instanceId, tkn );
	tkn = strtok(NULL, ":" );
	if( tkn != NULL )
		strcpy( childId, tkn );
	else
		;	// +++ Todo handle error

	Mds.MxoStartMsg();

	while(!found)
	{
		rc = zoo_exists(zh, dcsMaster.c_str(), 0, &stat);
		if( rc == ZNONODE )
			continue;
		else
		if( rc == ZOK )
		{
			rc = zoo_get_children(zh, dcsMaster.c_str(), 0, &children);
			if( children.count > 0 )
			{
				char zknodeName[2048];
				strcpy(zknodeName, children.data[0]);
				tkn = strtok(zknodeName, ":" );
				if( tkn != NULL )
					strcpy( masterHostName, tkn );

				tkn = strtok(NULL, ":" );
				if( tkn != NULL ) {
					strcpy( startPort, tkn );
					startPortNum = atoi(tkn);
				}

				tkn = strtok(NULL, ":" );
				if( tkn != NULL ) {
					strcpy( portRange, tkn );
					portRangeNum = atoi(tkn);
				}

				tkn = strtok(NULL, ":" );
				if( tkn != NULL )
					strcpy( masterTS, tkn );

				free_String_vector(&children);
				found = true;
			}
			else
				continue;
		}
		else	// error
		{
			sprintf(zkErrStr, "***** zoo_exists() for %s failed with error %d",dcsMaster.c_str(), rc);
			SendEventMsg(  MSG_SET_SRVR_CONTEXT_FAILED,
							   EVENTLOG_ERROR_TYPE,
							   processId,
							   ODBCMX_SERVER,
							   srvrObjRef,
							   1,
							   zkErrStr);
			break;
		}
	}

	// Initialize initparam to defaults
	initParam.transport = CEE_TRANSPORT_TCP;	// -T 3
	initParam.majorVersion = 3; 				// -V 3
	// Will need to remove $ZTC0 and NonStopODBC from below
	sprintf( initParam.asSrvrObjRef, "TCP:$ZTC0/%s:NonStopODBC", startPort);	// -A TCP:$ZTC0/52500:NonStopODBC
	// Will need to remove this after we get rid off all existing AS related processing
	sprintf( initParam.ASProcessName, "$MXOAS" );	// -AS $MXOAS
	// Will need to remove this after we get rid off all existing WMS related processing
	sprintf( initParam.QSProcessName, "$ZWMGR" );	// -QS $ZWMGR

	// moved this here from begining of the function
	BUILD_OBJECTREF(initParam.asSrvrObjRef, srvrObjRef, "NonStopODBC", initParam.portNumber);

	ss.str("");
	ss << zkRootNode << "/dcs/servers/registered";
	string dcsRegistered(ss.str());

	char realpath[1024];
	bool zk_error = false;

 	if( found )
	{
		sprintf(zkErrStr, "Found master node in Zookeeper");
		SendEventMsg(MSG_SERVER_TRACE_INFO, EVENTLOG_INFORMATION_TYPE,
					processId, ODBCMX_SERVER,
					srvrObjRef, 1, zkErrStr);

		found = false;
		while(!found)
		{
			rc = zoo_exists(zh, dcsRegistered.c_str(), 0, &stat);
			if( rc == ZNONODE )
				continue;
			else
			if( rc == ZOK )
			{
				int i;
				//This section is the original port finding mechanism.
				//All servers (the herd) start looking for any available port
				//between starting port number+2 through port range max.
				//This is mainly for backward compatability for DcsServers
				//that don't pass PORTMAPTOSECS and PORTBINDTOSECS param
				if(portMapToSecs == -1 && portBindToSecs == -1) {
					for(i = startPortNum+2; i < startPortNum+portRangeNum; i++) {
						if (GTransport.m_listener->verifyPortAvailable("SRVR", i))
							break;
					}

					if( i == startPortNum+portRangeNum )
					{
                        MXO_WARN("***** No ports free");
						zk_error = true;
						sprintf(zkErrStr, "***** No ports free");
						break;
					}
				} else {
					//This section is for new port map params, PORTMAPTOSECS and PORTBINDTOSECS,
					//passed in by DcsServer. DcsMaster writes the port map to data portion of
					//<username>/dcs/servers/registered znode. Wait PORTMAPTOSECS for port map
					//to appear in registered znode. When it appears read it and scan looking for
					//match of instance and child Id.
					long retryTimeout = 500;//.5 second
					long long timeout = JULIANTIMESTAMP();
					bool isPortsMapped = false;
					char *zkData = new char[1000000];
					int zkDataLen = 1000000;
					while(! isPortsMapped) {
						memset(zkData,0,1000000);
						rc = zoo_get(zh, dcsRegistered.c_str(), false, zkData, &zkDataLen, &stat);
						if( rc == ZOK && zkDataLen > 0 ) {
							sprintf(zkErrStr, "DCS port map = %s", zkData);
							SendEventMsg(MSG_SERVER_TRACE_INFO, EVENTLOG_INFORMATION_TYPE,
									processId, ODBCMX_SERVER,
									srvrObjRef, 1, zkErrStr);

							int myInstanceId = atoi(instanceId);
							int myChildId = atoi(childId);

							sprintf(zkErrStr, "Searching for my id (%d:%d) in port map",myInstanceId,myChildId);
							SendEventMsg(MSG_SERVER_TRACE_INFO, EVENTLOG_INFORMATION_TYPE,
									processId, ODBCMX_SERVER,
									srvrObjRef, 1, zkErrStr);

							char portMapInstanceId[8];
							char portMapChildId[8];
							char portMapPortNum[8];
							char* saveptr;
							char* token = strtok_r (zkData,":",&saveptr);
							while (token != NULL)
							{
								if( token != NULL )//instance Id
									strcpy( portMapInstanceId, token );
								token = strtok_r(NULL, ":",&saveptr);
								if( token != NULL )//child id
									strcpy( portMapChildId, token );
								token = strtok_r(NULL, ":",&saveptr);
								if( token != NULL )//port number
									strcpy( portMapPortNum, token );

								int currPortMapInstanceId = atoi(portMapInstanceId);
								int currPortMapChildId = atoi(portMapChildId);
								int currPortMapPortNum = atoi(portMapPortNum);

								if(myInstanceId == currPortMapInstanceId && myChildId == currPortMapChildId) {
									i = currPortMapPortNum;
									sprintf(zkErrStr, "Found my port number = %d in port map", i);
									SendEventMsg(MSG_SERVER_TRACE_INFO, EVENTLOG_INFORMATION_TYPE,
											processId, ODBCMX_SERVER,
											srvrObjRef, 1, zkErrStr);
									break;
								} else {
									token = strtok_r (NULL, ":",&saveptr);
								}
							}

							timeout = JULIANTIMESTAMP();
							bool isAvailable = false;
							while ( isAvailable == false ) {
								if (GTransport.m_listener->verifyPortAvailable("SRVR", i)) {
									isAvailable = true;
								} else {
									if((JULIANTIMESTAMP() - timeout) > (portBindToSecs * 1000000)) {
										sprintf(zkErrStr, "Port bind timeout...exiting");
										zk_error = true;
										break;
									} else {
										sprintf(zkErrStr, "Port = %d is already in use...retrying", i);
										SendEventMsg(MSG_SERVER_TRACE_INFO, EVENTLOG_INFORMATION_TYPE,
												processId, ODBCMX_SERVER,
												srvrObjRef, 1, zkErrStr);
										DELAY(retryTimeout);
									}
								}
							}

							isPortsMapped = true;

						} else {
							if((JULIANTIMESTAMP() - timeout) > (portMapToSecs * 1000000)) {
								sprintf(zkErrStr, "Port map read timeout...exiting");
								zk_error = true;
								break;
							} else {
								sprintf(zkErrStr, "Waiting for port map");
								SendEventMsg(MSG_SERVER_TRACE_INFO, EVENTLOG_INFORMATION_TYPE,
										processId, ODBCMX_SERVER,
										srvrObjRef, 1, zkErrStr);
								DELAY(retryTimeout);
								rc = zoo_exists(zh, dcsRegistered.c_str(), 0, &stat);
							}
						}
					}

					delete[] zkData;
				}

				initParam.portNumber = i;

				stringstream newpath;
				newpath.str("");
				newpath << dcsRegistered.c_str() << "/" << regZnodeName;
//				dcsRegisteredNode.str("");
//				dcsRegisteredNode << dcsRegistered.c_str() << "/" << regZnodeName;
				dcsRegisteredNode = newpath.str();

				ss.str("");
				ss << myPid;
				string pid(ss.str());

				ss.str("");
				ss << "STARTING"
				   << ":"
				   << JULIANTIMESTAMP()
				   << ":"
				   << ":"				// Dialogue ID
				   << myNid
				   << ":"
				   << myPid
				   << ":"
				   << myProcName.c_str()
				   << ":"			   		// Server IP address
				   << ":"					// Server Port
				   << ":"					// Client computer name
				   << ":"					// Client address
				   << ":"					// Client port
				   << ":"					// Client Appl name
				   << ":";

				regSrvrData = ss.str();

				rc = zoo_create(zh, dcsRegisteredNode.c_str(), regSrvrData.c_str(), regSrvrData.length(), &ZOO_OPEN_ACL_UNSAFE, ZOO_EPHEMERAL, realpath, sizeof(realpath)-1);
				if( rc != ZOK )
				{
					zk_error = true;
					sprintf(zkErrStr, "***** zoo_create() failed with error %d", rc);
					break;
				}
				found = true;
			}
			else	// error
			{
				zk_error = true;
				sprintf(zkErrStr, "***** zoo_exists() for %s failed with error %d",dcsRegistered.c_str(), rc);
				break;
			}
		}
	}

	if( zk_error ) {
		SendEventMsg(  MSG_SET_SRVR_CONTEXT_FAILED,
					   EVENTLOG_ERROR_TYPE,
					   processId,
					   ODBCMX_SERVER,
					   srvrObjRef,
					   1,
					   zkErrStr);
		exit(1);
	}

        // get the current epoch from zookeeper and also put a watch on it
        // (to be even safer, take epoch as a command line arg)
        epoch = getEpoch(zh);

//LCOV_EXCL_START
// when a server dies, the MXOAS sends message to CFG. CFG creates the MXOSRVR process
// and passess only one command line atribute: -SQL CLEANUP OBSOLETE VOLATILE TABLES
// It is for cleanup resources (volatile tables).
// Newly created MXOSRVR process executes CLEANUP OBSOLETE VOLATILE TABLES and exits.
   // (This process is not managed by AS!. It is only a helper.

	if (initParam.sql != NULL)
	{
      if (strncmp(initParam.sql, "SELECT COUNT", 12) == 0)
      {
         //You can specify a completion code with any positive value in a PROCESS_STOP_.
         //Negative completion codes are reserved for HP use.
         //Therefore negative codes will return as 1000 + abs(completionCode)
         short completionCode = -1;
         completionCode = SQL_EXECDIRECT_FETCH(&initParam);

         if (completionCode < 0)
            completionCode = 1000 + abs(completionCode);

#ifdef NSK_PLATFORM
         PROCESS_STOP_(,,,completionCode,,,,);
#else
		 /*
		  * TODO:
		  * need to revisit this logic to return a value via exit code
		  *
		  */
#endif
      }
      else
      {
		  sprintf(tmpString, "Server dies, see previous event."
				  "New temp MXOSRVR is cleaning up obsolete volatile tables and exit.");
		  SendEventMsg(	MSG_ODBC_SERVICE_STOPPED_WITH_INFO,
								  EVENTLOG_INFORMATION_TYPE,
								  processId,
								  ODBCMX_SERVER,
								  srvrObjRef,
								  1,
								  tmpString);
		  SQL_EXECDIRECT(&initParam);
		  exit(0);
	  }
   }
//LCOV_EXCL_STOP

	TransportTrace = initParam.debugFlag & SRVR_TRANSPORT_TRACE;

	switch (initParam.transport)
	{
	case CEE_TRANSPORT_TCP:
		if (initParam.portNumber == 0)
		{
//LCOV_EXCL_START
			sprintf(tmpString, "Invalid Port Number: %ld", initParam.portNumber);
			SendEventMsg(	MSG_PROGRAMMING_ERROR,
										EVENTLOG_ERROR_TYPE,
										processId,
										ODBCMX_SERVER,
										srvrObjRef,
										1,
										tmpString);
			exit(0);
//LCOV_EXCL_STOP
		}
		transport = initParam.transport;
		portNumber = (CEECFG_TcpPortNumber)initParam.portNumber;
		TcpProcessName = initParam.TcpProcessName;
//LCOV_EXCL_START
		if (initParam.eventFlag >= EVENT_INFO_LEVEL2)
		{
			char tmpStringEnv[512];

			sprintf(tmpStringEnv
				   ,"ODBC Server Initial Parameters: "
				    "AS Object Ref(-A) %s, "
					"ASProcessName(-AS) %s, "
					"DebugFlag(-D) %d, "
					"DsId(-DS) %d, "
					"EventFlag(-E) %d, "
					"EmsName(-EMS) %s, "
					"IpPortNumber(-PN) %d, "
					"Transport(-T) %d, "
					"TcpProcessName(-TCP) %s, "
					"MajorVersion(-V) %d"
				   ,initParam.asSrvrObjRef
				   ,initParam.ASProcessName
				   ,initParam.debugFlag
				   ,initParam.DSId
				   ,initParam.eventFlag
				   ,initParam.EmsName
				   ,initParam.portNumber
				   ,initParam.transport
				   ,initParam.TcpProcessName
				   ,initParam.majorVersion
					);

					SendEventMsg(MSG_SRVR_ENV
										, EVENTLOG_INFORMATION_TYPE
										, processId
										, ODBCMX_SERVICE
										, srvrObjRef
										, 1
										, tmpStringEnv);


		}
		break;
	default:
		sts = CEE_BADTRANSPORT;
		sprintf(tmpString, "%ld", sts);
		SendEventMsg(	MSG_KRYPTON_ERROR,
									EVENTLOG_ERROR_TYPE,
									processId,
									ODBCMX_SERVER,
									srvrObjRef,
									1,
									tmpString);
		exit(0);
		break;
	}
//LCOV_EXCL_STOP

	if ((initParam.majorVersion == NT_VERSION_MAJOR_1) ||
		(initParam.majorVersion == NSK_VERSION_MAJOR_1)) // Major Version == 1
	{
		ImplInit (NULL,(const char *)&initParam, sizeof(initParam), &sts, NULL, NULL);

		if (sts != CEE_SUCCESS)
		{
//LCOV_EXCL_START
			sprintf(tmpString, "%ld", sts);
			SendEventMsg(MSG_KRYPTON_ERROR,
									EVENTLOG_ERROR_TYPE,
									processId,
									ODBCMX_SERVER,
									srvrObjRef,
									1,
									tmpString);
			exit(0);
//LCOV_EXCL_STOP
		}
	}

    srvrGlobal->clientKeepaliveStatus = keepaliveStatus;
    srvrGlobal->clientKeepaliveIntervaltime = keepaliveIntervaltime;
    srvrGlobal->clientKeepaliveIdletime = keepaliveIdletime;
    srvrGlobal->clientKeepaliveRetrycount = keepaliveRetrycount;
    srvrGlobal->goingWMSStatus = goingWMSStatus;
    srvrGlobal->shmLimit = shmLimit;
    srvrGlobal->mds = mdsEnable;
    srvrGlobal->loadEnable = preTablesEnable;
    srvrGlobal->encryptBase64Enable = encryptBase64Enable;
    srvrGlobal->cpuLimitPercentWhileStart = cpuLimitPercentWhileStart;

if (mdsEnable == 1)
{
	if (CEE_HANDLE_IS_NIL(&mdsTimerHandle) == IDL_FALSE)
	{
		CEE_TIMER_DESTROY(&mdsTimerHandle);
		CEE_HANDLE_SET_NIL(&mdsTimerHandle);
	}
	CEE_TIMER_CREATE2(30,0,MdsTimerExpired,(CEE_tag_def)NULL,&mdsTimerHandle,srvrGlobal->receiveThrId);
}
    char *trafEnvVar = getenv("TRAF_INSTANCE_ID");
    if (trafEnvVar) {
        srvrGlobal->traf_instance_id = atoi(trafEnvVar);
    }

    trafEnvVar = getenv("TRAF_CLUSTER_ID");
    if (trafEnvVar) {
        srvrGlobal->traf_cluster_id = atoi(trafEnvVar);
    } 

    trafEnvVar = getenv("TRAF_CLUSTER_NAME");
    if (trafEnvVar) {
        snprintf(srvrGlobal->traf_cluster_name,sizeof(srvrGlobal->traf_cluster_name)-1,"%s",trafEnvVar);
    }

    trafEnvVar = getenv("TRAF_INSTANCE_NAME");
    if (trafEnvVar) {
        snprintf (srvrGlobal->traf_instance_name, sizeof(srvrGlobal->traf_instance_name)-1, "%s", trafEnvVar);
    }

    // TCPADD and RZ are required parameters.
	// The address is passed in with TCPADD parameter .
	// The hostName is passed in with RZ parameter.
	if( strlen(trafIPAddr) > 0 && strlen(hostname)>0 )
	{
		strcpy( srvrGlobal->IpAddress, trafIPAddr );
		strcpy( srvrGlobal->HostName, hostname);
        sprintf( errStrBuf1, "Server: IPaddr = %s, hostname = %s",
                 srvrGlobal->IpAddress,srvrGlobal->HostName);
	}
	else
	{
		SendEventMsg(  MSG_SET_SRVR_CONTEXT_FAILED,EVENTLOG_ERROR_TYPE,
                           processId,ODBCMX_SERVER,srvrObjRef,
                           1,"Cannot get TCPADD or RZ information from startup parameters");                           
	    exit(0);
	}	                            
	SendEventMsg(MSG_SRVR_ENV, EVENTLOG_INFORMATION_TYPE,
								  srvrGlobal->nskProcessInfo.processId,
								  ODBCMX_SERVICE, srvrGlobal->srvrObjRef,
								  1, errStrBuf1);
	if( strlen(tsdurl) > 0 )
	{
		strcpy( srvrGlobal->tsdURL, tsdurl );
        }
	if( stopOnDisconnect )
		srvrGlobal->stopTypeFlag = STOP_WHEN_DISCONNECTED;
	found = false;
	while(!found)
	{
		srvrGlobal->portNumber = initParam.portNumber;
		rc = zoo_exists(zh, dcsRegisteredNode.c_str(), 0, &stat);
		if( rc == ZOK )
		{
			ss.str("");
			ss << myNid
			   << ":"
			   << myPid
			   << ":"
			   << myProcName.c_str()
			   << ":"
			   << srvrGlobal->IpAddress
			   << ":"
			   << initParam.portNumber;

			regSrvrData = ss.str();

			ss.str("");
			ss << "STARTING"
			   << ":"
			   << JULIANTIMESTAMP()
			   << ":"					// Dialogue ID
			   << ":"
			   << regSrvrData
			   << ":"					// Client computer name
			   << ":"					// Client address
			   << ":"					// Client port
			   << ":"					// Client Appl name
			   << ":";

			string data(ss.str());

			rc = zoo_set(zh, dcsRegisteredNode.c_str(), data.c_str(), data.length(), -1);
			if( rc != ZOK )
			{
				zk_error = true;
				sprintf(zkErrStr, "***** zoo_set() failed for %s with error %d", dcsRegisteredNode.c_str(), rc);
				break;
			}
            rc = zoo_wexists(zh, dcsRegisteredNode.c_str(), registeredNodeWatcher, NULL, &stat);
			found = true;
		}
		else	// error
		{
			zk_error = true;
			sprintf(zkErrStr, "***** zoo_exists() for %s failed with error %d",dcsRegisteredNode.c_str(), rc);
			break;
		}
	}

	char tmpData[1024] = "";
	int tmpDateLen = sizeof(tmpData) - 1;
	int value = 0;
	Stat tmpStat;

	if ((zoo_wget(zh, tmpNode.c_str(), valueWatcher, NULL, tmpData, &tmpDateLen, &stat)) == ZOK)
	{
		if (tmpDateLen > 0)
			value = atol(tmpData);
		else
			value = 0;
	}
  else
    value = 0;

  srvrGlobal->syncQueryForBinlog = value;

	if( zk_error )
		SendEventMsg(  MSG_SET_SRVR_CONTEXT_FAILED,
					   EVENTLOG_ERROR_TYPE,
					   processId,
					   ODBCMX_SERVER,
					   srvrObjRef,
					   1,
					   zkErrStr);

try
{
	if((sts = runCEE(initParam.TcpProcessName, initParam.portNumber, TransportTrace)) != CEE_SUCCESS )
	{
//LCOV_EXCL_START
		sprintf(tmpString, "%ld", sts);
		SendEventMsg(	MSG_KRYPTON_ERROR,
									EVENTLOG_ERROR_TYPE,
									processId,
									ODBCMX_SERVER,
									srvrObjRef,
									1,
									tmpString);
//LCOV_EXCL_STOP
	}
}
catch (std::bad_alloc)
{
		SendEventMsg(MSG_ODBC_NSK_ERROR, EVENTLOG_ERROR_TYPE,
			0, ODBCMX_SERVER, srvrGlobal->srvrObjRef,
			1, "No more memory is available to allocate, Exiting MXOSRVR server");
}

	if (srvrGlobal->traceLogger != NULL)
	{
		delete srvrGlobal->traceLogger;
		srvrGlobal->traceLogger = NULL;
	}
	if (resStatSession != NULL)
	{
		delete resStatSession;
		resStatSession = NULL;
	}
	if (resStatStatement != NULL)
	{
		delete resStatStatement;
		resStatStatement = NULL;
	}

	exit(0);
}

void logError( short Code, short Severity, short Operation );

void logError( short Code, short Severity, short Operation )
{
	return;
}

extern "C" {
	const char* state2String(int state){
	  if (state == 0)
		return "CLOSED_STATE";
	  if (state == ZOO_CONNECTING_STATE)
		return "CONNECTING_STATE";
	  if (state == ZOO_ASSOCIATING_STATE)
		return "ASSOCIATING_STATE";
	  if (state == ZOO_CONNECTED_STATE)
		return "CONNECTED_STATE";
	  if (state == ZOO_EXPIRED_SESSION_STATE)
		return "EXPIRED_SESSION_STATE";
	  if (state == ZOO_AUTH_FAILED_STATE)
		return "AUTH_FAILED_STATE";

	  return "INVALID_STATE";
	}
	const char* type2String(int state){
	  if (state == ZOO_CREATED_EVENT)
		return "CREATED_EVENT";
	  if (state == ZOO_DELETED_EVENT)
		return "DELETED_EVENT";
	  if (state == ZOO_CHANGED_EVENT)
		return "CHANGED_EVENT";
	  if (state == ZOO_CHILD_EVENT)
		return "CHILD_EVENT";
	  if (state == ZOO_SESSION_EVENT)
		return "SESSION_EVENT";
	  if (state == ZOO_NOTWATCHING_EVENT)
		return "NOTWATCHING_EVENT";

	  return "UNKNOWN_EVENT_TYPE";
	}
}

void watcher(zhandle_t *zzh, int type, int state, const char *path, void *watcherCtx)
{
    /* Be careful using zh here rather than zzh - as this may be mt code
     * the client lib may call the watcher before zookeeper_init returns */
    if (path && strlen(path) > 0) {
    	fprintf(stderr, "Watcher %d state = %s for path %s\n", type, state2String(state), path);
    }
    else
    	fprintf(stderr, "Watcher %d state = %s\n", type, state2String(state));

    if(type == ZOO_SESSION_EVENT){
        if(state == ZOO_CONNECTED_STATE){
            pthread_mutex_lock(&smlock);
            pthread_cond_broadcast(&cond);
            pthread_mutex_unlock(&smlock);
        }
    }

    if (type == ZOO_SESSION_EVENT) {
        if (state == ZOO_CONNECTED_STATE) {
            const clientid_t *id = zoo_client_id(zzh);
            if (myid.client_id == 0 || myid.client_id != id->client_id) {
                myid = *id;
                fprintf(stderr,"Got a new session id: 0x%llx\n", myid.client_id);
             }
        } else if (state == ZOO_AUTH_FAILED_STATE) {
            fprintf(stderr,"Authentication failure. Shutting down...\n");
            zookeeper_close(zzh);
            shutdownThisThing=1;
            zh=0;
        } else if (state == ZOO_EXPIRED_SESSION_STATE) {
            fprintf(stderr,"Session expired. Shutting down...\n");
            zookeeper_close(zzh);
            shutdownThisThing=1;
            zh=0;
        }
    }

    if (type == ZOO_CHANGED_EVENT) {
      string masterNode(zkRootNode);

      masterNode.append("/dcs/master");

      if (masterNode.compare(path) == 0) {
        if (getEpoch(zzh) != epoch) {
          shutdownThisThing=1;
        }
      }
    }
}

void registeredNodeWatcher(zhandle_t *zh, int type, int state, const char *path, void* watcherCtx) {
    char zkData[1024] = {0};
    char saveZkData[1024] = {0};
    char dummyWriteBuffer[100] = "";
    int zkDataLen = sizeof(zkData) - 1;
    Stat stat;
    int retry = 3;
    int rc = zoo_wget(zh, path, registeredNodeWatcher, watcherCtx,zkData,&zkDataLen, &stat);
    if(rc != ZOK)
    {
        fprintf(stderr,"get data and set watcher failed");
    }
    strncpy(saveZkData, zkData, zkDataLen);
    saveZkData[zkDataLen] = '\0';

    if(type == ZOO_CHANGED_EVENT ){
        char *tkn = NULL;
        tkn = strtok(zkData, ":");
        if( tkn !=  NULL &&  !strcmp(tkn, "REBALANCE"))
        {
            isRebalanced = 1;
            clearSLA = 1;
        }

        if( tkn !=  NULL &&  !strcmp(tkn, "RESTART")){
            toRestart = 1;
            terminateAndClose =1;
        }

        if( tkn !=  NULL &&  !strcmp(tkn, "DISABLE")){
            toDisable = 1;
            strcpy(dummyWriteBuffer, "select loop next");
            GTransport.m_listener->writeDummy(dummyWriteBuffer, strlen(dummyWriteBuffer));
        }

        if( tkn !=  NULL &&  strlen(tkn) >= 6 &&
            !strncmp(tkn, "CANCEL", 6)) {
            //_exit(0);
            char *p = strtok(tkn, ",");
            char *qid = NULL;
            if (p)
             qid = strtok(NULL, ",");
            if (qid)
            {
              int len1 = strlen(qid), len2 = 0;
              char* currQid = NULL;
              if (pSrvrSession && pSrvrSession->pCurrentSrvrStmt)
              {
                len2 = pSrvrSession->pCurrentSrvrStmt->sqlUniqueQueryIDLen;
                currQid = pSrvrSession->pCurrentSrvrStmt->sqlUniqueQueryID;
              }
              if (len1 != 0 && len1 == len2 &&
                  strcmp(qid, currQid) == 0)
              {
                Int64 transid = -1;
                SQL_EXEC_GetTransactionId(&transid); 
                if (transid != -1)
                  SQL_CANCEL_OPERATION(transid);
                else
                  MXO_WARN("TranId is -1 for transaction cancel, do nothing. Qid from zk : %s",
                           qid);
              }
              else
                MXO_WARN("Unmatched query id for transaction cancel. Qid from zk : %s, current qid = %s",
                         qid,  currQid != NULL ? currQid : "NULL");
            }
            if(!updateZKState(CANCEL_OPERATION, CONNECTED, saveZkData, zkDataLen) ){
                MXO_WARN("update zookeeper for cancel query failed");
            }
        }

        if( tkn !=  NULL &&  !strcmp(tkn, "CONNECTED")){
            toRestore ++;
            toRestart = 0;
            terminateAndClose =0;
            isRebalanced = 0;
            clearSLA = 0;
            toDisable = 0;
        }

        if( tkn !=  NULL &&  !strcmp(tkn, "EXIT")){
            // current state is availible  we can exit mxosrvr
            _exit(0);
        }
    }
}


bool verifyPortAvailable(const char * idForPort,
                         int portNumber)
{
    int fdesc;
    int result;
	sockaddr_in6	portcheckaddr;
	char sTmp1[80];
	char sTmp2[80];

	if ((fdesc = socket(AF_INET6, SOCK_STREAM, 0)) < 0 )
		fdesc = socket(AF_INET, SOCK_STREAM, 0);

//LCOV_EXCL_START
    if (fdesc < 0) {
        return false;
    }
//LCOV_EXCL_STOP

    bzero((char*)&portcheckaddr, sizeof(portcheckaddr));
    portcheckaddr.sin6_family = AF_INET6;
    portcheckaddr.sin6_addr = in6addr_any;
    portcheckaddr.sin6_port = htons((uint16_t)portNumber);

    result = bind(fdesc, (struct sockaddr *)&portcheckaddr, sizeof(portcheckaddr));
//LCOV_EXCL_START
/*
    if (result < 0)
    {
		sprintf( sTmp1, "Cannot use %s port (%s).", idForPort,
                 strerror(errno));
		sprintf( sTmp2, "%d", portNumber );

		SendEventMsg(MSG_SET_SRVR_CONTEXT_FAILED,
                                    EVENTLOG_ERROR_TYPE,
                                    GetCurrentProcessId(),
                                    ODBCMX_SERVICE,
                                    tcpProcessName, 2,
                                    sTmp1, sTmp2);

        close(fdesc);
        return false;
    }
//LCOV_EXCL_STOP
    else
    {
        close(fdesc);
        return true;
    }
*/
    close(fdesc);
    return (result < 0) ? false : true;
}

BOOL getInitParamSrvr(int argc, char *argv[], SRVR_INIT_PARAM_Def &initParam, char* strName, char* strValue)
{
	int		count;
	char*	arg = NULL;
	int		number;
	BOOL	retcode;
	BOOL	argEmpty = FALSE;
	BOOL	argWrong = FALSE;

	initParam.debugFlag			= 0;
	initParam.eventFlag			= 0;
	initParam.asSrvrObjRef[0]	= '\0';
	initParam.srvrType			= SRVR_UNKNOWN;
	initParam.startType			= NON_INTERACTIVE;
	initParam.noOfServers		= DEFAULT_NUMBER_OF_SERVERS;
	initParam.cfgSrvrTimeout	= DEFAULT_CFGSRVR_TIMEOUT;
	initParam.DSId				= PUBLIC_DSID;
	initParam.transport			= CEE_TRANSPORT_TCP;
	initParam.portNumber		= DEFAULT_PORT_NUMBER;
	initParam.majorVersion		= DEFAULT_MAJOR_VERSION;
	initParam.IpPortRange		= DEFAULT_PORT_RANGE;
	initParam.DSName[0]			= '\0';
	initParam.tcp_address		= IP_ADDRESS;
	initParam.minSeg			= DEFAULT_MINIMUM_SEGMENTS;
	initParam.initSegTimeout	= DEFAULT_INIT_SEG_TIMEOUT;
	count = 1;

	stopOnDisconnect = 0;
	clientConnTimeOut = 60;	// Default to 60 secs
	zkSessionTimeout = 30;	// Default to 30 secs
	strName[0] = 0;
	strValue[0] = 0;
	maxHeapPctExit = 0;
	initSessMemSize = 0;
	initParam.timeLogger=false;
	initTcpProcessNameTemp(initParam.TcpProcessName);
	initParam.ASProcessName[0]	= '\0';
	strcpy( initParam.EmsName, DEFAULT_EMS );
	strcpy(initParam.QSProcessName, DEFAULT_QS_PROCESS_NAME);
	strcpy(initParam.QSsyncProcessName, DEFAULT_SYNC_PROCESS_NAME);
	memset(trafIPAddr,0,sizeof(trafIPAddr));
	memset(hostname,0,sizeof(hostname));
	aggrInterval = 60;
        statisticsCacheSize = 0;
	queryPubThreshold = 60;
	statisticsPubType = STATISTICS_AGGREGATED;
	bStatisticsEnabled = false;
	stInternalQueriesInRepo = false;
	bPublishStatsToTSDB = false;
	memset(tsdurl,0,sizeof(tsdurl));

	initParam.EmsTimeout		= DEFAULT_EMS_TIMEOUT;
	initParam.initIncSrvr		= DEFAULT_INIT_SRVR;
	initParam.initIncTime		= DEFAULT_INIT_TIME;
	initParam.DSG =DEFAULT_DSG;
	initParam.srvrTrace = false;
	initParam.TraceCollector[0]	= '\0';
	initParam.RSCollector[0]	= '\0';
	initParam.sql = NULL;
	initParam.mute = false;//Dashboard testing - no 21036 message
	initParam.ext_21036 = true; // new extended 21036 msg - for SRPQ
        initParam.floatIP = false;  // bool which indicates if the script which binds the floating ip address needs to be run

	memset(initParam.neoODBC,0,sizeof(initParam.neoODBC));

        while ( count < argc)
	{
		if( (arg = (char*)realloc( arg, strlen( argv[count]) + 1) ) == NULL)
		{
			strcpy( strName, "realloc");
			sprintf( strValue, "%d",strlen( argv[count]) + 1);
			return FALSE;
		}

		strcpy(arg, argv[count]);

		strupr(arg);

		if (strcmp(arg, "-D") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				if( getNumberTemp( argv[count], number ) == TRUE )
					initParam.debugFlag = number;
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-RZ") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				if (strlen(argv[count]) < sizeof(regZnodeName)-1)
				{
					strcpy(regZnodeName, argv[count]);
				}
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-ZKHOST") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				if (strlen(argv[count]) < sizeof(zkHost)-1)
				{
					strcpy(zkHost, argv[count]);
				}
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-ZKPNODE") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				if (strlen(argv[count]) < sizeof(zkRootNode)-1)
				{
					strcpy(zkRootNode, argv[count]);
				}
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-CNGTO") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				if( getNumberTemp( argv[count], number ) == TRUE )
					clientConnTimeOut = number;
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-ZKSTO") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				if( getNumberTemp( argv[count], number ) == TRUE )
					zkSessionTimeout = number;
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-EADSCO") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				if( getNumberTemp( argv[count], number ) == TRUE )
					stopOnDisconnect = number;
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-TCPADD") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				if (strlen(argv[count]) < sizeof(trafIPAddr)-1)
				{
					strcpy(trafIPAddr, argv[count]);
				}
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-MAXHEAPPCT") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				if( getNumberTemp( argv[count], number ) == TRUE )
				{
					if(number > 0)
						maxHeapPctExit = number;
				}
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-STATISTICSINTERVAL") == 0)
		{
			if (++count < argc )
			{			    
				//support positive & minus
				number=atoi(argv[count]);				
                if(number >= MIN_INTERVAL)
					aggrInterval = number;				
			}	
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-STATISTICSCACHESIZE") == 0)
		{
			if (++count < argc )
			{
                            if(strspn(argv[count], "0123456789")==strlen(argv[count])){
                                number=atoi(argv[count]);
                                if(number > 0)
                                    statisticsCacheSize = number;
                            }
                            else
                            {
                                argWrong = TRUE;
                            }
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
               else
		if (strcmp(arg, "-STATISTICSLIMIT") == 0)
		{
			if (++count < argc)
			{
				number=atoi(argv[count]);				
                if(!(number >0 && number<MIN_INTERVAL))
					queryPubThreshold = number;				
			}	
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-STATISTICSTYPE") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				char statisticsOpt[20];
				if (strlen(argv[count]) < sizeof(statisticsOpt)-1)
				{
					memset(statisticsOpt,0,sizeof(statisticsOpt));
					strcpy(statisticsOpt, argv[count]);
					if (stricmp(statisticsOpt, "session") == 0)
						statisticsPubType = STATISTICS_SESSION;						
					else
						statisticsPubType = STATISTICS_AGGREGATED;
				}
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-STATISTICSENABLE") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				char statisticsEnable[20];
				if (strlen(argv[count]) < sizeof(statisticsEnable)-1)
				{
					memset(statisticsEnable,0,sizeof(statisticsEnable));
					strcpy(statisticsEnable, argv[count]);
					if (stricmp(statisticsEnable, "false") == 0)
						bStatisticsEnabled = false;
					else
						bStatisticsEnabled = true;
				}
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}

		}
		else
		if (strcmp(arg, "-STOREINTERNALQUERIESINREPO") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				char storeIntQueriesInRepo[20];
				if (strlen(argv[count]) < sizeof(storeIntQueriesInRepo)-1)
				{
					memset(storeIntQueriesInRepo,0,sizeof(storeIntQueriesInRepo));
					strcpy(storeIntQueriesInRepo, argv[count]);
					if (stricmp(storeIntQueriesInRepo, "false") == 0)
						stInternalQueriesInRepo = false;
					else
						stInternalQueriesInRepo = true;
				}
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}

		}
		else
		if (strcmp(arg, "-PORTMAPTOSECS") == 0)
		{
			if (++count < argc )
			{
				portMapToSecs = atoi(argv[count]);;				
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-PORTBINDTOSECS") == 0)
		{
			if (++count < argc )
			{
				portBindToSecs = atoi(argv[count]);				
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
		else
		if (strcmp(arg, "-SQLPLAN") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				char planEnable[20];
				if (strlen(argv[count]) < sizeof(planEnable)-1)
				{
					memset(planEnable,0,sizeof(planEnable));
					strcpy(planEnable, argv[count]);
					if (stricmp(planEnable, "false") == 0)
						bPlanEnabled = false;
					else
						bPlanEnabled = true;
				}
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
                else
		if (strcmp(arg, "-PUBLISHSTATSTOTSDB") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				char publishStatsToOpenTSDB[20];
				if (strlen(argv[count]) < sizeof(publishStatsToOpenTSDB)-1)
				{
					memset(publishStatsToOpenTSDB,0,sizeof(publishStatsToOpenTSDB));
					strcpy(publishStatsToOpenTSDB, argv[count]);
					if (stricmp(publishStatsToOpenTSDB, "false") == 0)
						bPublishStatsToTSDB = false;
					else
						bPublishStatsToTSDB = true;
				}
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}

		}
                else
		if (strcmp(arg, "-OPENTSDURL") == 0)
		{
			if (++count < argc && argv[count][0] != '-' )
			{
				if (strlen(argv[count]) < sizeof(tsdurl)-1)
				{
					strcpy(tsdurl, argv[count]);
				}
				else
				{
					argWrong = TRUE;
					break;
				}
			}
			else
			{
				argEmpty = TRUE;
				break;
			}
		}
                else
                if (strcmp(arg, "-EXITSESSIONSCOUNT") == 0)
                {
                        if (++count < argc )
                        {
                                exitSessionsCount = atoi(argv[count]);                             
                        }
                        else
                        {
                                argEmpty = TRUE;
                                break;
                        }
                }
                else
                if (strcmp(arg, "-WMSTIMEOUT") == 0)
                {
                        if (++count < argc )
                        {
                                wmsTimeout = atoi(argv[count]);
                        }
                        else
                        {
                                argEmpty = TRUE;
                                break;
                        }
                }else
                if (strcmp(arg, "-USESSLENABLE") == 0)
                {
                    if (++count < argc && argv[count][0] != '-')
                    {
                        char useSSLEnabled[20]={0};
                        if (strlen(argv[count]) < sizeof(useSSLEnabled) - 1)
                        {
                            strncpy(useSSLEnabled, argv[count],20);
                            if (strcmp(useSSLEnabled, "false") == 0)
                                buseSSLEnabled = false;
                            else
                                buseSSLEnabled = true;
                        }
                        else
                        {
                            argWrong = TRUE;
                        }
                    }
                    else
                    {
                        argEmpty = TRUE;
                        break;
                    }
		}else
        if (strcmp(arg, "-TCPKEEPALIVESTATUS") == 0){
            if (++count < argc && argv[count][0] != '-')
            {
                char keepaliveEnable[20];
                if (strlen(argv[count]) < sizeof(keepaliveEnable) - 1)
                {
                    memset(keepaliveEnable, 0, sizeof(keepaliveEnable) - 1);
                    strncpy(keepaliveEnable, argv[count], sizeof(keepaliveEnable) - 1);
                    if(stricmp(keepaliveEnable, "true") == 0)
                        keepaliveStatus = true;
                    else
                        keepaliveStatus = false;
                }
                else
                {
                    argWrong = TRUE;
                }
            }
            else
            {
                argEmpty = TRUE;
                break;
            }
        }else
        if (strcmp(arg, "-TCPKEEPALIVEIDLETIME") == 0){
            if (++count < argc )
            {
                if(strspn(argv[count], "0123456789")==strlen(argv[count])){
                    keepaliveIdletime = atoi(argv[count]);
                }else
                {
                    argWrong = TRUE;
                }
			}
            else
            {
                argEmpty = TRUE;
                break;
            }
        }else
        if (strcmp(arg, "-TCPKEEPALIVEINTERVAL") == 0){
            if (++count < argc )
            {
                if(strspn(argv[count], "0123456789")==strlen(argv[count])){
                    keepaliveIntervaltime = atoi(argv[count]);
                }else
                {
                    argWrong = TRUE;
                }
            }
            else
            {
                argEmpty = TRUE;
                break;
            }
        }else
        if (strcmp(arg, "-TCPKEEPALIVERETRYCOUNT") == 0){
            if (++count < argc )
            {
                if(strspn(argv[count], "0123456789")==strlen(argv[count])){
                    keepaliveRetrycount = atoi(argv[count]);
                }else
                {
                    argWrong = TRUE;
                }
            }
            else
            {
                argEmpty = TRUE;
                break;
            }
        }else
        if (strcmp(arg, "-IFGOINGWMS") == 0){
            if (++count < argc && argv[count][0] != '-')
            {
                char goingWMSEnable[20];
                if (strlen(argv[count]) < sizeof(goingWMSEnable) - 1 )
                {
                    memset(goingWMSEnable, 0, sizeof(goingWMSEnable) - 1);
                    strncpy(goingWMSEnable, argv[count], sizeof(goingWMSEnable) - 1);
                    if(stricmp(goingWMSEnable, "true") == 0)
                        goingWMSStatus = true;
                    else
                        goingWMSStatus = false;
                }
                else
                {
                    argWrong = TRUE;
                }
            }
            else
            {
                argEmpty = TRUE;
                break;
            }
        }else
        if (strcmp(arg, "-MDSENABLE") == 0) {
            if (++count < argc && argv[count][0] != '-' )
            {
                char mdsEnableStr[20];
                if (strlen(argv[count]) < sizeof(mdsEnableStr)-1)
                {
                    memset(mdsEnableStr,0,sizeof(mdsEnableStr));
                    strcpy(mdsEnableStr, argv[count]);
                    if (stricmp(mdsEnableStr, "false") == 0)
                        mdsEnable = 0;
                    else
                        mdsEnable = 1;
                }
                else
                {
                    argWrong = TRUE;
                    break;
                }
            }
            else
            {
                argEmpty = TRUE;
                break;
            }
	    }
	    else
        if (strcmp(arg, "-PRELOADENABLE") == 0) { 
            int tempValue = 0;
            if (++count < argc && argv[count][0] != '-' )
            {    
                char preloadEnableStr[20];
                if (strlen(argv[count]) < sizeof(preloadEnableStr)-1)
                {    
                    memset(preloadEnableStr,0,sizeof(preloadEnableStr));
                    strcpy(preloadEnableStr, argv[count]);
                    if ((tempValue = atol(preloadEnableStr)) > 0)
                        preTablesEnable = tempValue; 
                    else 
                        preTablesEnable = 0; 
                }    
                else 
                {    
                    argWrong = TRUE;
                    break;
                }    
            }    
            else 
            {    
                argEmpty = TRUE;
                break;
            }    
	    }
        else
        if (strcmp(arg, "-ENCRYPTBASE64ENABLE") == 0) {
            int tempValue = 0;
            if (++count < argc && argv[count][0] != '-' )
            {
                char preloadEnableStr[20];
                if (strlen(argv[count]) < sizeof(preloadEnableStr)-1)
                {
                    memset(preloadEnableStr,0,sizeof(preloadEnableStr));
                    strcpy(preloadEnableStr, argv[count]);
                    if ((tempValue = atol(preloadEnableStr)) > 0)
                        encryptBase64Enable = tempValue;
                    else
                        encryptBase64Enable = 0;
                }
                else
                {
                    argWrong = TRUE;
                    break;
                }
            }
            else
            {
                argEmpty = TRUE;
                break;
            }
	    }	 
        else 
			if (strcmp(arg, "-MDSLIMITMS") == 0) {
				if (++count < argc && argv[count][0] != '-' )
				{
                    if(strspn(argv[count], "0123456789") == strlen(argv[count]))
                    {
                        mdsLimitMs = atoi(argv[count]);
                    }
                    else
                    {
                        argWrong = TRUE;
                    }
 
				}
				else
				{
					argEmpty = TRUE;
					break;
				}

			}else 
        if (strcmp(arg, "-RMSMEMLIMIT") == 0) {
                if (++count < argc)
                {
                    if(strspn(argv[count], "0123456789") == strlen(argv[count]))
                    {
                        shmLimit = atoi(argv[count]);
                    }
                    else
                    {
                        argWrong = TRUE;
                    }
                }
                else
                {
                    argEmpty = TRUE;
                    break;
                }
        }
        else if (strcmp(arg, "-CPULIMITPERWHILESTART") == 0) {
            if (++count < argc)
            {
                if(strspn(argv[count], "0123456789") == strlen(argv[count]))
                {
                    cpuLimitPercentWhileStart = atoi(argv[count]);
                }
                else
                {
                    argWrong = TRUE;
                }
            }
            else
            {
                argEmpty = TRUE;
                break;
            }
        }

		count++;
    }
}

void log4cplusInitialize(int myNid, int myPid)
{
    char logNameSuffix[32];
    const char *lv_configFileName = "log4cplus.trafodion.mxosrvr.config";
    bool singleSqlLogFile = true;
    const char *mxosrvr_config_env = getenv("EACH_MXOSRVR_LOG_FILE_CONFIG");
    if (strcmp(mxosrvr_config_env, "true") == 0)
        singleSqlLogFile = false;
    if (singleSqlLogFile) {
        lv_configFileName = "log4cplus.trafodion.mxosrvr.config";
        CommonLogger::instance().initLog4cplus(lv_configFileName);
    }
    else 
    {   
        sprintf( logNameSuffix, "_%d_%d.log", myNid, myPid );
        lv_configFileName = "log4cplus.trafodion.each.mxosrvr.config";
        CommonLogger::instance().initLog4cplus(lv_configFileName, logNameSuffix);
    }   
}

// The "epoch" is a time period between configuration changes in the
// system. When such a configuration change happens (e.g. the
// executable of the mxosrvr is replaced, or a system default is being
// changed), we want to stop all existing mxosrvrs once they become
// idle and replace them with new ones. Therefore, keep a watch on
// this value and exit when it changes and when our state is or
// becomes idle.
long  getEpoch(zhandle_t *zh) {
  char path[2000];
  char zkData[1000];
  int zkDataLen = sizeof(zkData);
  int result = -1;

  snprintf(path, sizeof(path), "%s/dcs/master", zkRootNode);
  int rc = zoo_get(zh, path, 1, zkData, &zkDataLen, NULL);

  if (rc == ZOK && zkDataLen > 0)
    result = atol(zkData);

  return result;
}

void valueWatcher(zhandle_t *handle, int type, int state, const char *path, void *watcherCtx)
{
#ifdef __aarch64__
    pthread_mutex_lock(&smlock);
    if(!zh){
        pthread_mutex_unlock(&smlock);
        return;
    }
#endif
    char zkData[1024] = {0};
    char saveZkData[1024] = {0};
    int zkDataLen = sizeof(zkData) - 1;
	int value = 0;
    Stat stat;
    if(type == ZOO_CREATED_EVENT || type == ZOO_CHANGED_EVENT){
    	if (zoo_wget(handle, path, valueWatcher, watcherCtx, zkData, &zkDataLen, &stat) == ZOK)
		{
			if (zkDataLen > 0)
				value = atol(zkData);
			srvrGlobal->syncQueryForBinlog = value;
		}
    }else if (type == ZOO_DELETED_EVENT){
    	zoo_wexists(zh, tmpNode.c_str(), valueWatcher, NULL, &stat);
	MXO_WARN("valueWatcher delete with value=%d", value);
    }

#ifdef __aarch64__
    pthread_mutex_unlock(&smlock);
#endif
}
