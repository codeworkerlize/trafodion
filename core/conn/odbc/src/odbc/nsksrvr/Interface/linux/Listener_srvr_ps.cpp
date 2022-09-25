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


#include <platform_ndcs.h>
#include "errno.h"
#include "Transport.h"
#include "Listener_srvr.h"
#include "TCPIPSystemSrvr.h"
#include "FileSystemSrvr.h"
#include "CommonLogger.h"
#include "Global.h"
#include "SrvrConnect.h"
#include <arpa/inet.h>
#include <sys/socket.h>
#include <netdb.h>
#include <ifaddrs.h>
#include <time.h>
#include "seabed/sys.h"
#include <dirent.h>

extern int availableFlag;
extern int terminateAndClose;
using namespace std;
extern char dcsStartTimeFile[1024];
extern bool checkNodeEnable();
extern bool updateZKState(DCS_SERVER_STATE currState, DCS_SERVER_STATE newState, char *currData = NULL, int currDataLen = 0);
extern SRVR_GLOBAL_Def *srvrGlobal;
extern bool gv_ms_shutdown_called;

extern void SyncPublicationThread();

void CNSKListenerSrvr::closeTCPIPSession(int fnum)
{
	CTCPIPSystemSrvr* pnode=GTransport.m_TCPIPSystemSrvr_list->find_node(fnum);
	if(pnode && buseSSLEnabled){
			SSL_shutdown(pnode->openssl);
			BIO_free_all(pnode->bio);
    		BIO_free_all(pnode->accept_bio);
	}
	shutdown(fnum, SHUT_RDWR);
	close(fnum);
	FD_CLR(fnum, &read_fds_);
	FD_CLR(fnum, &error_fds_);
//	if (fnum == max_read_fd_) max_read_fd_--;
//	max_read_fd_ =  m_nListenSocketFnum;
	max_read_fd_ =  pipefd[0]; // m_nListenSocketFnum;
}

bool CNSKListenerSrvr::ListenToPort(int port)
{
	char tmp[500];
    int error;
    struct sockaddr_in6 *sin6 = NULL;
    struct sockaddr_in  *sin4 = NULL;
    max_read_fd_ = 0;

	if (m_nListenSocketFnum < 1)
	{
		sprintf(tmp,"ListenToPort[%d][%d]", port, m_nListenSocketFnum );
		SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, errorType_, tmp, O_INIT_PROCESS, F_SOCKET, 0, 0);

		if (m_bIPv4 == false)
		{
//LCOV_EXCL_START
			if ((m_nListenSocketFnum = socket(AF_INET6, SOCK_STREAM, 0)) < 0 )
			{
				m_bIPv4 = true;
				m_nListenSocketFnum = socket(AF_INET, SOCK_STREAM, 0);
			}
//LCOV_EXCL_STOP
		}
		else
			m_nListenSocketFnum = socket(AF_INET, SOCK_STREAM, 0);

		if (m_nListenSocketFnum < 0)
		{
//LCOV_EXCL_START
			SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, errorType_,
                      "ListenToPort", O_INIT_PROCESS, F_SOCKET, errno, 0);
			goto bailout;
//LCOV_EXCL_STOP
		}

        if(strncmp(m_TcpProcessName,"$ZTC0",5) != 0)
        {
//LCOV_EXCL_START
		   /*
			* bind to a specific interface (m_TcpProcessName is initialized by default to $ztc0)
			*/
           struct ifaddrs *ifa = NULL, *ifp = NULL;
		   bool bFoundInterface = false;

           if (getifaddrs (&ifp) < 0)
           {
              SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, errorType_, "ListenToPort - getifaddrs", O_INIT_PROCESS, F_SOCKET, errno, 0);
              goto bailout;
           }

           for (ifa = ifp; ifa != NULL; ifa = ifa->ifa_next)
           {
              if(! ifa->ifa_addr)
                 continue;

              if( (m_bIPv4 == true  && ifa->ifa_addr->sa_family != AF_INET) ||
                  (m_bIPv4 == false && ifa->ifa_addr->sa_family != AF_INET6) ||
                  (strcmp(ifa->ifa_name,m_TcpProcessName) != 0) )
                 continue;

		      bFoundInterface = true;

              if(m_bIPv4 == false)
              {
                 sin6 = (struct sockaddr_in6*)ifa->ifa_addr;
                 memcpy(&m_ListenSocketAddr6,sin6,sizeof(m_ListenSocketAddr6));
                 m_ListenSocketAddr6.sin6_port = htons((uint16_t) port);
				 break;
              }
              else
              {
                 sin4 = (struct sockaddr_in*)ifa->ifa_addr;
                 memcpy(&m_ListenSocketAddr,sin4,sizeof(m_ListenSocketAddr));
                 m_ListenSocketAddr.sin_port = htons((in_port_t) port);
				 break;
              }
           } // for all interfaces

           freeifaddrs(ifp);
           if(!bFoundInterface)
           {
              SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, errorType_, "ListenToPort - no matching interface", O_INIT_PROCESS, F_SOCKET, errno, 0);
              goto bailout;
           }
//LCOV_EXCL_STOP
		}
		else
		{
		   /*
			* bind to all available interfaces
			*/
			if (m_bIPv4 == false)
			{
//LCOV_EXCL_START
				bzero((char*)&m_ListenSocketAddr6,sizeof(m_ListenSocketAddr6));
				m_ListenSocketAddr6.sin6_family = AF_INET6;
				m_ListenSocketAddr6.sin6_addr = in6addr_any;
				m_ListenSocketAddr6.sin6_port = htons((uint16_t) port);
//LCOV_EXCL_STOP
			}
			else
			{
				bzero((char*)&m_ListenSocketAddr,sizeof(m_ListenSocketAddr));
				m_ListenSocketAddr.sin_family = AF_INET;
				m_ListenSocketAddr.sin_addr.s_addr = INADDR_ANY;
				m_ListenSocketAddr.sin_port = htons((in_port_t) port);
			}
		}

		int optVal = 1;
		error = setsockopt(m_nListenSocketFnum, SOL_SOCKET, SO_REUSEADDR, (char*)&optVal, sizeof(optVal));
		if (error != 0)
		{
//LCOV_EXCL_START
			SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, errorType_,
                      "ListenToPort", O_INIT_PROCESS, F_SETSOCOPT, errno,
                      SO_REUSEADDR);
			goto bailout;
//LCOV_EXCL_STOP
		}
		if (m_bIPv4 == false)
			error = bind(m_nListenSocketFnum, (struct sockaddr *)&m_ListenSocketAddr6, (int)sizeof(m_ListenSocketAddr6));
		else
			error = bind(m_nListenSocketFnum, (struct sockaddr *)&m_ListenSocketAddr, (int)sizeof(m_ListenSocketAddr));

		if (error < 0)
		{
//LCOV_EXCL_START
			SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, errorType_,
                      "ListenToPort", O_INIT_PROCESS, F_BIND, errno, 0);
			goto bailout;
//LCOV_EXCL_STOP
		}

		optVal = 1;
		error = setsockopt(m_nListenSocketFnum, SOL_SOCKET, SO_KEEPALIVE, (char*)&optVal, sizeof(optVal));
		if (error != 0)
		{
//LCOV_EXCL_START
			SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, errorType_,
                      "ListenToPort", O_INIT_PROCESS, F_SETSOCOPT, errno,
                      SO_KEEPALIVE);
			goto bailout;
//LCOV_EXCL_STOP
		}
	}

	error = listen(m_nListenSocketFnum, 100);

	FD_ZERO(&read_fds_);
	FD_ZERO(&error_fds_);

	if(error >= 0)
	{
		FD_SET(m_nListenSocketFnum,&read_fds_);
		FD_SET(m_nListenSocketFnum,&error_fds_);

		// Keep track of highest socket file descriptor, for use in "select"
		if (m_nListenSocketFnum > max_read_fd_) max_read_fd_ = m_nListenSocketFnum;
	}
	else
	{
		SET_WARNING((long)0, NSK, TCPIP, UNKNOWN_API, E_LISTENER, "ListenToPort ", O_INIT_PROCESS, F_ACCEPT, errno, 0);
		goto bailout;
	}

    // If tracing is enabled, display trace info indicating new "listen"
	LISTEN_ON_SOCKET((short)m_nListenSocketFnum);
	return true;

bailout:
	if (m_nListenSocketFnum > 0)
	{
        closeTCPIPSession(m_nListenSocketFnum);
		GTransport.m_TCPIPSystemSrvr_list->del_node(m_nListenSocketFnum);
	}
	m_nListenSocketFnum = -2;
	sprintf(tmp,"bailout ListenToPort[%d][%d] ", port, m_nListenSocketFnum );
	SET_WARNING((long)0, NSK, TCPIP, UNKNOWN_API, errorType_, tmp, O_INIT_PROCESS, F_SOCKET, 0, 0);
	return false;
}

void* CNSKListenerSrvr::OpenTCPIPSession()
{
	CTCPIPSystemSrvr* pnode = NULL;
    int error;
    int nSocketFnum = -2;
	SSL_CTX *ctx;
    if (m_bIPv4 == false)
    {
//LCOV_EXCL_START
       m_nAcceptFromSocketAddrLen = sizeof(m_AcceptFromSocketAddr6);
       nSocketFnum = accept(m_nListenSocketFnum, (sockaddr*)&m_AcceptFromSocketAddr6, (socklen_t *)&m_nAcceptFromSocketAddrLen);
//LCOV_EXCL_STOP
    }
    else
    {
        m_nAcceptFromSocketAddrLen = sizeof(m_AcceptFromSocketAddr);
        nSocketFnum = accept(m_nListenSocketFnum, (sockaddr*)&m_AcceptFromSocketAddr, (socklen_t *)&m_nAcceptFromSocketAddrLen);
    }

	if(nSocketFnum == -1)
	{
//LCOV_EXCL_START
		SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, errorType_,
                  "OpenTCPIPSession", O_INIT_PROCESS, F_ACCEPT,
                  errno, 0);
		goto bailout;
//LCOV_EXCL_STOP
	}

    TCP_SetKeepalive(nSocketFnum,
            srvrGlobal->clientKeepaliveStatus,
            srvrGlobal->clientKeepaliveIdletime,
            srvrGlobal->clientKeepaliveIntervaltime,
            srvrGlobal->clientKeepaliveRetrycount);
	pnode = GTransport.m_TCPIPSystemSrvr_list->ins_node(nSocketFnum);
	
	#define MAX_FILE_PATH 1024
	if(buseSSLEnabled&&pnode){
        char *certdir = NULL;
        char *clustername = getenv("CLUSTERNAME");
        char certifiate_file_path[MAX_FILE_PATH] = {0};
        char private_key_path[MAX_FILE_PATH] = {0};

        if(clustername == NULL || (strcmp(clustername, "") == 0))
            certdir = getenv("TRAF_VAR");
        else
            certdir = getenv("HOME");

        snprintf(certifiate_file_path, MAX_FILE_PATH, "%s/sqcert/server.crt", certdir);
        snprintf(private_key_path, MAX_FILE_PATH, "%s/sqcert/server.key", certdir);

		SSL_load_error_strings();
        ERR_load_crypto_strings();
        
        OpenSSL_add_all_algorithms();
        SSL_library_init();
        
        ctx = SSL_CTX_new(TLSv1_server_method());
        if (ctx == NULL) {
            //erro unable to load context.
            goto bailout;
        }
		int retcode=0;
        retcode=SSL_CTX_use_certificate_file(ctx, certifiate_file_path, SSL_FILETYPE_PEM);
		if(retcode!=1){
			goto bailout;
		}
        retcode=SSL_CTX_use_PrivateKey_file(ctx, private_key_path, SSL_FILETYPE_PEM);
		if(retcode!=1){
			goto bailout;
		}

		pnode->openssl = NULL;
		pnode->openssl = SSL_new(ctx);
        pnode->accept_bio = NULL;
        pnode->accept_bio = BIO_new_socket(nSocketFnum, BIO_CLOSE);
        if(pnode->openssl&&pnode->accept_bio){ 
            SSL_set_bio(pnode->openssl, pnode->accept_bio, pnode->accept_bio);
        
       		retcode=SSL_accept(pnode->openssl);
			if(retcode<=0){
				goto bailout;
			}
        
        	ERR_print_errors_fp(stderr);
        
        	pnode->bio = NULL;
        	pnode->bio = BIO_pop(pnode->accept_bio);
		}else{
			goto bailout;
		}

	}
	if (pnode == NULL)
	{
//LCOV_EXCL_START
		SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, errorType_,
                  "OpenTCPIPSession", O_INIT_PROCESS, F_INS_NODE,
                  SRVR_ERR_MEMORY_ALLOCATE, 0);
		goto bailout;
//LCOV_EXCL_STOP
	}
// clear/zero the set
	FD_ZERO(&read_fds_);
        FD_ZERO(&error_fds_);

// (re)set the listening socket
	FD_SET(m_nListenSocketFnum,&read_fds_);
        FD_SET(m_nListenSocketFnum,&error_fds_);

//  (re) set the dummy pipe-read-fd
	FD_SET(pipefd[0],&read_fds_);
        FD_SET(pipefd[0],&error_fds_);

//set the connected socket
	FD_SET(pnode->m_nSocketFnum,&read_fds_);
	FD_SET(pnode->m_nSocketFnum,&error_fds_);

	if (pnode->m_nSocketFnum > max_read_fd_)
	   max_read_fd_ = pnode->m_nSocketFnum;
	m_nSocketFnum = (short) nSocketFnum;

	return pnode;

bailout:
	if (pnode != NULL){
		if(pnode->openssl){
 			SSL_shutdown(pnode->openssl);
		}
		if(pnode->bio){
 			//BIO_free_all(pnode->bio);
		}
		if(pnode->accept_bio){
    		BIO_free_all(pnode->accept_bio);
		}
        GTransport.m_TCPIPSystemSrvr_list->del_node(nSocketFnum);
	}
    SRVR::BreakDialogue(NULL);

	return NULL;
}

void getDcsStartFile()
{
    const char* filePrefix="DCS-START-TIME-";
    char *trafDir = getenv("TRAF_VAR");
    struct dirent *direntp;
    DIR *dirp = opendir(trafDir);
    if (dirp == NULL)
    {
        MXO_WARN("can not open dir TRAF_VAR %s",trafDir);
    }
    while ((direntp = readdir(dirp)) != NULL)
    {
        if(0==strncmp(direntp->d_name,filePrefix,strlen(filePrefix)))
        {
            snprintf(dcsStartTimeFile,1000,"%s/%s",trafDir,direntp->d_name);
            dcsStartTimeFile[strlen(trafDir)+strlen(direntp->d_name)+1]='\0';
            break;
        } 
    } 
    closedir(dirp);
    if(dcsStartTimeFile[0]=='\0')
    {
        MXO_WARN("DCS-START-TIME file dose not exist");
    }
}
void * CNSKListenerSrvr::tcpip_listener(void *arg)
{
   // Parameter is the CNSKListenerSrvr object
   CNSKListenerSrvr *listener = (CNSKListenerSrvr *) arg;

   int numReadyFds;
   int handledFds;
   ssize_t countRead;
   CTCPIPSystemSrvr* pnode=NULL;
   fd_set temp_read_fds, temp_error_fds;
   struct timeval timeout;
   struct timeval *pTimeout;
   msg_enable_open_cleanup();
   file_enable_open_cleanup();
   getDcsStartFile();
   //create a the dummy pipe
   int rc = pipe(listener->pipefd);
   if (rc < 0)
   {
	listener->TRACE_UNKNOWN_INPUT();
	SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"tcpip_listener", O_PIPE, F_INIT_PIPE,SRVR_ERR_UNKNOWN_REQUEST,0);
        listener->TCP_TRACE_OUTPUT_R0();
   }
   FD_SET(listener->pipefd[0],&listener->read_fds_);
   FD_SET(listener->pipefd[0],&listener->error_fds_);
   if (listener->pipefd[0] > listener->max_read_fd_)
           listener->max_read_fd_ = listener->pipefd[0];

   // if enabled by cqd, preload traf metadata in compiler cache.
   SQL_EXEC_ClearDiagnostics(0);

   bool needToRestoreCpuBind = false;
   int cpuCount = (int)sysconf(_SC_NPROCESSORS_CONF);
   cpu_set_t get;
   cpu_set_t mask;
   if (srvrGlobal->cpuLimitPercentWhileStart == 100)
   {
	   // do nothing
   }
   else if (srvrGlobal->cpuLimitPercentWhileStart > 20 && srvrGlobal->cpuLimitPercentWhileStart < 100)
   {
	   CPU_ZERO(&get);
	   if (pthread_getaffinity_np(pthread_self(), sizeof(cpu_set_t), &get) == -1)
		   MXO_WARN("Could not get process affinity with using sched_getaffinity");
	   else
	   {
		   vector<int> cpuInUse;
		   for (int i = 0; i < cpuCount; i++)
			   if (CPU_ISSET(i, &get))
				   cpuInUse.push_back(i);

		   cpuCount = cpuInUse.size() * srvrGlobal->cpuLimitPercentWhileStart / 100;
		   if (cpuCount > 0)
		   {
			   CPU_ZERO(&mask);
			   for (int i = 0; i < cpuCount; i++)
				   CPU_SET(cpuInUse[i], &mask);

			   if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &mask) == -1)
				   MXO_WARN("Could not set process affinity with using sched_setaffinity and cpu count = %d", cpuCount);
			   else
				   needToRestoreCpuBind = true;
		   }
		   else
			   MXO_WARN("The percent of cpu used for starting is too low and cpu count = %d", cpuCount);


		   if (cpuCount == cpuInUse.size())
		   {
			   CPU_ZERO(&get);
			   for (int i = 0; i < __CPU_SETSIZE; i++)
				   CPU_SET(cpuInUse[i], &mask);
		   }
	   }
   }
   else
	   MXO_WARN("The percent of cpu used for starting is too low or too high and percent is %d", srvrGlobal->cpuLimitPercentWhileStart);

   int retcode=SQL_EXEC_LoadTrafMetadataInCache();
   if (retcode < 0)
	SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"LoadTrafMetadataIncache", O_LOAD_METADATA, F_LISTEN,retcode,0);

   if(srvrGlobal->loadEnable) {
       CommonLogger::log("MXOSRVR", LL_WARN, "-----------------------------PRELOAD BEGIN--------------------------------");
       preloadTables();
       CommonLogger::log("MXOSRVR", LL_WARN, "-----------------------------PRELOAD END  --------------------------------");
   }else {
       CommonLogger::log("MXOSRVR", LL_WARN, "-----------------------------PRELOAD CLOSED-------------------------------");
   }

   if (checkNodeEnable())
   {
       if(!updateZKState(INITIAL, AVAILABLE) ){
           SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"tcpip_listener:updateZKState failed", O_SELECT, F_FD_ISSET,SRVR_ERR_UNKNOWN_REQUEST, -2);
           SRVR::exitServerProcess();
       }
   }
   else
   {
       if (!updateZKState(INITIAL, DISABLE)){
           SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"tcpip_listener:updateZKState failed", O_SELECT, F_FD_ISSET,SRVR_ERR_UNKNOWN_REQUEST, -2);
           SRVR::exitServerProcess();
       }
   }

   SQL_EXEC_ClearDiagnostics(0);

   if (needToRestoreCpuBind)
   {
      memcpy(&mask, &get, sizeof(cpu_set_t));
      if (pthread_setaffinity_np(pthread_self(), sizeof(cpu_set_t), &mask) == -1)
         MXO_WARN("could not restore process affinity after initial");
   }

   // Persistently wait for input on sockets and then act on it.
   while(listener->m_bTCPThreadKeepRunning)
   {
      // Wait for ready-to-read on any of the tcpip ports
      memcpy(&temp_read_fds, &listener->read_fds_, sizeof(temp_read_fds));
      memcpy(&temp_error_fds, &listener->error_fds_, sizeof(temp_error_fds));
      
      long connIdleTimeout = SRVR::getConnIdleTimeout();
      long srvrIdleTimeout = SRVR::getSrvrIdleTimeout();
      bool connIdleTimer = false;
      bool srvrIdleTimer = false;
      if (srvrGlobal->srvrState == SRVR_CONNECTED)
      {
         if (connIdleTimeout != INFINITE_CONN_IDLE_TIMEOUT)
         {
            timeout.tv_sec = connIdleTimeout;
            timeout.tv_usec = 0; 
            connIdleTimer = true;
            pTimeout = &timeout;
         }
         else 
         {
             timeout.tv_sec = 0;
             timeout.tv_usec = 0;
             pTimeout = NULL;
         }
      }
      else
      {
         if (srvrIdleTimeout != INFINITE_SRVR_IDLE_TIMEOUT)
         {
            timeout.tv_sec = srvrIdleTimeout;
            timeout.tv_usec = 0; 
            srvrIdleTimer = true;
            pTimeout = &timeout;
         }
         else 
         {
             timeout.tv_sec = 0;
             timeout.tv_usec = 0;
             pTimeout = NULL;
         }
      }

      numReadyFds = select(listener->max_read_fd_+1, &temp_read_fds, NULL,&temp_error_fds, pTimeout);
      srvrGlobal->mutex->lock();
      if (numReadyFds == -1)
      {
         if (errno == EINTR)
         {
            srvrGlobal->mutex->unlock();
	    continue;
         }
         else 
         {
            SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"tcpip_listener", O_SELECT, F_SELECT,errno,numReadyFds);
            abort();
         }
      }

      if (numReadyFds == 0)  //Timeout expired
      {
         if (connIdleTimer)
            SRVR::BreakDialogue(NULL);
         else if (srvrIdleTimer)
            SRVR::srvrIdleTimerExpired(NULL);
         else
         {
            SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"tcpip_listener", O_SELECT, F_SELECT,errno,numReadyFds);
            abort();
         }
      }
      else
      {
            // Handle all ready-to-read file descriptors
            handledFds = 0;

	    if(FD_ISSET(listener->pipefd[0], &temp_read_fds))
	    {
            int size = 0;
            int readed = 0;
            char bufferFromPipe[100] = "";
            readed = read(listener->pipefd[0], &size, sizeof(int));
            if (readed == 4)
            {
                readed = 0;
                while (readed < size)
                {
                    readed += read(listener->pipefd[0], bufferFromPipe + readed, size);
                }
                bufferFromPipe[readed] = '\0';
            }

            if ((strcmp(bufferFromPipe, "select loop next")) == 0) //dummy write, next the loop
            {
                srvrGlobal->mutex->unlock();
                continue;
            }
            else //dummy write, exit the loop
            {
                listener->m_bTCPThreadKeepRunning = false;
                srvrGlobal->mutex->unlock();
                break;
            }
	    }
	    else if (FD_ISSET(listener->m_nListenSocketFnum,&temp_read_fds))
	    {
                 // Initiate a new client session
               listener->OpenTCPIPSession();
               listener->TRACE_INPUT((short)listener->m_nListenSocketFnum, 0, 0, 0);
			   handledFds++;

            }
            else if ((pnode=GTransport.m_TCPIPSystemSrvr_list->m_current_node) != NULL && FD_ISSET(pnode->m_nSocketFnum,&temp_read_fds))
            {

		    do{
			short retries = 0;
			    do
			    {
				    if (buseSSLEnabled)
				    {           
					    countRead = SSL_read(pnode->openssl, pnode->m_IObuffer, MAX_TCP_BUFFER_LENGTH);

					    switch (SSL_get_error(pnode->openssl, countRead))
					    { 
						    case SSL_ERROR_NONE:
							    break;
						    case SSL_ERROR_ZERO_RETURN:
							    break;
						    case SSL_ERROR_WANT_READ:
							    continue;
						    case SSL_ERROR_WANT_CONNECT:
							    goto bailout;
						    case SSL_ERROR_WANT_ACCEPT:
							    goto bailout;
						    case SSL_ERROR_WANT_X509_LOOKUP:
							    goto bailout;
						    case SSL_ERROR_SYSCALL:
							    goto bailout;
						    default:
							    goto bailout;
					    }
				    }else{
					    countRead = recv(pnode->m_nSocketFnum,
							    pnode->m_IObuffer,
							    MAX_TCP_BUFFER_LENGTH, 0);
				    }
			    } while ((countRead < 0) && (errno == EINTR) && (retries++ < 3));
		    }while((countRead < 0) && (errno == EAGAIN));

               if (countRead <= 0)
	       {
                  int cached_errno = errno; // avoid the following operation changing errno
                  if(terminateAndClose){
                      SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"tcpip_listener:terminateAndClose", O_SELECT, F_FD_ISSET,SRVR_ERR_UNKNOWN_REQUEST, -2);
                      SRVR::exitServerProcess();
                  }
                  if(availableFlag == 1){
                      availableFlag = 0;
                      if(!updateZKState(CONNECTED, AVAILABLE) ){
                          SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"tcpip_listener:updateZKState failed", O_SELECT, F_FD_ISSET,SRVR_ERR_UNKNOWN_REQUEST, -2);
                          SRVR::exitServerProcess();
                      }
                  }
                  int sfd = pnode->m_nSocketFnum; // pnode would be released
                  SRVR::BreakDialogue(NULL);
                  GTransport.m_TCPIPSystemSrvr_list->del_node(sfd);
                  if (countRead < 0) // 0 means the client has closed
                     SET_WARNING((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER, "tcpip_listener", O_SELECT, F_SELECT, cached_errno, sfd);
                  else
                     SET_INFO((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"tcpip_listener", O_SELECT, F_SELECT, cached_errno, sfd);
           }
               else
		{
                    pnode->m_rlength = countRead;
                    if (listener->CheckTCPIPRequest(pnode) == NULL)
		    {
			SRVR::BreakDialogue(NULL);
		     }
	   	}

		handledFds++;
	   }
            else
            {
               if (NULL == pnode){
                   listener->TRACE_UNKNOWN_INPUT();
                   SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"tcpip_listener:pnode is null", O_SELECT, F_FD_ISSET,SRVR_ERR_UNKNOWN_REQUEST, -2);
                   listener->TCP_TRACE_OUTPUT_R0();
               }else{
                   listener->TRACE_UNKNOWN_INPUT();
                   SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"tcpip_listener:socketnum has been deleted", O_SELECT, F_FD_ISSET,SRVR_ERR_UNKNOWN_REQUEST, -2);
                   listener->TCP_TRACE_OUTPUT_R0();
               }
               SRVR::exitServerProcess();
            }

	   if(handledFds != numReadyFds)
	   {
               listener->TRACE_UNKNOWN_INPUT();
               SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER,"tcpip_listener", O_SELECT, F_FD_ISSET,SRVR_ERR_UNKNOWN_REQUEST,0);
               listener->TCP_TRACE_OUTPUT_R0();
               SRVR::exitServerProcess();
            }
      } 

      srvrGlobal->mutex->unlock();

   } //while(listener->m_bTCPThreadKeepRunning)

   return NULL;
	bailout:
		if(pnode->openssl){
 			SSL_shutdown(pnode->openssl);
		}
		if(pnode->bio){
 			BIO_free_all(pnode->bio);
		}
		if(pnode->accept_bio){
    		BIO_free_all(pnode->accept_bio);
		}
 	if (pnode->m_nSocketFnum > 0)
         {
 		GTransport.m_TCPIPSystemSrvr_list->del_node(pnode->m_nSocketFnum);
 		}
    return NULL;
}

int CNSKListenerSrvr::runProgram(char* TcpProcessName, long port, int TransportTrace)
{
   short fnum,error;
   _cc_status cc = 0;
   short timeout;
   unsigned short countRead;
   SB_Tag_Type tag;

   sprintf(m_TcpProcessName,"%s",TcpProcessName);
   m_port = port;

   INITIALIZE_TRACE(TransportTrace);


   if ((error = FILE_OPEN_("$RECEIVE",8,&m_ReceiveFnum, 0, 0, 1, 4000)) != 0)
   {
      SET_ERROR((long)0, NSK, FILE_SYSTEM, UNKNOWN_API, E_SERVER,
         "runProgram", O_INIT_PROCESS, F_FILE_OPEN_, error, 0);
      return error;
   }

//   if (ListenToPort(port) == false)
//      return SRVR_ERR_LISTENER_ERROR1;

   READUPDATEX(m_ReceiveFnum, m_RequestBuf, MAX_BUFFER_LENGTH );

   // Register with association server
   SRVR::RegisterSrvr(srvrGlobal->IpAddress, srvrGlobal->HostName);

   // Start tcpip listener thread
    tcpip_tid = tcpip_listener_thr.create("TCPIP_listener",
      CNSKListenerSrvr::tcpip_listener, this);

   // Persistently wait for input on $RECEIVE and then act on it.
   while(m_bKeepRunning)
   {
      RESET_ERRORS((long)0);

      timeout = -1;
      fnum = m_ReceiveFnum;

      if (gv_ms_shutdown_called)
          {
              m_bKeepRunning = false;
              break;
          }
      
	  cc = AWAITIOX(&fnum, OMITREF, &countRead, &tag, timeout);
	  if (_status_lt(cc)) // some error or XCANCEL
	  {
//LCOV_EXCL_START
             error=0;
             XFILE_GETINFO_(fnum, &error);
             if (error == 26) // XCANCEL was called
	    {
                //join the tcpip thread
                 if(tcpip_tid != 0)
			tcpip_listener_thr.join(tcpip_tid,NULL);
                m_bKeepRunning = false;
             	break;
	    }
//LCOV_EXCL_STOP
	  }

      TRACE_INPUT(fnum,countRead,tag,cc);

      if (fnum == m_ReceiveFnum)
      {
         ADD_ONE_TO_HANDLE(&m_call_id);

         CheckReceiveMessage(cc, countRead, &m_call_id);

         READUPDATEX(m_ReceiveFnum, m_RequestBuf, MAX_BUFFER_LENGTH );
         FS_TRACE_OUTPUT(cc);

      }
      else
      {
//LCOV_EXCL_START
         TRACE_UNKNOWN_INPUT();
         SET_ERROR((long)0, NSK, TCPIP, UNKNOWN_API, E_SERVER, "runProgram",
            O_DO_WRITE_READ, F_FILE_COMPLETE, SRVR_ERR_UNKNOWN_REQUEST,
            fnum);
//LCOV_EXCL_STOP
      }
   }

   return 0;
}

void CNSKListenerSrvr::SYSTEM_SNAMP(FILE* fp)
{
   short info_ele;
   char obuffer[1000];
   char* pbuffer = obuffer;
   int	ip;

   ip=sprintf(pbuffer,"\t<----SYSTEM SNAP---->\n");

   pbuffer +=ip;
   ip=sprintf(pbuffer,"\t\t%15.15s\t\t=\t\t%s(%d)\n","srvrState",frmt_serverstate(srvrGlobal->srvrState),srvrGlobal->srvrState);
   pbuffer +=ip;
   pbuffer = GTransport.m_FSystemSrvr_list->enum_nodes(pbuffer,fp);
   pbuffer = GTransport.m_TCPIPSystemSrvr_list->enum_nodes(pbuffer,fp);

   fwrite(obuffer, strlen(obuffer),1,fp);
   fwrite("\r\n",2,1,fp);
   fflush(fp);
}

void CNSKListenerSrvr::terminateThreads(int status)
{
//   m_bKeepRunning = false; // break out of $RECEIVE and listen loop
    char dummyWriteBuffer[100];

   // Calling sync of repository thread here instead of exitServerProcess() since
   // this also takes care of the case when the process is stopped via a system message.
   SyncPublicationThread();

   if(GETTID() == srvrGlobal->receiveThrId)
   {
      // we're in the $recv thread
	  // If the tcp/ip thread is processing a request, the mutex will be locked
	  // in which case, we'll wait for that request to complete. Once the request
	  // is complete, the listen loop will exit out because m_bKeepRunning is false
	  // If we're able to acquire the lock rightaway, it means the tcp/ip thread is
	  // waiting on a select - we can then safely terminate the thread

      /*
	if(tcpip_tid != 0 && srvrGlobal->mutex->trylock() == 0)
         tcpip_listener_thr.cancel(tcpip_tid);
      */
	// Dummy write
      if((tcpip_tid != 0) && (pipefd[1] != 0))
      {
	strcpy(dummyWriteBuffer, "bye-bye tcp/ip thread!");
	writeDummy(dummyWriteBuffer, strlen(dummyWriteBuffer));
      }
      //Wait tcpip thread to exit
      if(tcpip_tid != 0)
         tcpip_listener_thr.join(tcpip_tid,NULL);
   }
   else
   {
      // we're in the tcp/ip thread - we can just cancel the outstanding
	  // readupdate posted on $receive and exit the thread
      int cc = XCANCEL(m_ReceiveFnum);
      tcpip_listener_thr.exit(NULL);
   }
}

bool CNSKListenerSrvr::verifyPortAvailable(const char * idForPort, int port)
{
	char tmp[500];
    int error;
    struct sockaddr_in6 *sin6 = NULL;
    struct sockaddr_in  *sin4 = NULL;
    max_read_fd_ = 0;

    if (m_bIPv4 == false)
	{
		if ((m_nListenSocketFnum = socket(AF_INET6, SOCK_STREAM, 0)) < 0 )
		{
			m_bIPv4 = true;
			m_nListenSocketFnum = socket(AF_INET, SOCK_STREAM, 0);
		}
	}
	else
		m_nListenSocketFnum = socket(AF_INET, SOCK_STREAM, 0);

	if (m_nListenSocketFnum < 0)
	{
		SET_WARNING((long)0, NSK, TCPIP, UNKNOWN_API, errorType_,
				  "verifyPortAvailable", O_INIT_PROCESS, F_SOCKET, errno, 0);
		return false;
	}

   /*
	* bind to all available interfaces
	*/
	if (m_bIPv4 == false)
	{
		bzero((char*)&m_ListenSocketAddr6,sizeof(m_ListenSocketAddr6));
		m_ListenSocketAddr6.sin6_family = AF_INET6;
		m_ListenSocketAddr6.sin6_addr = in6addr_any;
		m_ListenSocketAddr6.sin6_port = htons((uint16_t) port);
	}
	else
	{
		bzero((char*)&m_ListenSocketAddr,sizeof(m_ListenSocketAddr));
		m_ListenSocketAddr.sin_family = AF_INET;
		m_ListenSocketAddr.sin_addr.s_addr = INADDR_ANY;
		m_ListenSocketAddr.sin_port = htons((in_port_t) port);
	}

	int optVal = 1;
	error = setsockopt(m_nListenSocketFnum, SOL_SOCKET, SO_REUSEADDR, (char*)&optVal, sizeof(optVal));
	if (error != 0)
	{
		SET_WARNING((long)0, NSK, TCPIP, UNKNOWN_API, errorType_,
				  "verifyPortAvailable", O_INIT_PROCESS, F_SETSOCOPT, errno,
				  SO_REUSEADDR);
		return false;
	}
	if (m_bIPv4 == false)
		error = bind(m_nListenSocketFnum, (struct sockaddr *)&m_ListenSocketAddr6, (int)sizeof(m_ListenSocketAddr6));
	else
		error = bind(m_nListenSocketFnum, (struct sockaddr *)&m_ListenSocketAddr, (int)sizeof(m_ListenSocketAddr));

	if (error < 0)
	{
		sprintf(tmp,"verifyPortAvailable:[%d]",port);
		SET_INFO((long)0, NSK, TCPIP, UNKNOWN_API, errorType_,
				  tmp, O_INIT_PROCESS, F_BIND, errno, 0);
		return false;
	}

	optVal = 1;
	error = setsockopt(m_nListenSocketFnum, SOL_SOCKET, SO_KEEPALIVE, (char*)&optVal, sizeof(optVal));
	if (error != 0)
	{
		SET_WARNING((long)0, NSK, TCPIP, UNKNOWN_API, errorType_,
				  "verifyPortAvailable", O_INIT_PROCESS, F_SETSOCOPT, errno,
				  SO_KEEPALIVE);
		return false;
	}
	return (ListenToPort(port));
}

