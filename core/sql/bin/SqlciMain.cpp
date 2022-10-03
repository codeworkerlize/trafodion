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
 * File:         SqlciMain.cpp
 * Description:  Main program for mxci
 *               
 *               
 * Created:      4/15/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Platform.h"


#include <iostream>
#include <stdio.h>
#include <stdlib.h>
#include "common/BaseTypes.h"
#include "common/NAAssert.h"
#include "SqlciEnv.h"
#include "SqlciNode.h"
#include "TempIncludes.h"
//#include "UtilInt.h"
#include "parser/StmtCompilationMode.h" // because of a kludge in eh/EHException.cpp
#include "export/ComDiags.h"


  // The following are needed only until the "shadow-process" is implemented.
#include "rosetta/rosgen.h"
#define psecure_h_including_section
#define psecure_h_security_app_priv_
#define psecure_h_security_ntuser_set_
#include "security/psecure.h"





#include "seabed/ms.h"
#include "seabed/fs.h"
#include "SCMBuildStr.h"
#include "SCMVersHelp.h"
#ifdef _DEBUG
#include "cli/Globals.h"
#endif  // _DEBUG

DEFINE_DOVERS(sqlci)

#if defined(_DEBUG)
#include "security/dsecure.h"
#define psecure_h_including_section
#define psecure_h_security_psb_set_
#define psecure_h_security_psb_get_
#include "security/psecure.h"
#define MAX_PRINTABLE_SID_LENGTH 256
#endif  //_DEBUG

#ifdef CLI_DLL
#include "cli/SQLCLIdev.h" 
#endif

#include "eh/EHException.h"

#ifdef _DEBUG_RTS
#include "comexe/ComQueue.h"
#include "cli/Globals.h"
#include "runtimestats/SqlStats.h"
#include "dhalt.h"
#endif

#include "qmscommon/QRLogger.h"

#include <pthread.h>
#include <errno.h>

extern void my_mpi_fclose();

static pthread_t gv_main_thread_id;

  //    jmp_buf Buf;
THREAD_P jmp_buf ExportJmpBuf;
extern THREAD_P jmp_buf* ExportJmpBufPtr;

SqlciEnv * global_sqlci_env = NULL ; // Global sqlci_env for break key handling purposes.

// This processes options of the form:
//   <hyphen><single option char><zero or more blanks><required argument string>
// e.g.  "-ifilename"  "-i filename".
// An (English) error message is emitted for "-i" without an argument,
// "-" without an option letter, and "x" without a hyphen, but
// all other options pass thru with no error, as they may be an X option
// processed later in the InitializeX call.
//
// If the "-u name" option is specified, a copy of the name is
// returned in the user_name output parameter.
// If the "-t name" option is specified, a copy of the name is
// returned in the tenant_name output parameter.
// The names are always uppercased regardless of whether thay are enclosed
// in double quotes.
void processOption(Int32 argc, char *argv[], Int32 &i, 
		   const char *& in_filename,
		   const char *& input_string,
		   const char *& out_filename,
		   char *& sock_port,
                   NAString &user_name,
                   NAString &tenant_name,
                   char *& max_heap
		   )
{
  Int32 iorig = i;
  char op = '\0';
// const char *argAfterOp = NULL;
  char *argAfterOp = NULL;
  Int32 status;
  if (argv[i][0] == '-')
  {
    op = argv[i][1];
    if (op)
      if (argv[i][2])
	argAfterOp = &argv[i][2];
    else if (i < argc-1)
      argAfterOp = argv[++i];
  }

  if (argAfterOp)
  {
    switch (op)
    {
      case 's':	sock_port = argAfterOp;				// socket port number
			break;
      case 'i':	in_filename = argAfterOp;			// input file
			break;
      case 'o':	out_filename = argAfterOp;			// output file
		  break;
      case 'q':	input_string = argAfterOp;			// input qry
		  break;
//    case 'p':	sqlci->getUtil()->setProgramName(argAfterOp);	// utility name
//    		break;
      case 't': // tenant name
      {
        // TODO: disallow this option if this instance isn't licensed for multi-tenancy
        // The tenant_name output parameter will contain a copy of the
        // name
        tenant_name = argAfterOp;
        tenant_name.toUpper();
      } // case 't'
      break;

      case 'u': // user name
      {
        // The user_name output parameter will contain a copy of the
        // name
        user_name = argAfterOp;
        user_name.toUpper();

      } // case 'u'
      break;
      case 'H': // max heap size
      {
        // this used to set the max heap size of jvm.
        max_heap = argAfterOp;
      }
      break;

      default:
	; // ok -- may be an X option, processed later in InitializeX

    } // switch (op)
  } // if (argAfterOp)

  else
  {
    // No argument appears after the option
    switch (op)
    {
      case 'v': printf("sqlci version: %s\n", SCMBuildStr);
        exit(0);
        break;

      case 'i':
      case 'p':
      case 't':
        cerr << argv[0] << ": " << argv[iorig]
             << " option requires an argument -- ignored" << endl;
        break;
        
      case '\0':
        cerr << argv[0] << ": " << argv[iorig]
             << " option is unknown -- ignored" << endl;
        break;
        
      default:
	; // ok -- may be an X option, processed later in InitializeX
        
    } // switch (op)
  } // if (argAfterOp) else ...
}

#ifdef _DEBUG_RTS
_callable void removeProcess()
{
  CliGlobals *cliGlobals = GetCliGlobals();
  StatsGlobals *statsGlobals = cliGlobals->getStatsGlobals();
  if (statsGlobals != NULL)
  {
    int error = statsGlobals->getStatsSemaphore(cliGlobals->getSemId(),
                cliGlobals->myPin());
    if (error == 0)
    {
      statsGlobals->removeProcess(cliGlobals->myPin());
      statsGlobals->releaseStatsSemaphore(cliGlobals->getSemId(), cliGlobals->myPin());
      statsGlobals->logProcessDeath(cliGlobals->myCpu(), cliGlobals->myPin(), "Normal process death");
    }
    else if (error == 4011)
    {
      // BINSEM_LOCK_ timed out. Halt the CPU
      PROCESSOR_HALT_ (SQL_FS_INTERNAL_ERROR);
    }
  }
}
#endif

  // process command line options
  char * g_in_filename = NULL;
  char * g_input_string = NULL;
  char * g_out_filename = NULL;
  char * g_sock_port = NULL;
  char * g_max_heap = NULL;
  NAString g_user_name("");
  NAString g_tenant_name("");

static NABoolean g_sync_with_stdio;

static int    g_argc;
static char** g_argv=0;

void * thread_main (void *p_arg)
{
  void*           lv_thread_ret = 0;
  
  atexit(my_mpi_fclose);

  if (g_sync_with_stdio)
    ios::sync_with_stdio();

  // Establish app user id from the current NT process user identity.
  // This must be done explicitly until the "shadow-process" mechanism
  // is fully implemented.  (It is done too late in cli/Context.cpp.)
  // FX: I'm not sure whether the following code applies
  // 	 to NT only.
 
  Int32 i = 1;
  for (; i < g_argc; i++)
    processOption(g_argc, g_argv, 
                           i, 
                          (const char *&)g_in_filename, 
                          (const char *&)g_input_string,
                          (const char *&)g_out_filename,
                          (char *&)g_sock_port,
                          g_user_name,
                          g_tenant_name,
                          (char *&)g_max_heap
      );

  if (g_sock_port) 
  {
  }  

  // create a SQLCI object
  SqlciEnv * sqlci = new SqlciEnv();
  global_sqlci_env = sqlci;

  if (g_user_name.length() > 0)
    sqlci->setUserNameFromCommandLine(g_user_name);

  if (g_tenant_name.length() > 0)
    sqlci->setTenantNameFromCommandLine(g_tenant_name);   

  if (g_max_heap)
  {
    sqlci->setMaxHeapSize((ULng32)atoi(g_max_heap));
  }

  if (setjmp(ExportJmpBuf))
  {

    printf("\nSQLCI terminating due to assertion failure");
    delete sqlci;
    exit(1); // NAExit(1);
  } 

  ExportJmpBufPtr = &ExportJmpBuf;

  if ((!g_in_filename) &&
      (g_out_filename))
    {
      sqlci->setNoBanner(TRUE);

      // create a logfile with the name out_filename.
      // Do not do that if an in_filename is specified. Users should
      // put the log command in the input file.
      char * logf = new char[strlen("LOG ") + 
			    strlen(g_out_filename) + 
			    strlen(" clear;") +
			    1];
      sprintf(logf, "LOG %s clear;", g_out_filename);
      sqlci->run(NULL, logf);
      delete logf;

      sqlci->get_logfile()->setNoDisplay(TRUE);
    }

  // setup log4cxx, need to be done here so initLog4cxx can have access to
  // process information since it is needed to compose the log name
  QRLogger::initLog4cplus(QRLogger::QRL_MXEXE);

  // run it -- this is where the action is!
  if (g_in_filename || g_input_string)
    sqlci->run(g_in_filename, g_input_string);
  else
    sqlci->run();
    
  if ((!g_in_filename) &&
      (g_out_filename))
    {
      sqlci->run(NULL, (char *)"LOG;");
    }

  // Now we are done, delete SQLCI object
  delete sqlci;
#ifdef _DEBUG_RTS
  removeProcess();
#endif
#ifdef _DEBUG
  // Delete all contexts
  GetCliGlobals()->deleteContexts();
#endif  // _DEBUG

  return (void *) lv_thread_ret;
}

Int32 main (Int32 argc, char *argv[])
{
  dovers(argc, argv);

  // check this before file_init_attach overwrites the user env
  g_sync_with_stdio = (getenv("NO_SYNC_WITH_STDIO") == NULL);

  try
  {
    file_init_attach(&argc, &argv, TRUE, (char *)"");
    msg_debug_hook("sqlci", "sqlci.hook");
    file_mon_process_startup2(true, false);
  }
  catch (...)
  {
    cerr << "Error while initializing messaging system. Please make sure Trafodion is started and up. Exiting..." << endl;
    exit(1);
  }

  g_argc = argc;
  g_argv = argv;

  void* res;
  int s;
  pthread_attr_t lv_attr;
  pthread_attr_init(&lv_attr);

  s = pthread_create(&gv_main_thread_id, &lv_attr,
                     thread_main, (void *) 0);
  if (s != 0) {
    printf("main: Error %d in pthread_create. ret code: %d \n", errno, s);
    return -1;
  }

  s = pthread_join(gv_main_thread_id, &res);
  
  if (s != 0) {
    printf("main: Error %d in pthread_join. ret code: %d \n", errno, s);
    return -1;
  }
  
  return 0;

}
