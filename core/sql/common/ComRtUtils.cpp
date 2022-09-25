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
 * File:         ComRtUtils.cpp
 * Description:  Some common OS functions that are called by the
 *               executor (run-time) and may also b called by other components
 *
 * Created:      7/4/97
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "Platform.h"
#include "PortProcessCalls.h"


#include "ExCextdecs.h"
#include "str.h"
#include "ComRtUtils.h"
#include "charinfo.h"

#include "ComCextdecs.h"


#ifdef _DEBUG
#include <execinfo.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <fstream>
#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <cxxabi.h>
#endif

#include <stdio.h>
#include <dlfcn.h>

#include <pwd.h>
#include <grp.h>


#include "nsk/nskcommonhi.h"
#define  psecure_h_including_section
#define  psecure_h_security_psb_get_
#include "security/psecure.h"
#define  dsecure_h_including_section
#define  dsecure_h_psb_selectors
#include "security/dsecure.h"


#include "fs/feerrors.h"


#include "ComDistribution.h"

#include "logmxevent.h"
#undef SQL_TEXT


#include "seabed/ms.h"
#include "seabed/fs.h"

#include "HdfsClient_JNI.h"
#include "HDFSHook.h"
#include "Range.h"

struct ModName {
public:
  const char * name;
};

static const ModName internalSystemSchemaModNameList[] = {
  {"CMNAMEMAPSQLM_N29_000"} 
  ,{"CMSMDIOREADM_N29_000"} 
  ,{"CMSMDIOWRITEM_N29_000"} 
  ,{"MVQR_N29_000"} 
  ,{"READDEF_N29_000"} 
  ,{"SQLHIST_N29_000"} 
  ,{"SQLUTILS_N29_000"} 
  ,{"HP_ROUTINES_N29_000"} 
  ,{"ANSINAMES_N29_000"} 
  ,{"SECURITYDATA_N29_000"} 
  ,{"USERLIST_N29_000"} 
  ,{"SSQLSRVRDATA_N29_000"} 
  ,{"QVP_N29_000"} 
};

static const ModName internalMxcsSchemaModNameList[] = {
  {"CATANSIMXGTI"},
  {"CATANSIMXJAVA"},
  {"CATANSIMX"}
};

// returns TRUE, if modName is an internal module name
NABoolean ComRtIsInternalModName(const char * modName)
{
  Int32 i = 0;
  for (i = 0; i < sizeof(internalSystemSchemaModNameList)/sizeof(ModName); i++)
    {
      if (strcmp(internalSystemSchemaModNameList[i].name, modName) == 0)
	return TRUE;
    }

  for (i = 0; i < sizeof(internalMxcsSchemaModNameList)/sizeof(ModName); i++)
    {
      if (strcmp(internalMxcsSchemaModNameList[i].name, modName) == 0)
	return TRUE;
    }

  return FALSE;
}

// returns 'next' internal 3-part system mod name.
// 'index' keeps track of the current mod name returned. It should
// be initialized to 0 on the first call to this method.
const char * ComRtGetNextInternalModName(Lng32 &index, char * modNameBuf)
{
  if (index == (sizeof(internalSystemSchemaModNameList)
		) / sizeof(ModName)
      )
    return NULL;

  if (index < sizeof(internalSystemSchemaModNameList) / sizeof(ModName))
    {
      strcpy(modNameBuf, systemModulePrefix);
      strcat(modNameBuf, internalSystemSchemaModNameList[index].name);
    }

  index++;

  return modNameBuf;
}

// implementation of open, seek, read, close is platform-dependent
// (no use of C runtime on NSK, not even in the debug version)

// non NSK implementations uses IOSTREAM library
ModuleOSFile::ModuleOSFile() {}

ModuleOSFile::~ModuleOSFile() {}

Int32 ModuleOSFile::open(const char *fname)
{
  fs_.open(fname, ios::in | ios::binary);
  if (fs_.fail())
    return 1;
  else
    return 0;
}

Int32 ModuleOSFile::close()
{
  fs_.close();
  if (fs_.fail())
    return 1;
  else
    return 0;
}

Int32 ModuleOSFile::readpos(char *buf, Lng32 pos, Lng32 len, short &countRead)
{
  // no explicit error handling for these operations
  fs_.seekg(pos, ios::beg);
  fs_.read(buf, len);
  return 0;
}


// -----------------------------------------------------------------------
// Utility proc to move result into a buffer of limited size. Make
// sure the result buffer is always NUL-terminated.
// -----------------------------------------------------------------------
static Lng32 ComRtMoveResult(char *tgt,
			    const char *src,
			    Lng32 tgtBufferLength,
			    Lng32 srcLength)
{
  if (tgtBufferLength > srcLength)
    {
      // the easy case, move result and add NUL terminator
      // (don't rely on the source being NUL-terminated)
      str_cpy_all(tgt,src,srcLength);
      tgt[srcLength] = '\0';
      return 0;
    }
  else
    {
      str_cpy_all(tgt,src,tgtBufferLength-1);
      tgt[tgtBufferLength-1] = '\0';
      return -1;
    }
}

// -----------------------------------------------------------------------
// Get the directory name where NonStop SQL software resides
// (from registry on NT, $SYSTEM.SYSTEM on NSK)
// -----------------------------------------------------------------------
Lng32 ComRtGetInstallDir(
     char *buffer,
     Lng32 inputBufferLength,
     Lng32 *resultLength)	// OUT optional
{
  if (resultLength)
    *resultLength = 0;

  Lng32 result = 0;
  Lng32 lResultLen;

  // For Linux, we need to decide what to do for the install directory. 
  // This is work that is TBD. For now, this is set to null, so that
  // we can re-visit this once a decision has been made. This API
  // is used by catman to determine where to put saved DDL for drop
  // table commands.
  lResultLen=0;
  buffer[lResultLen] = '\0';

  if (result == 0 && resultLength)
    *resultLength = lResultLen;
  return result;
}

static NABoolean canUseModuleDirEnvVar() {
  // get session user id
  return TRUE; // anything goes on NT
}

#define SYSTEMMODULESDIR "/usr/tandem/sqlmx/SYSTEMMODULES/"
#define USERMODULESDIR   "/usr/tandem/sqlmx/USERMODULES/"

Lng32 ComRtGetModuleFileName(
     const char *moduleName,
     const char *moduleDir, // use this as the module dir, if not NULL.
     char *buffer,
     Lng32 inputBufferLength,
     char * sysModuleDir,
     char * userModuleDir,
     Lng32 *resultLength,
     short &isASystemModule)	// OUT optional
{
  if (resultLength)
    *resultLength = 0;

  // NOTE: this code is temporary until modules become SQL objects
  // which may not be in this millennium
  const char * envVal;

  #define bufSize 512
  char * mySQROOT;
  char sysModuleDirNameBuf[bufSize];
  char userModuleDirNameBuf[bufSize];

  //Initialize the buffer
  memset(sysModuleDirNameBuf, 0, bufSize);
  memset(userModuleDirNameBuf, 0, bufSize);

  mySQROOT = getenv("TRAF_HOME");
  if (mySQROOT != NULL && strlen(mySQROOT) <= bufSize-100) {
    strcpy(sysModuleDirNameBuf, mySQROOT);
    strcpy(userModuleDirNameBuf, mySQROOT);
  } else {
    sysModuleDirNameBuf[0] = '\0';
    userModuleDirNameBuf[0] = '\0';
    assert(0);
  }

  //System Module location
  strcat(sysModuleDirNameBuf,"/sql/sqlmx/SYSTEMMODULES/");
  sysModuleDir = sysModuleDirNameBuf;

  // User Module location
  strcat(userModuleDirNameBuf,"/sql/sqlmx/USERMODULES/");
  userModuleDir = userModuleDirNameBuf;


  Int32 isSystemModule = 0;
  //  const char *systemModulePrefix = "NONSTOP_SQLMX_NSK.SYSTEM_SCHEMA.";
  //  const char *systemModulePrefixODBC = "NONSTOP_SQLMX_NSK.MXCS_SCHEMA.";
  //  int systemModulePrefixLen = str_len(systemModulePrefix);
  //  int systemModulePrefixLenODBC = str_len(systemModulePrefixODBC);
  Int32 modNameLen = str_len(moduleName);
  Lng32 lResultLen;
  Lng32 result = 0;
  return result;
}

// -----------------------------------------------------------------------
// Get the cluster (EXPAND node) name (returns "NSK" on NT)
// -----------------------------------------------------------------------
Lng32 ComRtGetOSClusterName(
     char *buffer,
     Lng32 inputBufferLength,
     Lng32 *resultLength, // OUT optional
     short * nodeNumber)
{
  if (resultLength)
    *resultLength = 0;

  Lng32 result = 1;		// positive "ldev"
  Lng32 lResultLen = 0;

  // for now, the cluster name on NT is always NSK
  lResultLen = 3;
  ComRtMoveResult(buffer,"NSK",inputBufferLength,lResultLen);

  if (result > 0 && resultLength)
    *resultLength = lResultLen;
  return result;
}
// -----------------------------------------------------------------------
// Get the MP system catalog name.
// the guardian name of the system catalog is returned in sysCatBuffer and
// its size in sysCatLength.
// Error code or 0 is returned for error case and success case respectively.
// -----------------------------------------------------------------------
Lng32 ComRtGetMPSysCatName(
     char *sysCatBuffer,        // in/out
     Lng32 inputBufferLength,    // in
     char *inputSysName,        // in, must set to NULL if no name is passed.
     Lng32 *sysCatLength,	// out
     short *detailError,        // out
     CollHeap *heap)            // in

{
  // the following enum is replicated from ComMPSysCat.h for detail error
  enum MPSysCatErr { NSK_SYSTEM_NAME_ERROR
                   , NSK_CANNOT_LOCATE_MP_SYSTEM_CATALOG
                   , NSK_DEVICE_GETINFOBYLDEV_ERROR
                   , NSK_FILE_GETINFOBYNAME_ERROR
                   };

  const Int32 DISPLAYBUFSIZE = 8000;
  const Int32 FILENAMELEN = 36;

  char mpSysCat[FILENAMELEN];
  Lng32 nameSize = 0;

  Lng32 error = 0;
  *detailError = 0;
  char *sysCatLoc = NULL;

#ifdef _DEBUG
  sysCatLoc = getenv("MP_SYSTEM_CATALOG");  // for Dev+QA convenience
#endif

  if (sysCatLoc)
  {
    if (inputBufferLength < str_len(sysCatLoc))
      return -1;
    Int32 locLen = str_len (sysCatLoc);

    // add local system name if not specified.
    //
    if ((sysCatLoc[0] != '\\') && (sysCatLoc[0] == '$'))
      {
	mpSysCat[0] = '\\';

        error = ComRtGetOSClusterName (&mpSysCat[1], sizeof(mpSysCat), &nameSize);
	if (error || nameSize == 0)
	  {
	    *detailError = NSK_SYSTEM_NAME_ERROR;
	    return -1;
	  }
	nameSize++;
	mpSysCat[nameSize] = '.';
	str_cpy_all(&mpSysCat[nameSize+1], sysCatLoc, locLen);
	locLen = locLen + nameSize + 1;
	str_cpy_all (sysCatBuffer, mpSysCat,locLen );
      }
    else
      {
	if (sysCatLoc[0] != '\\')
	  return -1;
	str_cpy_all (sysCatBuffer, sysCatLoc, locLen);
      }
    sysCatBuffer[locLen] = '\0';
    *sysCatLength = locLen;
  }
  else {
    sysCatLoc = sysCatBuffer;
    *sysCatLoc = '\0';
    *sysCatLength = 0;


    // Allows some debugging/testing of this code while on NT.
    struct la_display_table_struct { char cat_volname[10], cat_subvolname[10]; };
    la_display_table_struct la   = { "??MPVOL ", "MPSYSCAT" };
    la_display_table_struct *tab = &la;
    char sysName[8] = "\\MPSYS ";


    size_t i, z;
    for (i = 0; i < 8; i++)  //padded with blanks
      {
	if (sysName[i] == ' ') break;
      }
    str_cpy_all(sysCatLoc, sysName, i);
    if (i)
      sysCatLoc[i++] = '.';
    z = i;

    for (i = 2; i < 8; i++)  // the first 2 chars are the sysnum, and the volume
                             // name is blank-padded
      {
	if (tab->cat_volname[i] == ' ') break;
      }
    //ComDEBUG(i > 2);
    sysCatLoc[z++] = '$';
    str_cpy_all(sysCatLoc + z, tab->cat_volname + 2, i - 2);
    z += i - 2;
    sysCatLoc[z++] = '.';
    for (i = 0; i < 8; i++)  //padded with blanks
      {
	if (tab->cat_subvolname[i] == ' ') break;
      }
    str_cpy_all(sysCatLoc + z, tab->cat_subvolname, i);
    sysCatLoc[z+i] = '\0';
    *sysCatLength = (Lng32)z+i;

  }

  return 0;
}

// -----------------------------------------------------------------------
// Upshift a simple char string
// -----------------------------------------------------------------------
//#define TOUPPER(c) (((c >= 'a') && (c <= 'z')) ? (c - 32) : c);
void ComRt_Upshift (char * buf)
{
// NOTE: buf is assumed to be non-NULL so it will not be checked again
//  if (buf && (*buf != '\"'))

    if (*buf != '\"')
    {
        // Assume that the name is in the form of an SQL92 <identifier>,
        // meaning that it could be a (case-insensitive) unquoted name
        // or a (case-sensitive) delimited identifier in double quotes.
        // So, upshift the name if it's not in double quotes.
        // NOTE: this code has a counterpart in the
        // parser. Make sure both parts are kept in sync.

        register char * pBuf = buf;

        // NOTE: the string in buf is assumed to null terminated, so no need
        //       to worry about the loop not terminating appropriately

        while (*pBuf)
        {
	    *pBuf = TOUPPER(*pBuf);
            ++pBuf;
        }
    }
}

const char * ComRtGetEnvValueFromEnvvars(const char ** envvars,
					 const char * envvar,
					 Lng32 * envvarPos)
{
  if (envvarPos)
    *envvarPos = -1;

  if (! envvars)
    return NULL;

  Lng32 envvarLen = str_len(envvar);
  for (Int32 i = 0; envvars[i]; i++)
    {
      // Each envvar[i] is of the form:  envvar=value
      // search for '='
      Int32 j = 0;
      for (j = 0;  ((envvars[i][j] != 0) && (envvars[i][j] != '=')); j++);

      if (envvars[i][j] == '=')
	{
	  if ((j == envvarLen) && (str_cmp(envvar, envvars[i], j) == 0))
	    {
	      if (envvarPos)
		*envvarPos = i;
	      return &envvars[i][j+1];
	    }
	}
    }

  return NULL;
}

#if defined (_DEBUG)
// -----------------------------------------------------------------------
// Convenient handling of envvars: Return a value if one exists
// NB: DEBUG mode only!
// -----------------------------------------------------------------------
NABoolean ComRtGetEnvValue(const char * envvar, const char ** envvarValue)
{
  const char * ptr = getenv(envvar);
  if (!ptr)
    // envvar not there
    return FALSE;
  if (!strlen(ptr))
    // envvar there but no value
    return FALSE;

  if (envvarValue)
    // only return a value if caller asked for one
    *envvarValue = ptr;
  return TRUE;
}

NABoolean ComRtGetEnvValue(const char * envvar, Lng32 * envvarValue)
{
  const char * ptr;
  if (!ComRtGetEnvValue(envvar, &ptr))
    // envvar not there or no value
    return FALSE;

  Int32 max = strlen(ptr);
  Lng32 tempValue = 0;
  for (Int32 i = 0;i < max;i++)
  {
    if (ptr[i] < '0' || ptr[i] > '9')
      // value is not numeric
      return FALSE;
    tempValue = (tempValue * 10) + (ptr[i] - '0');
  }
  *envvarValue = tempValue;
  return TRUE;
}

NABoolean ComRtGetValueFromFile (const char * envvar, char * valueBuffer,
                                 const UInt32 valueBufferSizeInBytes)
{
  // "envvar" supposedly specifies the name of a file, from which we
  // read values.
  // Return TRUE if value was read from the file, FALSE otherwise
  // Only one file at a time can be handled - each time

  static char * theFileName = NULL;
  static char * theEnvVar;
  static ifstream * theFile;
  const char * tempFileName;

  if (!ComRtGetEnvValue (envvar, &tempFileName))
  {
    // The requested envvar is not set.
    // Close the corresponding file if we have it open
    if (theFileName != NULL)
    {
      if (!strcmp (theEnvVar, envvar))
      {
        // The same envvar => we have the file open
        delete theFile;
        delete theFileName;
        delete theEnvVar;
        theFileName = NULL;
      }
    }
    return FALSE;
  }

  if (theFileName != NULL)
  {
    // We already have a file open, see if it is the same
    // env var and file name
    if (strcmp (theFileName, tempFileName) || strcmp (theEnvVar, envvar))
    {
      // a different env var or file, close the previous one
      delete theFile;
      delete theFileName;
      delete theEnvVar;
      theFileName = NULL;
    }
  }

  if (theFileName == NULL)
  {
    // Set the current env var name and file name and open the file
    theFileName = new char [strlen(tempFileName) + 1];
    strcpy (theFileName, tempFileName);
    theEnvVar = new char [strlen(envvar) + 1];
    strcpy (theEnvVar, envvar);

    theFile = new ifstream (theFileName, ios::in);

  }

  if (theFile->good())
  {
    // Check to make sure the buffer is big enough:
    // The buffer needs at least to be able to hold a NULL terminator
    if (valueBufferSizeInBytes < 1)
      return FALSE;

    UInt32 valStrLimitInBytes = valueBufferSizeInBytes - 1;

    // Read from the file if last read was OK
    char tmpBuf[81];
    theFile->getline(tmpBuf, 80, '\n');
    if (theFile->good())
    {
      if (valStrLimitInBytes <= strlen(tmpBuf))
        strcpy(valueBuffer, tmpBuf);
      else
      {
        // The input string is too long, truncate it to fit the valueBuffer.
        memcpy(valueBuffer, tmpBuf, valStrLimitInBytes);
        valueBuffer[valStrLimitInBytes] = '\0';
      }
      return TRUE;
    }
  }

  if (theFile->eof())
    return FALSE;

  if (theFile->bad() || theFile->fail())
  {
    // File is unusable, get rid of it and clear the
    // current file
    delete theFile;
    delete theFileName;
    theFileName = NULL;
  }
  return FALSE;

}
#endif // #if defined(_DEBUG) ...

// -----------------------------------------------------------------------
//
// ComRtGetProgramInfo()
//
// Outputs:
// 1) the pathname of the directory where the application program
//    is being run from.
//    For OSS processes, this will be the fully qualified oss directory
//      pathname.
//    For Guardian processes, pathname is not set
// 2) the process type (oss or guardian).
// 3) Other output values are: cpu, pin, nodename, nodename Len, processCreateTime
//       and processNameString in the format <\node_name>.<cpu>,<pin>
//
// // Return status:      0, if all ok. <errnum>, in case of an error.
//
// -----------------------------------------------------------------------
Lng32 ComRtGetProgramInfo(char * pathName,    /* out */
			 Lng32 pathNameMaxLen,
			 short  &processType,/* out */
			 Int32  &cpu, /* cpu */
			 pid_t  &pin, /* pin */
			 Lng32   &nodeNumber,
			 char * nodeName, // GuaNodeNameMaxLen+1
			 short  &nodeNameLen,
			 Int64  &processCreateTime,
			 char *processNameString,
			 char *parentProcessNameString
                         , SB_Verif_Type *verifier
                         , Int32 *ancestorNid
                         , pid_t *ancestorPid
)
{
  Lng32 retcode = 0;

  processType = 2;
  strcpy(nodeName, "NSK");
  nodeNameLen = strlen("NSK");
  NAProcessHandle myPhandle;
  myPhandle.getmine();
  myPhandle.decompose();
  cpu = myPhandle.getCpu();
  pin = myPhandle.getPin();
  if (verifier)
    *verifier = myPhandle.getSeqNum();

  // Map the node number to cpu
  nodeNumber = cpu;
  strcpy(processNameString, myPhandle.getPhandleString());
  MS_Mon_Process_Info_Type processInfo;
  if ((retcode = msg_mon_get_process_info_detail(
     processNameString, &processInfo))
                        != XZFIL_ERR_OK)
     return retcode;
  processCreateTime = ComRtGetJulianFromUTC(processInfo.creation_time);
  if (processInfo.parent_nid != -1 && 
      processInfo.parent_pid != -1 && parentProcessNameString)
    strcpy(parentProcessNameString, processInfo.parent_name);
  else
    parentProcessNameString = NULL;

  if (ancestorNid == NULL && ancestorPid == NULL) {
    return retcode;
  }

  // Get the ancestor process information
  char *parentName = parentProcessNameString;
  while (parentName != NULL) {
    retcode = msg_mon_get_process_info_detail(parentName, &processInfo);
    if (retcode != XZFIL_ERR_OK) {
      return retcode;
    }
    if (processInfo.parent_nid != -1 &&
        processInfo.parent_pid != -1) {
      parentName = processInfo.parent_name;
    } else {
      parentName = NULL;
    }
  }

  if (ancestorNid != NULL) {
    *ancestorNid = processInfo.nid;
  }
  if (ancestorPid != NULL) {
    *ancestorPid = processInfo.pid;
  }
  return retcode;
}

Lng32 ComRtGetProcessPriority(Lng32  &processPriority /* out */)
{
  Lng32 retcode = 0;

  processPriority = -1;

  return retcode;
}

Lng32 ComRtGetProcessPagesInUse(Int64 &pagesInUse /* out */)
{
    pagesInUse = -1;
    return 0;
}


// IN:  if cpu, pin and nodeName are passed in, is that to find process.
//      Otherwise, use current process
// OUT: processCreateTime: time when this process was created.
Lng32 ComRtGetProcessCreateTime(short  *cpu, /* cpu */
			       pid_t  *pin, /* pin */
			       short  *nodeNumber,
			       Int64  &processCreateTime,
			       short  &errorDetail
			       )
{
  Lng32 retcode = 0;

  MS_Mon_Process_Info_Type processInfo;
  char processName[MS_MON_MAX_PROCESS_NAME];

  Int32 lnxCpu = (Int32) (*cpu);
  Int32 lnxPin = (Int32) (*pin);
  processCreateTime = 0;
  if ((retcode = msg_mon_get_process_name(lnxCpu, lnxPin, processName))
                        != XZFIL_ERR_OK)
     return retcode;
  if ((retcode = msg_mon_get_process_info_detail(processName, &processInfo))
                        != XZFIL_ERR_OK)
     return retcode;
  processCreateTime = ComRtGetJulianFromUTC(processInfo.creation_time);
  return retcode;
}



Lng32 ComRtSetProcessPriority(Lng32 priority,
			     NABoolean isDelta)
{
  short rc = 0;

  return rc;
}


Lng32 ComRtGetIsoMappingEnum()
{

  return (Lng32)CharInfo::DefaultCharSet;

}

char * ComRtGetIsoMappingName()
{
  Lng32 ime = ComRtGetIsoMappingEnum();

  return (char*)CharInfo::getCharSetName((CharInfo::CharSet)ime);
}

Int32 ComRtPopulatePhysicalCPUArray(Int32 *&cpuArray, NAHeap *heap)
{
  Int32 nodeCount = 0;
  Int32 configuredNodeCount=0;
  Int32 nodeMax = 0;
  Int32 prevCPU = -1;
  MS_Mon_Node_Info_Entry_Type *nodeInfo = NULL;
  NABoolean match = FALSE;

  cpuArray = NULL;

  while (!match)
    {
      // Get the number of nodes to know how much info space to allocate
      Int32 error = msg_mon_get_node_info(&nodeCount, 0, NULL);
      if (error != 0)
        return 0;
      if (nodeCount <= 0)
        return 0;

      // Allocate the space for node info entries
      nodeInfo = new(heap) MS_Mon_Node_Info_Entry_Type[nodeCount];
      cpuArray = new(heap) Int32[nodeCount];

      if (!nodeInfo || !cpuArray)
        return 0;

      // Get the node info
      memset(nodeInfo, 0, sizeof(MS_Mon_Node_Info_Entry_Type) * nodeCount);
      nodeMax = nodeCount;
      error = msg_mon_get_node_info(&nodeCount, nodeMax, nodeInfo);
      if (error == 0 && nodeCount == nodeMax)
        {
          match = TRUE;
        }
      else
        { 
          NADELETEBASIC(nodeInfo, heap);
          NADELETEBASIC(cpuArray, heap);
          cpuArray = NULL;

          if (error != 0 || nodeCount == 0)
            // an error occurred, don't retry
            return 0;

          // the node count didn't match on the second call,
          // this should be extremely rare; try again
          break;
        }

      for (Int32 i = 0; i < nodeCount; i++)
        {
          if (!nodeInfo[i].spare_node)
            {
              Int32 currCPU = nodeInfo[i].nid;

              cpuArray[configuredNodeCount] = currCPU;
              // make sure we have a list of ascending node ids
              assert(prevCPU < currCPU);
              prevCPU = currCPU;
              configuredNodeCount++;
            }
        }
    } // while !match

  NADELETEBASIC(nodeInfo, heap);
  return configuredNodeCount;
}

NABoolean ComRtGetCpuStatus(char *nodeName, short cpuNum)
{
  NABoolean retval = FALSE;   // assume cpu is down
  MS_Mon_Node_Info_Type nodeInfo;
  memset(&nodeInfo, 0, sizeof(nodeInfo));
  Int32 error = msg_mon_get_node_info_detail(cpuNum, &nodeInfo);
  if ( XZFIL_ERR_OK == error ) {
        if ( MS_Mon_State_Up == nodeInfo.node[0].state ) retval = TRUE;
  }
  return retval;
}

void genLinuxCorefile(const char *eventMsg)
{
  if (eventMsg)
    SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__, eventMsg, 0);
  NAProcessHandle myPhandle;
  myPhandle.getmine();
  myPhandle.decompose();

  char coreFile[PATH_MAX];
  msg_mon_dump_process_name(NULL, myPhandle.getPhandleString(),
                              coreFile);

  char coreLocationMessage[PATH_MAX + 200];
  sprintf(coreLocationMessage, 
            "Core-file for this process created at %s.", coreFile);
  SQLMXLoggingArea::logExecRtInfo(__FILE__, __LINE__,
                                    coreLocationMessage, 0);
}

#ifdef _DEBUG
void saveTrafStack(LIST(TrafAddrStack*) *la, void *addr)
{
  void *btArr[12];
  size_t size;
  char **strings;

  size = backtrace(btArr, 12);
  strings = backtrace_symbols(btArr, size);
  TrafAddrStack *tas = NULL;

  if (size > 0)
    {
      tas = (TrafAddrStack*) malloc(sizeof(TrafAddrStack));
      if (tas)
        {
          tas->addr = addr;
          tas->size = size;
          tas->strings = strings;
          la->insert(tas);
        }
    }
}

bool delTrafStack(LIST(TrafAddrStack*) *la, void *addr)
{
  for (CollIndex i = 0; i < la->entries(); i++)
    {
      TrafAddrStack * tas = la->at(i);

      if (tas->addr == addr)
        {
          free(tas->strings);
          la->removeAt(i);
          delete tas;
          return (true);
        }
    }
  return false; // should not happen
}

// helper function to de-mangle c++ names
void stackDemangle(char *stackString, char *callName, Int32 callNameLen)
{
  char *nameStart = NULL, *nameOffset = NULL, *nameEnd = NULL;

  for (char *loc = stackString; *loc; loc++)
    {
      if (*loc == '(')
        nameStart = loc;
      else if (*loc == '+')
        nameOffset = loc;
      else if (*loc == ')' && nameOffset)
        {
          nameEnd = loc;
          break;
        }
    }
  if (nameStart && nameOffset && nameEnd && nameStart < nameOffset)
    {
      Int32 stat;
      size_t realSize = callNameLen;
      char retedName[800];
      *nameStart = 0;
      *nameOffset++ = 0;
      *nameEnd = 0;
      char *ret = abi::__cxa_demangle(++nameStart, retedName, &realSize, &stat);
      if (stat == 0)
        {
          sprintf(callName, "  %s : %s+%s", stackString, ret, nameOffset);
        }
      else
        { // C function name?
          sprintf(callName, "  %s : %s()+%s", stackString, nameStart, nameOffset);
        }
    }
  else // failed to parse
    {
      sprintf(callName, "  %s", stackString);
    }
}

void dumpTrafStack(LIST(TrafAddrStack*) *la, const char *header, bool toFile)
{
  static THREAD_P Int32 fnValid = 0;
  static THREAD_P char fn[120];
  char funcName[800];
  size_t size;
  char **strings;
  FILE *myFd = NULL;

  if (la == NULL || la->entries() < 1)
    return;

  if (toFile)
    {
      if (fnValid == 0)
        {
          Int32 nid = 0;
          Int32 pid = 0;
          Int64 tid = 0;
          char *progFileName = (char *) "noname";
          char pName[MS_MON_MAX_PROCESS_NAME];
          if (XZFIL_ERR_OK != msg_mon_get_my_info(&nid, &pid, &pName[0],
                                sizeof(pName), NULL, NULL, NULL, &tid))
            tid = rand();

          char *fname = getenv("TRAF_STACK_TRACE_FILE_NAME");

          if (fname == NULL || *fname == 0)
            sprintf(fn, "proc_%lu", tid);
          else
            sprintf(fn, "%s_%lu", fname, tid);

          fnValid = 1;
        }

      if (fnValid)
        myFd = fopen(fn, "a");

      if (myFd == NULL)
        {
          if (header)
            printf("%s:\n", header);
          fnValid = 0;
        }
      else
        {
          if (header)
            fprintf(myFd, "%s:\n", header);
        }
    }

  for (CollIndex i = 0; i < la->entries(); i++)
    {
      TrafAddrStack * tas = la->at(i);
      size = tas->size;
      strings = tas->strings;
  
      if (myFd)
        {
          if (header)
            fprintf(myFd, ">>Unfreed at %p:\n", tas->addr);

          for (size_t k = 0; k < size; k++)
            {
              stackDemangle(strings[k], funcName, 800);
              fprintf(myFd, "%s\n", funcName);
            }
          fflush(myFd);
        }
      else
        {
          if (header)
            printf(">>Unfreed at %p:\n", tas->addr);

          for (size_t k = 0; k < size; k++)
            {
              stackDemangle(strings[k], funcName, 800);
              printf("%s\n", funcName);
            }
        }
      free(strings);  // free the memory allocated by backtrace_symbols()
    }
  la->clear();   // all gone

  if (myFd)
    {
      fprintf(myFd, "\n");
      fclose(myFd);
      myFd = NULL;
    }
}

void displayCurrentStack(Int32 depth)
{
   displayCurrentStack(cout, depth);
}

void displayCurrentStack(ostream& out, Int32 depth)
{
  void *btArr[12];
  size_t size;
  char **strings;

  if ( depth > 12 )
    depth = 11;

  depth++;

  size = backtrace(btArr, depth);
  strings = backtrace_symbols(btArr, size);

  char funcName[800];
  for (Int32 k=1; k<size; k++ ) {

     stackDemangle(strings[k], funcName, sizeof(funcName));

     // try to skip the name of the containing library
     char* actualFuncName = strstr(funcName, " : ");
     if ( actualFuncName ) 
        actualFuncName += 3;
     else 
        actualFuncName = funcName;
       
     out << "stack[" << k << "]=" << actualFuncName << endl;

     //Dl_info info;
     //int ok = dladdr(btArr[k], &info);
     //if (ok)
     //  cout << " saddr=" << (char *)btArr[k] - (char *)info.dli_saddr;
  }
  out << endl;
}

#endif // _DEBUG


Int16 getBDRClusterName(char *bdrClusterName)
{
  MS_Mon_Reg_Get_Type regList;
  Int16  error;
  strcpy(bdrClusterName, "UNKNOWN");

  if ((error = msg_mon_reg_get(MS_Mon_ConfigType_Cluster,
                               false,
                               (char *)"CLUSTER",
                               (char *)BDR_CLUSTER_NAME_KEY,
                               &regList)) == XZFIL_ERR_OK)
  {
    if (regList.num_returned > 0 &&
        (strcmp(regList.list[0].key, BDR_CLUSTER_NAME_KEY) == 0))
    {
      strncpy(bdrClusterName, regList.list[0].value, BDR_CLUSTER_NAME_LEN);
      Int16 i = BDR_CLUSTER_NAME_LEN-1;
      while (bdrClusterName[i] == ' ')
        i--;
      bdrClusterName[i+1] = '\0';
    }
  }
  return error;
}
 
int get_phandle_with_retry(char *pname, SB_Phandle_Type *phandle)
{
  Int32 retrys = 0;
  int lv_fserr = FEOK;
  const Int32 NumRetries = 10;
  timespec retryintervals[NumRetries] = {
                               {  0, 10*1000*1000 }  // 10 ms
                             , {  0, 100*1000*1000 } // 100 ms
                             , {  1, 0 } // 1 second
                             , {  3, 0 } // 3 seconds
                             , {  6, 0 } // 6 seconds
                             , { 10, 0 } // 10 seconds
                             , { 15, 0 } // 15 seconds
                             , { 15, 0 } // 15 seconds
                             , { 15, 0 } // 15 seconds
                             , { 15, 0 } // 15 seconds
                           } ;

  for (;;)
  {
    lv_fserr = XFILENAME_TO_PROCESSHANDLE_(pname, strlen(pname), phandle);
    if (retrys >= NumRetries)
      break;
    if ((lv_fserr == FEPATHDOWN) ||
        (lv_fserr == FEOWNERSHIP))
      nanosleep(&retryintervals[retrys++], NULL);
    else
      break;
  }
  return lv_fserr;
}

// returns:
//  true - found groups
//  false - no groups available
NABoolean getLinuxGroups(const char *userName,
                         std::set<std::string> &userGroups)
{
  // Fetch passwd structure (contains first group ID for user)
  // Assume Linux usernames are lowercase which may not be true. 
  // This is a limitation for now
  NAString linuxName(userName);
  linuxName.toLower();
  struct passwd *pw = getpwnam(linuxName.data());
  if (pw == NULL)
    return false;

  // If user belongs to more than 0 groups (ngroups) -1 is returned and
  // ngroups returns the number of groups user belongs to
  gid_t *groups = NULL;
  int ngroups = 0;
  if (getgrouplist(linuxName.data(), pw->pw_gid, NULL, &ngroups) == -1)
  {
    if (ngroups > 0)
    {
      groups = (gid_t *) malloc(ngroups * sizeof (gid_t));
      getgrouplist(linuxName.data(), pw->pw_gid, groups, &ngroups);
    }
    else
      ngroups = 0;
  }

 // Insert group names into list
  if (ngroups > 0)
  {
    for (int j = 0; j < ngroups; j++)
    {
      struct group *gr = getgrgid(groups[j]);
      if (gr != NULL)
        userGroups.insert(gr->gr_name);
    }
    free (groups);
  }
  return true;
}


// A function to return the string "UNKNOWN (<val>)" which can be
// useful when displaying values from an enumeration and an unexpected
// value is encountered. The function is thread-safe. The returned
// string can be overwritten by another call to the function from the
// same thread.
static __thread char ComRtGetUnknownString_Buf[32];
const char *ComRtGetUnknownString(Int32 val)
{
  sprintf(ComRtGetUnknownString_Buf, "UNKNOWN (%d)", (int) val);
  return &(ComRtGetUnknownString_Buf[0]);
}

NABoolean getHDFSDefaultName(NAString& host, Int32& port)
{
   NABoolean result = FALSE;

  char buf[1024];
  HDFS_Client_RetCode rc = HdfsClient::getFsDefaultName(buf, sizeof(buf));
  if (rc == HDFS_CLIENT_OK) {
     HHDFSDiags diags;
     if ( HHDFSTableStats::splitLocation(buf, host, port, diags) ) {
        result = TRUE;
     } else {
         host = "";
         port = 0;
     }
   }
   return result;
}

extern char *__progname;

fstream& getPrintHandle()
{
   static fstream fout;

   char hostname[100];
   gethostname(hostname, sizeof(hostname));

   char fname[128];
   sprintf(fname, "%s.%s.%d", hostname, __progname, getpid());

   fout.open(fname, ios::out | ios::app);

   return fout;
}
                 
void 
genFullMessage(char* buf, Int32 len, const char* className, Int32 queryNodeId)
{
   char hostname[100];
   gethostname(hostname, sizeof(hostname));
                 
   snprintf(buf, len, 
            "\nIn %s(%s:%d) of nodeId %d",
            className, 
            hostname, 
            getpid(),
            queryNodeId
           );
}

void OrcSearchArg::outputOperatorControl(std::vector<std::string>& textVec, ExtPushdownOperatorType type)
{
   OrcSearchArg arg;
   arg.getText().append((char*)&type, sizeof(type));
                    
   arg.appendColName(NULL);
   arg.appendTypeIndicator(NULL);
   arg.appendValue(NULL, 0);

   textVec.push_back(arg.getText());
}

void OrcSearchArg::appendOperator(ExtPushdownOperatorType type)
{
    text_.append((char*)&type, sizeof(type));
}

void OrcSearchArg::appendColName(const char* colName)
{
   Lng32 len = (colName ? strlen(colName) : 0);
 
   // column name length
   text_.append((char*)&len, sizeof(len));

   // column name 
   if (len> 0) {
     text_.append(colName);
   } 
}

void 
OrcSearchArg::appendTypeIndicator(const char* colType)
{
   Lng32 indicator = 0;

   if (colType == "TIMESTAMP") 
     indicator = 1;

   text_.append((char*)&indicator, sizeof(indicator));
}

void OrcSearchArg::appendValue(Int32 v)
{
   char buf[15]; // 10 decimal digits max plus a sign character
   snprintf(buf, sizeof(buf), "%d", v);
   Lng32 sz = strlen(buf);
   text_.append((char*)&sz, sizeof(sz));
   text_.append(buf, sz);
}

void OrcSearchArg::appendValue(Int64 v)
{
   char buf[25]; // 19 decimal digits max plus a sign character
   snprintf(buf, sizeof(buf), "%ld", v);
   Lng32 sz = strlen(buf);
   text_.append((char*)&sz, sizeof(sz));
   text_.append(buf, sz);
}

void OrcSearchArg::appendValue(double v)
{
   char buf[50]; 

   snprintf(buf, sizeof(buf), "%lf", v);
   Lng32 sz = strlen(buf);
   text_.append((char*)&sz, sizeof(sz));
   text_.append(buf, sz);
}

void OrcSearchArg::appendValue(const char* v, Lng32 len)
{
   text_.append((char*)&len, sizeof(len));

   if ( v )
      text_.append(v, len);
}

void OrcSearchArg::appendValue(const char* v)
{
   Lng32 len = (v) ? strlen(v) : 0;
   appendValue(v, len);
}

void OrcSearchArg::appendValue(const wchar_t* v)
{
   Lng32 sz = (v) ? NAWstrlen(v) : 0;
   text_.append((char*)&sz, sizeof(sz));
   text_.append((char*)v, 2*sz);
}

void OrcSearchArg::appendValue(RangeDate* d)
{
   char buf[11];
   appendValue(convertDateToAscii(d->date(), buf, sizeof(buf)));
}

void OrcSearchArg::appendValue(RangeTime* t)
{
   char buf[21];
   appendValue(convertTimeToAscii(t->time(), buf, sizeof(buf)));
}

                          
// The source is constructed via the following operationrs.
//
//   arg.appendOperator(type); 
//   arg.appendColName(colName);
//   arg.appendTypeIndicator(colType);
//   arg.appendValue(data, dataLen);
//
//   Extract the data (i.e., the value put via the 4th call above) 
std::string
OrcSearchArg::extractValueFromPredicate(const std::string& source)
{
   Int32 pos = 0;
   Int32 sz = 0;

   const char* p = source.c_str();

   // type
   p += sizeof(ExtPushdownOperatorType);

   // colName
   sz = *(Lng32*)p;
   p += sizeof(Lng32) + sz;

   // type indicator 
   p += sizeof(Lng32);

   // value
   sz = *(Int32*)p;
   p += sizeof(Lng32);

   return std::string(p, sz);
}

OrcSearchArg::OrcSearchArg(const std::string& encoded)
{
   const char* data = encoded.data();

   operatorType_ = *(int*)(data);
      
   filterId_ = -1;

   expectedRows_ = 0;

   if ( isBloomFilterPredicate() ) {
   // format of the buffer (encoded) for each BF ppi_vec entry:
   // (refer to RegularBloomFilter::serializeToSearchPredicate())
   //    operatorType: 4 bytes. 
   //    colNameLen:   4 bytes. 
   //    colName:      colNameLen bytes.
   //    filterId:     4 bytes
   //    minValLen:    4 bytes
   //    minValue:     minValLen bytes.
   //    maxValLen:    4 bytes
   //    maxValue:     maxValLen bytes.
   //    bloomfilter:  all remaining bytes.
      data += sizeof(int);
      colNameLen_ = *(int*)(data);
      data += sizeof(int);
      colName_ = data;
      data += colNameLen_;

      // col type indicator (as int)
      colTypeIndicator_ = *(int32_t*)(data);
      data += sizeof(int32_t);

      // filter id
      int32_t len = *(int32_t*)(data);
      data += sizeof(int32_t);
      std::string x(data, len);
      filterId_ = std::stoi(x);
      data += len;

      // min value
      len = *(int*)(data);
      data += sizeof(int);
      min_.append(data, len);
      data += len;

      // max value
      len = *(int*)(data);
      data += sizeof(int);
      max_.append(data, len);
      data += len;

      //bloom filter body (stored in operand_ and operandLen_
      operandLen_ = *(int*)(data);
      data += sizeof(int);
      operand_ = data;
     
   } else
   if ( isSimplePushdownPredicate() ) {
   // format of bb buffer for each ppi_vec entry:
   //    operatorType: 4 bytes. 
   //    colNameLen:   4 bytes. 
   //    colName:      colNameLen bytes.
   //    operandType:  4 bytes
   //    operandLen:   4 bytes
   //    operandVal:   operandLen bytes
      data += sizeof(int);
      colNameLen_ = *(int*)(data);
      data += sizeof(int);
      colName_ = data;
      data += colNameLen_;
      operandType_ = *(int*)(data);
      data += sizeof(int);
      operandLen_ = *(int*)(data);
      data += sizeof(int);
      operand_ = data;
   } else {
      colNameLen_ = 0;
      colName_ = NULL;
      operandType_ = 0;
      operandLen_ = 0;
      operand_ = NULL;
   }
}

NABoolean OrcSearchArg::isEqualTypePred() const
{
   return ( operatorType_ == SearchOperatorType::EQUALS );
}

NABoolean OrcSearchArg::isLessThanPred() const
{
   return ( operatorType_ == SearchOperatorType::LESSTHAN );
}

NABoolean OrcSearchArg::isLessThanEqualPred() const
{
   return (operatorType_ == SearchOperatorType::LESSTHANEQUALS );
}

NABoolean OrcSearchArg::isGreaterThanPred() const
{
   return ( operatorType_ == SearchOperatorType::GREATERTHAN );
}

NABoolean OrcSearchArg::isGreaterThanEqualPred() const
{
   return ( operatorType_ == SearchOperatorType::GREATERTHANEQUALS );
}

NABoolean OrcSearchArg::isSimplePushdownPredicate() const
{
   switch ( operatorType_ )
   {
      case ExtPushdownOperatorType::EQUALS:
      case ExtPushdownOperatorType::LESSTHAN:
      case ExtPushdownOperatorType::LESSTHANEQUALS:
      case ExtPushdownOperatorType::ISNULL:
         return TRUE;
         break;

      // Note these three enums (GREATERTHAN, GREATERTHANEQUALS, 
      // and ISNOTNULL) are not in ExtPushdownOperatorType and
      // therefore are not listed here. 
      default:
         break;
   }
   return FALSE;
}

NABoolean OrcSearchArg::matchColName(const std::string& name) const
{
   if ( colNameLen_ != name.length() )
      return FALSE;

   return ( strncmp(name.data(), colName_, colNameLen_) == 0 );
}

void OrcSearchArg::reverseOperatorTypeForNot()
{
  switch (operatorType_)
   {
     case SearchOperatorType::LESSTHAN:
       operatorType_ = SearchOperatorType::GREATERTHANEQUALS;
       break;
     case SearchOperatorType::LESSTHANEQUALS:
       operatorType_ = SearchOperatorType::GREATERTHAN;
       break;
     case SearchOperatorType::GREATERTHAN:
       operatorType_ = SearchOperatorType::LESSTHANEQUALS;
       break;
     case SearchOperatorType::GREATERTHANEQUALS:
       operatorType_ = SearchOperatorType::LESSTHAN;
       break;
     case SearchOperatorType::ISNULL:
       operatorType_ = SearchOperatorType::ISNOTNULL;
       break;
     default:
       break;
   }
}

void OrcSearchArg::display()
{
   cout << "Predicate type=" << operatorType_ << " (";
 
   switch ( operatorType_ ) {
     case UNKNOWN_OPER: 
       cout << "unknown oper";
       break;
     case EQUALS: 
       cout << "==";
       break;
     case LESSTHAN: 
       cout << "<";
       break;
     case LESSTHANEQUALS: 
       cout << "<=";
       break;
     case ISNULL:
       cout << "is null";
       break;
     case BLOOMFILTER:
       cout << "bloom";
       break;
     case GREATERTHAN:
       cout << ">";
       break;
     case GREATERTHANEQUALS:
       cout << ">=";
       break;
     case ISNOTNULL:
       cout << "is not null";
       break;
      default:
       cout << "unknown operator type";
       break;
  }
  cout << ")" << endl;
}

bool OrcSearchArg::checkOperandAsBool(NABoolean forced) const
{
   if ( operandAsStdstring().compare("TRUE") == 0 )
      return true;

   if ( operandAsStdstring().compare("FALSE") == 0 )
      return false;

   if ( forced ) 
     assert (false);  

   return false;
}

EncodedHiveType::EncodedHiveType(const std::string& x)
{
   const char* data = x.data();
   type_ = *(Int32*)data;
   data += sizeof(Int32);

   length_ = *(Int32*)(data);
   data += sizeof(Int32);

   precision_ = *(Int32*)(data);
   data += sizeof(Int32);

   scale_ = *(Int32*)(data);
}

void checkSpan(const char* x, Lng32 len)
{
   fstream& out = getPrintHandle();

   for (int i=0; i<len; i++ )
   {
      if ( i % 1000 == 0 )
        out << i << endl;

      out << x[i] << ", ";
   }

   out.close();
}

pid_t ComRtGetConfiguredPidMax()
{
   FILE *fd_pid_max;
   char buffer[100];
   size_t bytesRead = 0;
   pid_t pid_max = 0;

   fd_pid_max = fopen("/proc/sys/kernel/pid_max", "r");
   if (fd_pid_max != NULL) {
      bytesRead = fread(buffer, 1, sizeof(buffer)-1, fd_pid_max);
      if (ferror(fd_pid_max))
         assert(false); 
      if (feof(fd_pid_max))
         clearerr(fd_pid_max);
      buffer[bytesRead] = '\0';
      pid_max = atoi(buffer);
      fclose(fd_pid_max);
      return pid_max;
   } 
   return 0;
}

Int64 getCurrentTime()
{
  // GETTIMEOFDAY returns -1, in case of an error
  Int64 currentTime;
  TimeVal currTime;
  if (GETTIMEOFDAY(&currTime, 0) != -1)
    currentTime = currTime.tv_sec;
  else
    currentTime = 0;
  return currentTime;
}

   
bool filterRecord::operator<(const filterRecord& rhs) const
{
   return rowsToSelect() < rhs.rowsToSelect();
}
   
UInt64 filterRecord::rowsToSelect() const
{
   return MINOF((UInt64)(((double)(rowCount_) / uec_) * probes_), rowCount_);
}
   
float filterRecord::selectivity() const
{
   return (float)((double)(rowsToSelect()) / rowCount_);
}
   
Int16 filterRecord::getFilterId()
{
   NAString name(filterName_);
   size_t p = name.index("_sys_RangeOfValues", 0);

   if ( p != NA_NPOS ) {
     p += strlen("_sys_RangeOfValues");
     return (Int16)(atoi(filterName_ + p));
   }

   return -1;
}
   
void filterRecord::display(ostream& out, const char* msg) const
{
   if ( msg )
      out << msg << endl;

   out << "FilterName=" << filterName_;
   out << ", rowCount=" << rowCount_;
   out << ", uec=" << uec_;
   out << ", probes=" << probes_;
   out << ", rowsToSelect=" << rowsToSelect();
   out << ", selectivity=" << selectivity();
   out << endl;
}

// append a short-handled version of the filter name to list.
// such as RV0(4549 bytes)
void filterRecord::appendTerseFilterName(NAString& list)
{
   char* p = str_str(filterName_, "_sys_RangeOfValues"); 

   if ( p )  {
      // p points at the last character of the substring 
      // "_sys_RangeOfValues" in the host variable, 
      // such as "_sys_RangeOfValues3(4.567KB)"
      p += sizeof("_sys_RangeOfValues") - 1;

      list.append("RV");

      // q points at the character before ')'.
      char* q = str_str(p, ")") - 1; 

      if ( !q ) {
         // unlikely
         list.append(p);
         return;
      }

      // append the substring starting after 
      // "_sys_RangeOfValues", and end at one character
      // before ")".
      list.append(p, q-p+1);

      // prepare and append the selectivity
      char buf[40];
      snprintf(buf, sizeof(buf), ",%ld", rowsToSelect());
      list.append(buf);

      // prepare and append the # of rows to select
      snprintf(buf, sizeof(buf), ",%ld", rowCount());
      list.append(buf);

      list.append(")");
    }
}

UInt32 ScanFilterStats::packedLength()
{
/*
  UInt32 size = sizeof(filterId_);
  size += sizeof(flags_);
*/
  UInt32 size = sizeof(packedBits_);
  size += sizeof(totalRowsAffected_);

  return size;
}

UInt32 ScanFilterStats::pack(char* &buffer)
{
/*
  UInt32 size = packIntoBuffer(buffer, filterId_);
  size += packIntoBuffer(buffer, flags_);
*/
  UInt32 size = packIntoBuffer(buffer, packedBits_);
  size += packIntoBuffer(buffer, totalRowsAffected_);

  return size;
}

void ScanFilterStats::unpack(const char* &buffer)
{
/*
  unpackBuffer(buffer, filterId_);
  unpackBuffer(buffer, flags_);
*/
  unpackBuffer(buffer, packedBits_);

  Int64 rows = 0;
  unpackBuffer(buffer, rows);
  //unpackBuffer(buffer, totalRowsAffected_);
  totalRowsAffected_ = rows;
}

void ScanFilterStats::merge(const ScanFilterStats& other, 
                            ScanFilterStats::MergeSemantics semantics)
{
   switch (semantics) 
   {
     case ScanFilterStats::ADD:
      totalRowsAffected_ += other.totalRowsAffected_;
      break;

     case ScanFilterStats::MAX:
      totalRowsAffected_ = 
         MAXOF(totalRowsAffected_, 
               other.totalRowsAffected_);

      break;

     case ScanFilterStats::COPY:
      totalRowsAffected_ = other.totalRowsAffected_;

      break;

      default:
        assert(0);
   }

   setIsEnabled(getIsEnabled() && other.getIsEnabled() ? TRUE: FALSE);
   setIsSelected(getIsSelected() && other.getIsSelected() ? TRUE: FALSE);
}

// return:
//   0: disabled, regardlesss of selected or not
//   2: enabled and selected
//   1: enabled  and de-selected
Int32 ScanFilterStats::getStateCode() const
{
   if ( !getIsEnabled() )
     return 0;

   if ( getIsSelected() )
     return 2;

   return 1;
}
  
void ScanFilterStats::getVariableStatsInfo(char* buf, Int32 len) const
{
  snprintf(buf, len, "RV%d(%1d,%ld)", 
               packedBits_.filterId_, getStateCode(), totalRowsAffected_); 
}

Lng32 ScanFilterStats::getVariableStatsInfoLen() const
{
  // The max value for Int8 is 255.
  // The max value for Int64 is 9,223,372,036,854,775,807.
  // The materialized format "RV%d(%d,%ld)" is thus no more 
  // than 2+5+1+1+1+19+1+1 bytes = 31 bytes.

  char buf[40]; 

  getVariableStatsInfo(buf, sizeof(buf));

  return strlen(buf);
}

///////////////////////////////////////////////////
// ScanFilterStatsList related methods start here.
///////////////////////////////////////////////////
ScanFilterStatsList::ScanFilterStatsList(ScanFilterStatsList& other)
{
   // Use entries() in case entries_ is corrupted.
   entries_ = other.entries();

   for ( Int32 i=0; i<entries(); i++ )
      scanFilterStats_[i] = other.scanFilterStats_[i];
}

ScanFilterStatsList& 
ScanFilterStatsList::operator=(ScanFilterStatsList& other)
{
   // Use entries() in case entries_ is corrupted.
   entries_ = other.entries();

   for ( Int32 i=0; i<entries(); i++ )
      scanFilterStats_[i] = other.scanFilterStats_[i];

   return *this;
}

UInt32 ScanFilterStatsList::packedLength()
{
   UInt32 size = sizeof(entries_);

   // Use entries() in case entries_ is corrupted.
   for ( Int32 i=0; i<entries(); i++ )
      size += scanFilterStats_[i].packedLength();

/*
cout << "ScanFilterStatsList::packedLength()=" << size 
     << ", sizeof(ScanFilterStats)=" << sizeof(ScanFilterStats) 
     << ", sizeof(ScanFilterStatsList)=" << sizeof(*this) << endl;
*/

   return size;
}

UInt32 ScanFilterStatsList::pack(char * &buffer)
{
   UInt32 size = packIntoBuffer(buffer, entries());

   // Use entries() in case entries_ is corrupted.
   for ( Int32 i=0; i<entries(); i++ )
      size += scanFilterStats_[i].pack(buffer);

   return size;
}

void ScanFilterStatsList::unpack(const char* &buffer)
{
   unpackBuffer(buffer, entries_);

   // Use entries() in case entries_ is corrupted.
   for ( Int32 i=0; i<entries(); i++ ) 
      scanFilterStats_[i].unpack(buffer);
}

void ScanFilterStatsList::addEntry(const ScanFilterStats& x, 
                                   ScanFilterStats::MergeSemantics semantics)
{
   // Merge in the stats if an entry already exists for
   // entry x.
   for ( Int32 j=0; j<entries(); j++ ) {

      if ( x.getFilterId() == scanFilterStats_[j].getFilterId() ) {
         scanFilterStats_[j].merge(x, semantics);
         return;
      }
   }

   if ( entries_ >= MAX_FILTER_STATS )
     return;

    // maintain the ascending order on filterId for all entries 
    // including the new entry 
   for ( Int32 j=0; j<entries(); j++ ) {
     if ( scanFilterStats_[j].getFilterId() > x.getFilterId() ) {

       for ( Int32 k=entries()-1; k>=j; k-- ) {
         scanFilterStats_[k+1] = scanFilterStats_[k];
       }

       scanFilterStats_[j] = x;
       entries_++;

       return;
     }
   }

   scanFilterStats_[entries_++] = x;
}

void ScanFilterStatsList::merge(ScanFilterStats& source, 
                                ScanFilterStats::MergeSemantics semantics)
{
   // Use entries() in case entries_ is corrupted.
   for ( Int32 j=0; j<entries(); j++ ) {
      if ( source.getFilterId() == scanFilterStats_[j].getFilterId() ) {
         scanFilterStats_[j].merge(source, semantics);
         return;
      }
   }

   // attempt adding the source. Could fail if no more space available.
   addEntry(source, semantics);
}

void ScanFilterStatsList::merge(ScanFilterStatsList& other, 
                                ScanFilterStats::MergeSemantics semantics)
{
   // Use entries() in case entries_ is corrupted.
   for ( Int32 j=0; j<other.entries(); j++ ) {
      merge(other.scanFilterStats_[j], semantics);
   }
}

Lng32 ScanFilterStatsList::getVariableStatsInfoLen(const char* msg) const
{
   Lng32 len = 0;
   // Use entries() in case entries_ is corrupted.
   for ( Int32 i=0; i<entries(); i++ ) {
      len += scanFilterStats_[i].getVariableStatsInfoLen();
   }

   len += (msg) ? strlen(msg) : 0;
   len += (entries() > 0) ? entries() - 1 : 0; // for ','s
   len += 2;  // for '[' and ']'
   len += 1;  // for terminating null

   return len;
}

void ScanFilterStatsList::getVariableStatsInfo(char* buf, Int32 maxlen, const char* msg) const
{
   Int32 varStatsInfoLen = getVariableStatsInfoLen(msg);

   if ( maxlen == 0 )
     return;

   buf[0] = NULL;

   if ( varStatsInfoLen > maxlen || entries() == 0 )
     return;

   char* p = buf;

   snprintf(p, maxlen, "%s[", (msg) ? msg : "");

   maxlen -= strlen(p);
   p += strlen(p);

   for ( Int32 i=0; i<entries(); i++ ) {

      if ( scanFilterStats_[i].getVariableStatsInfoLen() >= maxlen )
        return;

      scanFilterStats_[i].getVariableStatsInfo(p, maxlen);

      if ( i < entries()-1 ) {
         maxlen -= strlen(p);
         p += strlen(p);
         snprintf(p, maxlen, ",");
      }

      maxlen -= strlen(p);
      p += strlen(p);
   }
         
   snprintf(p, maxlen, "]");
}

void ScanFilterStatsList::dump(ostream& out, const char* msg)
{
  Lng32 len = getVariableStatsInfoLen(msg);
  
  char* buf = new char[len];

  getVariableStatsInfo(buf, len, msg);

  out << "dump(): " << buf << ", end of dump()" << endl;

  delete buf;
}


Int32 convertJulianTimestamp(Int64 julianTimestamp, char* target)
{
  short timestamp[8];
  INTERPRETTIMESTAMP(julianTimestamp, timestamp);
  short year = timestamp[0];
  char month = (char) timestamp[1];
  char day = (char) timestamp[2];
  char hour = (char) timestamp[3];
  char minute = (char) timestamp[4];
  char second = (char) timestamp[5];
  Lng32 fraction = timestamp[6] * 1000 + timestamp[7];

  str_cpy_all(target, (char *) &year, sizeof(year));
  target += sizeof(year);
  *target++ = month;
  *target++ = day;
  *target++ = hour;
  *target++ = minute;
  *target++ = second;
  str_cpy_all(target, (char *) &fraction, sizeof(fraction));

  return 11;
}

ClusterRole::ClusterRole() {
  FILE* fpipe = popen("atrxdc_get_role", "r");
  if (fpipe > 0) {
    char buf[32];
    if (fread(buf, 32, 1, fpipe) >= 0) {
      if (strncmp(buf, "primary", 7) == 0) {
        role_ = PRIMARY;
      } else {
        role_ = SECONDARY;
      }
      pclose(fpipe);
      return;
    }
    pclose(fpipe);
  }

  // TODO: report error
  role_ = PRIMARY;
}

ClusterRole::ClusterRole(ClusterRole::RoleType role)
  : role_(role){}

bool ClusterRole::operator==(const ClusterRole& other) const {
  return role_ == other.role_;
}

bool ClusterRole::operator!=(const ClusterRole& other) const {
  return !(*this == other);
}

ClusterRole& ClusterRole::get_role() {
  static ClusterRole role;
  return role;
}
