/*********************************************************************
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
 * File:         <file>
 * Description:  
 *               
 *               
 * Created:      7/10/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "Platform.h"

#include <ctype.h>
#include <string.h>

#include "ComCextdecs.h"

#include "NLSConversion.h"
#include "nawstring.h"
#include "exp_stdh.h"
#include "exp_clause_derived.h"
#include "exp_function.h"

#include "ExpLOB.h"
#include "ExpLOBinterface.h"
#include "ExpLOBexternal.h"
//#include "ExpLobOperV2.h"
#include "ex_globals.h"
#include "ex_god.h"

ExLobGlobals *ExpLOBoper::initLOBglobal(NAHeap *parentHeap, ContextCli *currContext, NABoolean useLibHdfs,
             NABoolean useHdfsWriteLock, short hdfsWriteLockTimeout, NABoolean isHiveRead)
{
  NAHeap *lobHeap = new (parentHeap) NAHeap("LOB Heap", parentHeap);
  ExLobGlobals *exLobGlobals = new (lobHeap) ExLobGlobals(lobHeap);
  exLobGlobals->setUseLibHdfs(useLibHdfs);
  exLobGlobals->setUseHdfsWriteLock(useHdfsWriteLock);
  exLobGlobals->setHdfsWriteLockTimeout(hdfsWriteLockTimeout);
  exLobGlobals->initialize();
  // initialize lob interface
  ExpLOBoper::initLOBglobal(exLobGlobals, lobHeap, currContext, (char *)"default", (Int32)0, isHiveRead);
  return exLobGlobals; 
}


Lng32 ExpLOBoper::initLOBglobal(ExLobGlobals *& exLobGlobals, NAHeap *heap, ContextCli *currContext, char *hdfsServer ,Int32 port, NABoolean isHiveRead)
{
  // call ExeLOBinterface to initialize lob globals
  ExpLOBinterfaceInit(exLobGlobals, heap, currContext, isHiveRead, hdfsServer, port);
  return 0;
}

void ExpLOBoper::deleteLOBglobal(ExLobGlobals *exLobGlobals, NAHeap *heap)
{
  NAHeap *lobHeap = exLobGlobals->getHeap();
  NADELETE(exLobGlobals, ExLobGlobals, lobHeap);
  NADELETE(lobHeap, NAHeap, heap);
}

char * ExpLOBoper::ExpGetLOBDirName(Int64 uid, char * outBuf, Lng32 outBufLen)
{
  if (outBufLen < 26)
    return NULL;

  str_sprintf(outBuf, "LOBP_%020ld", uid);

  return outBuf;
}

char * ExpLOBoper::ExpGetLOBname(Int64 uid, Lng32 lobNum, char * outBuf, Lng32 outBufLen)
{
  if (outBufLen < 31)
    return NULL;

  str_sprintf(outBuf, "LOBP_%020ld_%04d",
	      uid, lobNum);

  return outBuf;
}

char * ExpLOBoper::ExpGetLOBnameWithFilenum(Int64 uid, Lng32 lobNum, Lng32 fileNum,
                                            char * outBuf, Lng32 outBufLen)
{
  if (fileNum <= 0)
    return ExpGetLOBname(uid, lobNum, outBuf, outBufLen);

  str_sprintf(outBuf, "LOBP_%020ld_%04d_%04d",
	      uid, lobNum, fileNum);

  return outBuf;
}

char * ExpLOBoper::ExpGetLOBname(Int64 uid, Lng32 lobNum, 
                                 Int64 keyToHash, Int32 numHashVals,
                                 char * outBuf, Lng32 outBufLen)
{
  if ((keyToHash <= 0) || (numHashVals <= 0))
    return ExpGetLOBname(uid, lobNum, outBuf, outBufLen);

  // hash keyToHash and generate datafile num between 1 and numHashVals
  Int64 hashVal = ExHDPHash::hash8((char*)&keyToHash, 4/*SWAP_EIGHT*/);
  UInt32 datafileNo = (UInt32)((((Int64)hashVal * (Int64)numHashVals) >> 32) + 1);

  return ExpGetLOBnameWithFilenum(uid, lobNum, datafileNo,
                                  outBuf, outBufLen);
}

char * ExpLOBoper::ExpGetLOBDescName(Lng32 schNameLen, char * schName,
				     Int64 uid, Lng32 num, 
				     char * outBuf, Lng32 outBufLen)
{
  if (outBufLen < 512)
    return NULL;

  // if schName is NULL, return unquoted object name
  if (schName == NULL)
    str_sprintf(outBuf, "LOBDsc_%020ld_%04d",
                uid, num);
  else
    str_sprintf(outBuf, "%s.\"LOBDsc_%020ld_%04d\"",
	      schName, uid, num);

  return outBuf;
}

char * ExpLOBoper::ExpGetLOBDescHandleObjNamePrefix(Int64 uid, 
					   char * outBuf, Lng32 outBufLen)
{
  if (outBufLen < 512)
    return NULL;
  
  str_sprintf(outBuf, "%s_%020ld", LOB_DESC_HANDLE_PREFIX,uid);
  
  return outBuf;
}

char * ExpLOBoper::ExpGetLOBDescHandleName(Lng32 schNameLen, char * schName,
					   Int64 uid, Lng32 num, 
					   char * outBuf, Lng32 outBufLen)
{
  if (outBufLen < 512)
    return NULL;
  
  // if schName is NULL, return unquoted object name
  if (schName == NULL)
    str_sprintf(outBuf, "%s_%020ld_%04d",
                LOB_DESC_HANDLE_PREFIX,uid, num);
  else
    str_sprintf(outBuf, "%s.\"%s_%020ld_%04d\"",
                schName, LOB_DESC_HANDLE_PREFIX,uid, num);
  
  return outBuf;
}

Lng32 ExpLOBoper::ExpGetLOBnumFromDescName(char * descName, Lng32 descNameLen)
{
  // Desc Name Format: LOBDescHandle_%020ld_%04d
  char * lobNumPtr = &descName[sizeof(LOB_DESC_HANDLE_PREFIX) + 20 + 1];
  Lng32 lobNum = str_atoi(lobNumPtr, 4);
  
  return lobNum;
}

char * ExpLOBoper::ExpGetLOBDescChunksName(Lng32 schNameLen, char * schName,
					   Int64 uid, Lng32 num, 
					   char * outBuf, Lng32 outBufLen)
{
  if (outBufLen < 512)
    return NULL;
  
  // if schName is NULL, return unquoted object name
  if (schName == NULL)
    str_sprintf(outBuf, "%s_%020ld_%04d",
                LOB_DESC_CHUNK_PREFIX,uid, num);
  else
    str_sprintf(outBuf, "%s.\"%s_%020ld_%04d\"",
                schName, LOB_DESC_CHUNK_PREFIX,uid, num);
    
  return outBuf;
}



char * ExpLOBoper::ExpGetLOBMDName(Lng32 schNameLen, char * schName,
                                   Int64 uid,  
                                   char * outBuf, Lng32 outBufLen)
{
  if (outBufLen < 512)
    return NULL;

  // if schName is NULL, return unquoted object name
  if (schName == NULL)
    str_sprintf(outBuf, "%s_%020ld",
                LOB_MD_PREFIX,uid);
  else
    str_sprintf(outBuf, "%s.\"%s_%020ld\"",
                schName, LOB_MD_PREFIX,uid);

  return outBuf;
}
Lng32 ExpLOBoper::createLOB(ExLobGlobals * exLobGlob, ContextCli *currContext, 
			    char * lobLoc,Int32 hdfsPort,char *hdfsServer,
			    Int64 uid, Lng32 num, Int64 lobMaxSize ,
                            Int32 numLOBdatafiles)
{
  Lng32 rc = 0;

  char buf[LOB_NAME_LEN];
  
  char *lobName = NULL;
  if (numLOBdatafiles < 0) // single datafile for V1 lobs
    {
      lobName = ExpGetLOBname(uid, num, buf, LOB_NAME_LEN);
      if (lobName == NULL)
        return -1;

      // Call ExeLOBinterface to create the LOB
      rc = ExpLOBinterfaceCreate(exLobGlob, lobName, lobLoc, Lob_Outline,
                                 hdfsServer,lobMaxSize, hdfsPort);
    }
  else if (numLOBdatafiles > 0) // multiple datafiles
    {
      for (Int32 i = 1; ((rc == 0) && (i <= numLOBdatafiles)); i++)
        {
          lobName = ExpGetLOBnameWithFilenum(uid, num, i, buf, LOB_NAME_LEN);
          if (lobName == NULL)
            return -1;

          // Call ExeLOBinterface to create the LOB
          rc = ExpLOBinterfaceCreate(exLobGlob, lobName, lobLoc, Lob_Outline,
                                     hdfsServer,lobMaxSize, hdfsPort);
        } // for
    }

  return rc;
}

void ExpLOBoper::calculateNewOffsets(ExLobInMemoryDescChunksEntry *dcArray, Lng32 numEntries)
{
  Int32 i = 0;
  //Check if there is a hole right up front for the first entry. If so start compacting with the first entry.
  if (dcArray[0].getCurrentOffset() != 0)
    {
      dcArray[0].setNewOffset(0);
      for (i = 1; i < numEntries; i++)
        {
          dcArray[i].setNewOffset(dcArray[i-1].getNewOffset() + dcArray[i-1].getChunkLen());
        }
    }
  else
    //Look for the first unused section and start compacting from there.
    {
      NABoolean done = FALSE;
      i = 0;
      Int32 j = 0;
      while (i < numEntries && !done )
        {
          if ((dcArray[i].getCurrentOffset()+dcArray[i].getChunkLen()) != 
              dcArray[i+1].getCurrentOffset())
            {
              j = i+1;
              while (j < numEntries)
                {
                   dcArray[j].setNewOffset(dcArray[j-1].getNewOffset()+dcArray[j-1].getChunkLen());
                   j++;
                }
              done = TRUE;
            }
          i++;
        }
    }
  return ;
}

Lng32 ExpLOBoper::compactLobDataFile(ExLobGlobals *exLobGlob,ExLobInMemoryDescChunksEntry *dcArray,Int32 numEntries,char *tgtLobName,Int64 lobMaxChunkMemSize, NAHeap *lobHeap,  ContextCli *currContext,char *hdfsServer, Int32 hdfsPort, char *lobLoc)
{
  Int32 rc = 0;
  ExLobGlobals * exLobGlobL = NULL;
  // Call ExeLOBinterface to create the LOB
  if (exLobGlob == NULL)
    {
      rc = initLOBglobal(exLobGlobL, lobHeap,currContext,hdfsServer,hdfsPort);
      if (rc)
	return rc;
    }
  else
    exLobGlobL = exLobGlob;
 
   
  rc = ExpLOBinterfacePerformGC(exLobGlobL,tgtLobName, (void *)dcArray, numEntries,hdfsServer,hdfsPort,lobLoc,lobMaxChunkMemSize);
  
  if (exLobGlob == NULL)
     ExpLOBinterfaceCleanup(exLobGlobL);
  return rc;
}

Int32 ExpLOBoper::restoreLobDataFile(ExLobGlobals *exLobGlob, char *lobName, NAHeap *lobHeap, ContextCli *currContext,char *hdfsServer, Int32 hdfsPort, char *lobLoc)
{
  Int32 rc = 0;
  ExLobGlobals * exLobGlobL = NULL;
   if (exLobGlob == NULL)
    {
      rc = initLOBglobal(exLobGlobL, lobHeap,currContext, hdfsServer,hdfsPort);
      if (rc)
	return rc;
    }
  else
    exLobGlobL = exLobGlob;
  rc = ExpLOBinterfaceRestoreLobDataFile(exLobGlobL,hdfsServer,hdfsPort,lobLoc,lobName);
  if (exLobGlob == NULL)
     ExpLOBinterfaceCleanup(exLobGlobL);
  return rc;

}

Int32 ExpLOBoper::purgeBackupLobDataFile(ExLobGlobals *exLobGlob,char *lobName, NAHeap *lobHeap, ContextCli *currContext, char * hdfsServer, Int32 hdfsPort, char *lobLoc)
{
  Int32 rc = 0;
  ExLobGlobals * exLobGlobL = NULL;
  if (exLobGlob == NULL)
    {
      rc = initLOBglobal(exLobGlobL, lobHeap,currContext,hdfsServer,hdfsPort);
      if (rc)
	return rc;
    }
  else
    exLobGlobL = exLobGlob;
  rc = ExpLOBinterfacePurgeBackupLobDataFile(exLobGlobL,(char *)hdfsServer,hdfsPort,lobLoc,lobName);
  if (exLobGlob == NULL)
     ExpLOBinterfaceCleanup(exLobGlobL);
  return rc;
}


Lng32 ExpLOBoper::dropLOB(ExLobGlobals * exLobGlob, ContextCli *currContext,
			  char * lobLoc,Int32 hdfsPort, char *hdfsServer,
			  Int64 uid, Lng32 lobNum, Int32 numLOBdatafiles)
{
  Lng32 rc = 0;

  char buf[LOB_NAME_LEN];

  char *lobName = NULL;
  if (numLOBdatafiles > 0)
    {
      Int32 fileNum = 1;
      for (int fileNum = 1; fileNum <= numLOBdatafiles; fileNum++)
        {
          lobName = ExpGetLOBnameWithFilenum(uid, lobNum, fileNum,  
                                             buf, LOB_NAME_LEN);
          if (lobName == NULL)
            return -1;   

          // Call ExeLOBinterface to drop the LOB
          rc = ExpLOBinterfaceDrop(exLobGlob, hdfsServer, hdfsPort, 
                                   lobName, lobLoc);
        }
    }
  else
    {
      lobName = ExpGetLOBname(uid, lobNum, buf, LOB_NAME_LEN);
      if (lobName == NULL)
        return -1;
      
      // Call ExeLOBinterface to drop the LOB
      rc = ExpLOBinterfaceDrop(exLobGlob, hdfsServer, hdfsPort,
                               lobName, lobLoc);
    }
  return rc;
}

Lng32 ExpLOBoper::purgedataLOB(ExLobGlobals * exLobGlob, char * lobLoc, 
			       Int64 uid, Lng32 lobNum, Int32 numLOBdatafiles)
{
  Lng32 rc = 0;

  char buf[LOB_NAME_LEN];

  char *lobName = NULL;
  if (numLOBdatafiles > 0)
    {
      Int32 fileNum = 1;
      for (int fileNum = 1; fileNum <= numLOBdatafiles; fileNum++)
        {
          lobName = ExpGetLOBnameWithFilenum(uid, lobNum, fileNum,  
                                             buf, LOB_NAME_LEN);
          if (lobName == NULL)
            return -1;   

          // Call ExeLOBinterface to purgedata the LOB
          rc = ExpLOBInterfacePurgedata(exLobGlob, lobName, lobLoc);
        }
    }
  else
    {
      lobName = ExpGetLOBname(uid, lobNum, buf, LOB_NAME_LEN);
      if (lobName == NULL)
        return -1;
      
      // Call ExeLOBinterface to purgedata the LOB
      rc = ExpLOBInterfacePurgedata(exLobGlob, lobName, lobLoc);
    }
  return rc;
}

ExpLOBoper::ExpLOBoper()
{
};

ExpLOBoper::ExpLOBoper(OperatorTypeEnum oper_type,
		       short num_operands,
		       Attributes ** attr,
		       Space * space)
  : ex_clause(ex_clause::LOB_TYPE, oper_type, num_operands, attr, space),
    flags_(0),
    lobNum_(-1),
    lobHandleLenSaved_(0),
    lobStorageType_((short)Lob_Invalid_Storage),
    requestTag_(-1),
    descSchNameLen_(0),
    numLOBdatafiles_(-1),
    numSrcLOBdatafiles_(-1)
{
  lobHandleSaved_[0] = 0;
  lobStorageLocation_[0] = 0;
  srcLobStorageLocation_[0] = 0;
  lobHdfsServer_[0] = 0;
  strcpy(lobHdfsServer_,"");
  lobHdfsPort_ = -1;
  descSchName_[0] = 0;
  lobSize_ = 0;
  lobMaxSize_ = 0;
  lobMaxChunkMemSize_ = 0;
  lobGCLimit_ = 0;
};

ExpLOBoper::~ExpLOBoper()
{
};

void ExpLOBoper::displayContents(Space * space, const char * displayStr, 
				 Int32 clauseNum, char * constsArea)
{
  ex_clause::displayContents(space, displayStr, clauseNum, constsArea);

  char buf[100];
  str_sprintf(buf, "    lobNum_ = %d, lobStorageType_ = %d",
	      lobNum_, lobStorageType_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  Lng32 len = MINOF(strlen(lobStorageLocation_), 60);
  char loc[100];
  str_cpy_all(loc, lobStorageLocation_, len);
  loc[len] = 0;
  if (strlen(srcLobStorageLocation_) > 0)
    str_sprintf(buf, "    lobStorageLocation_    = %s", loc);
  else
    str_sprintf(buf, "    lobStorageLocation_    = %s", loc);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  if (strlen(srcLobStorageLocation_) > 0)
    {
      len = MINOF(strlen(srcLobStorageLocation_), 60);
      char loc[100];
      str_cpy_all(loc, srcLobStorageLocation_, len);
      loc[len] = 0;
      str_sprintf(buf, "    srcLobStorageLocation_ = %s",
                  loc);
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }

  if (strlen(srcLobStorageLocation_) > 0)
    str_sprintf(buf, "    numLOBdatafiles_ = %d", getNumLOBdatafiles());
  else
    str_sprintf(buf, "    numLOBdatafiles_ = %d\n", getNumLOBdatafiles());
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  if (strlen(srcLobStorageLocation_) > 0)
    {
      str_sprintf(buf, "    numSrcLOBdatafiles_ = %d\n", getSrcNumLOBdatafiles());
      space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
    }
}

Lng32 findNumDigits(Int64 val)
{
  Int64 v = val/10;
  Lng32 n = 1;
  while (v > 0)
    {
      n++;
      
      v = v / 10;
    }

  return n;
}

// Generates LOB handle that is stored in the SQL row.
// LOB handle len:  48 bytes plus schNameLen bytes
//
// Version_1:
// <flags><LOBtype><LOBnum><objectUid><descKey><descTS><schNameLen><schName>
// <--4--><---4---><--4---><----8----><---8---><--8---><-----2----><--vc--->
//
void ExpLOBoper::genLOBhandle(Int64 uid, 
			      Lng32 lobNum,
			      Int32 lobType,
			      Int64 descKey, 
			      Int64 descTS,
			      Lng32 flags,
			      short schNameLen,
			      char  * schName,
			      Lng32 &handleLen,
			      char * ptr)
{
  LOBHandle * lobHandle = (LOBHandle*)ptr;
  lobHandle->flags_ = flags;
  lobHandle->lobType_ = lobType;
  lobHandle->lobNum_ = lobNum;
  lobHandle->objUID_ = uid;
  lobHandle->descSyskey_ = descKey;
  lobHandle->descPartnkey_ = descTS;
  lobHandle->schNameLen_ = schNameLen;

  handleLen = sizeof(LOBHandle);
  if (schNameLen > 0)
    {
      char * s = &lobHandle->schName_;
      str_cpy_all(s, schName, schNameLen);
      s[schNameLen] = 0;

      handleLen += schNameLen;
    }
}

// Generates LOB handle that is stored in the SQL row.
// Version_2:
// <flags><LOBtype><LOBnum><objectUid><unused><lobUID><lobSeq>
// <--4--><---4---><--4---><----8----><---8--><--8---><--8--->
void ExpLOBoper::genLOBhandleV2(Int64 uid, 
                                Lng32 lobNum,
                                Int32 lobType,
                                Int64 lobUID,
                                Int64 lobSeq,
                                Lng32 flags,
                                Lng32 &handleLen,
                                char * ptr)
{
  LOBHandleV2 * lobHandle = (LOBHandleV2*)ptr;
  lobHandle->flags_ = flags;
  lobHandle->lobType_ = lobType;
  lobHandle->lobNum_ = lobNum;
  lobHandle->objUID_ = uid;
  lobHandle->lobUID_ = lobUID;
  lobHandle->lobSeq_ = lobSeq;
  lobHandle->inlineDataLen_ = 0;
  lobHandle->inlineDataOffset_ = 0;

  handleLen = sizeof(LOBHandleV2);
}

void ExpLOBoper::updLOBhandleDescKey(Int64 descSyskey, 
                                     char * ptr)
{
  LOBHandle * lobHandle = (LOBHandle*)ptr;
  lobHandle->descSyskey_ = descSyskey;
}

void ExpLOBoper::updLOBhandleType(Int32 lobType, 
                                  char * ptr)
{
  LOBHandle * lobHandle = (LOBHandle*)ptr;
  lobHandle->lobType_ = lobType;
}

void ExpLOBoper::updLOBhandleInlineData(char * handlePtr, 
                                        Int32 inlineDataLen,
                                        char *inlineData)
{
  LOBHandle * lobHandle = (LOBHandle*)handlePtr;
  LOBHandleV2 *lobHandleV2 = (LOBHandleV2*)handlePtr;
  if (isLOBHflagSet(lobHandle->flags_, LOBH_VERSION2))
    {
      lobHandleV2->inlineDataLen_ = inlineDataLen;
      lobHandleV2->inlineDataOffset_ = (inlineDataLen > 0 ? sizeof(LOBHandle) : 0);
      str_cpy_all((char*)(handlePtr + lobHandleV2->inlineDataOffset_), 
                  inlineData, inlineDataLen);
    }
}

// Extracts values from the LOB handle stored at ptr
Lng32 ExpLOBoper::extractFromLOBhandle(Int16 *flags,
				       Lng32 *lobType,
				       Lng32 *lobNum,
				       Int64 *uid, 
				       Int64 *descSyskey, 
				       Int64 *descPartnKey,
				       short *schNameLen,
				       char * schName,
				       char * ptrToLobHandle,
				       Lng32 handleLen)
{
  LOBHandle * lobHandle = (LOBHandle*)ptrToLobHandle;
  if (flags)
    *flags = lobHandle->flags_;
  if (lobType)
    *lobType = lobHandle->lobType_;
  if (lobNum)
    *lobNum = lobHandle->lobNum_;
  if (uid)
    *uid = lobHandle->objUID_;

  if (descSyskey)
    *descSyskey = lobHandle->descSyskey_;

  if (descPartnKey)
    *descPartnKey = lobHandle->descPartnkey_;

  if (schNameLen)
    *schNameLen = lobHandle->schNameLen_;
  if ((schNameLen) && (*schNameLen > 0) && (schName != NULL))
    {
      str_cpy_all(schName, &lobHandle->schName_, *schNameLen);
      schName[*schNameLen] = 0;
    }

  return 0;
}

// Extracts values from the LOB handle stored at ptr
Lng32 ExpLOBoper::extractFromLOBhandleV2(Int16 *flags,
                                         Lng32 *lobType,
                                         Lng32 *lobNum,
                                         Int64 *objUID,
                                         Int32 *inlineDataLen,
                                         Int32 *inlineDataOffset,
                                         Int64 *lobUID,
                                         Int64 *lobSeq,
                                         char * ptrToLobHandle)
{
  LOBHandleV2 * lobHandle = (LOBHandleV2*)ptrToLobHandle;
  if (flags)
    *flags = lobHandle->flags_;
  if (lobType)
    *lobType = lobHandle->lobType_;
  if (lobNum)
    *lobNum = lobHandle->lobNum_;
  if (objUID)
    *objUID = lobHandle->objUID_;

  if (inlineDataLen)
    *inlineDataLen = lobHandle->inlineDataLen_;

  if (inlineDataOffset)
    *inlineDataOffset = lobHandle->inlineDataOffset_;

  if (lobUID)
    *lobUID = lobHandle->lobUID_;

  if (lobSeq)
    *lobSeq = lobHandle->lobSeq_;

  return 0;
}

// 12 byte lock identifier uniquely identifies the LOB file that is being 
// locked.
// <object UID + lob number > Each LOB column has a unique lob number and 
// each column has a unique data file.
void ExpLOBoper::genLobLockId(Int64 objid, Int32 lobNum, char *llid)
{
  memset(llid,'\0',LOB_LOCK_ID_SIZE);
  if (objid != -1 && lobNum != -1)
    {
      memcpy(llid,&objid,sizeof(Int64)) ;
      memcpy(&(llid[sizeof(Int64)]),&lobNum,sizeof(Int32));
    }    
}

// creates LOB handle in string format.
void ExpLOBoper::createLOBhandleString(Int16 flags,
				       Lng32 lobType,
				       Int64 uid, 
				       Lng32 lobNum,
				       Int64 descKey, 
				       Int64 descTS_or_lobUID,
				       short schNameLen,
				       char * schName,
				       char * lobHandleBuf)
{
  str_sprintf(lobHandleBuf, "LOBH%04d%04d%04d%020ld%02d%ld%02d%ld%03d%s",
              flags, lobType, lobNum, uid,
              findNumDigits(descKey), descKey, 
              findNumDigits(descTS_or_lobUID), descTS_or_lobUID,
              schNameLen, (schNameLen > 0 ? schName : ""));
}

// creates LOB handle in string format.
void ExpLOBoper::createLOBhandleStringV2(Int16 flags,
                                         Lng32 lobType,
                                         Lng32 lobNum,
                                         Int64 uid, 
                                         Int32 inlineDataLen,
                                         Int32 inlineDataOffset,
                                         Int64 lobUID,
                                         Int64 lobSeq,
                                         char * lobHandleBuf)
{
  Int32 strInlineDataOffset = 
    sizeof(LOBHandleV2ext) + (inlineDataOffset - sizeof(LOBHandleV2));
  str_sprintf(lobHandleBuf, "LOBH%04d%04d%04d%020ld%010d%010d%020ld%020ld",
              flags, lobType, lobNum, uid,
              inlineDataLen, strInlineDataOffset,
              lobUID, lobSeq);
}

// Extracts values from the string format of LOB handle V2
Lng32 ExpLOBoper::extractFromLOBstringV2(
     Int16 *flags,
     Lng32 *lobType,
     Lng32 *lobNum,
     Int64 *objectUID,
     Int64 *lobUID,
     Int64 *lobSeq,
     char *handleStr,
     Lng32 handleLen)
{
  LOBHandleV2ext *handle = (LOBHandleV2ext*)handleStr;

  if (flags)
    *flags = (Lng32)str_atoi(handle->flags, 4);
  if (lobType)
    *lobType = (Lng32)str_atoi(handle->lobType, 4);
  if (lobNum)
    *lobNum = (Lng32)str_atoi(handle->lobNum, 4);
  if (objectUID)
    *objectUID = (Int64)str_atoi(handle->objectUID, 20);
  if (lobUID)
    *lobUID = (Int64)str_atoi(handle->lobUID, 20);
  if (lobSeq)
    *lobSeq = (Int64)str_atoi(handle->lobSeq, 20);

  return 0;
}

Lng32 ExpLOBoper::extractFromLOBstring(Int64 *uid, 
				       Lng32 *lobNum,
				       Int64 *descPartnKey,
				       Int64 *descSyskey,
				       Int16 *flags,
				       Lng32 *lobType,
				       short *schNameLen,
				       char  *schName,
                                       Int32 *inlineDataLen,
                                       char  *&inlineData,
				       char * handle,
				       Lng32 handleLen)
{
  // opp of:
  // lobV1 format:
  //  str_sprintf(lobHandleBuf, "LOBH%04d%04d%04d%020ld%02d%ld%02d%ld%03d%s",
  //	      flags, lobType, lobNum, uid,
  //	      findNumDigits(descKey), descKey, 
  //	      findNumDigits(descTS), descTS,
  //	      schNameLen, schName)
  
  if ((handleLen < 4) ||
      ((handleLen > 4) && (memcmp(handle, "LOBH", 4) != 0)))
    return -1;

  Lng32 curPos = 4; // skip LOBH header
  if (flags)
    *flags = (Lng32)str_atoi(&handle[curPos], 4);
  curPos += 4;
                                  
  if (handleLen < (4 + 4 + 4  + 20 + 2)) // Minimum sanity check.
    return -1;

  if (lobType)
    *lobType = (Lng32)str_atoi(&handle[curPos], 4);
  curPos += 4;

  if (lobNum)
    *lobNum = (Lng32)str_atoi(&handle[curPos], 4);
  curPos += 4;

  if (uid)
    *uid = str_atoi(&handle[curPos], 20);
  curPos += 20;

  short len1;
  len1 = (short)str_atoi(&handle[curPos], 2);
  curPos += 2;

  if (handleLen < (curPos + len1 + 2))
    return -1;

  if (descPartnKey)
    *descPartnKey = str_atoi(&handle[curPos], len1);
  curPos += len1;

  short len2;
  len2 = (short)str_atoi(&handle[curPos], 2);
  curPos += 2;

  if (handleLen < (curPos + len2 + 3))
    return -1;
  
  if (descSyskey)
    *descSyskey = str_atoi(&handle[curPos], len2);
  curPos += len2;
  
  short lSchNameLen = 0;
  if (schNameLen)
    {
      *schNameLen = (short)str_atoi(&handle[curPos], 3);
      lSchNameLen = *schNameLen;
    }

  curPos += 3;
  if (handleLen < (curPos + lSchNameLen))
    return -1;
  
  if (schNameLen)
    str_cpy_and_null(schName, &handle[curPos], 
                     *schNameLen, '\0', ' ', TRUE);

  if (inlineData)
    inlineData = NULL;
  if (inlineDataLen)
    {
      if (handleLen > curPos + lSchNameLen)
        {
          str_cpy_all((char *)inlineDataLen, (char *)&handle[curPos+lSchNameLen],sizeof(Int32));
          
          inlineData = (char *)&handle[curPos + lSchNameLen + sizeof(Int32)];
        }
      else
        {
          *inlineDataLen = 0;
          inlineData = NULL;
        }
    }

  return 0;
}

Lng32 ExpLOBoper::extractFieldsFromHandleString(
     /*IN*/    char *handleStr,
     /*IN*/    Lng32 handleLen,
     /*INOUT*/ Int32 *lobV2,
     /*INOUT*/ Int64 *objectUID,
     /*INOUT*/ Int32 *inlineDataLen,
     /*INOUT*/ Int32 *inlineDataOffset)
{
  // header and flags are at the same location for both V1 and V2
  LOBHandleV1ext *handleV1ext = (LOBHandleV1ext*)handleStr;
  LOBHandleV2ext *handleV2ext = (LOBHandleV2ext*)handleStr;
  
  if (memcmp(handleV1ext->header, "LOBH", 4) != 0)
    return -1;

  Int32 flags = (Lng32)str_atoi(handleV1ext->flags, 4); 
  NABoolean isV2 = isLOBHflagSet(flags, LOBH_VERSION2);
  if (lobV2)
    *lobV2 = (isV2 ? 1 : 0);

  if (isV2)
    {
      ExpLOBoper::extractFromLOBstringV2(NULL, NULL, NULL, objectUID, 
                                         NULL, NULL,
                                         handleStr, handleLen);
    }
  else
    {
      char *inlineData = NULL;
      ExpLOBoper::extractFromLOBstring(objectUID, NULL, NULL, NULL, NULL,
                                       NULL, NULL, NULL, NULL, inlineData,
                                       handleStr, handleLen);      
    }

  return 0;
}

Lng32 ExpLOBoper::extractObjectUIDFromHandleString(char *objectUID,
                                                   NABoolean &lobV2,
                                                   char *handleStr,
                                                   Lng32 handleLen)
{
  // header and flags are at the same location for both V1 and V2
  LOBHandleV1ext *handleV1ext = (LOBHandleV1ext*)handleStr;
  LOBHandleV2ext *handleV2ext = (LOBHandleV2ext*)handleStr;
  
  if (memcmp(handleV1ext->header, "LOBH", 4) != 0)
    return -1;

  Int32 flags = (Lng32)str_atoi(handleV1ext->flags, 4);  
  lobV2 = isLOBHflagSet(flags, LOBH_VERSION2);

  if (lobV2)
    str_cpy_all(objectUID, handleV2ext->objectUID, 20); 
  else
    str_cpy_all(objectUID, handleV1ext->objectUID, 20); 

  objectUID[20] = 0;

  return 0;
}

Lng32 ExpLOBoper::genLOBhandleFromHandleString(char * lobHandleString,
					       Lng32 lobHandleStringLen,
					       char * lobHandle,
					       Lng32 &lobHandleLen)
{
  
  Int64 uid;
  Lng32 lobNum;
  Int32 lobType;
  Int64 descPartnKey;
  Int64 descSyskey;
  Int16 flags;
  short schNameLen = 0;
  char  schNameLenBuf[1024];
  Lng32 handleLen;
  Int32 inlineLength = 0;
  char *inlineData;

  if (extractFromLOBstring(&uid, &lobNum, &descPartnKey, &descSyskey, &flags,
			   &lobType, &schNameLen, schNameLenBuf, &inlineLength,
                           inlineData,
			   lobHandleString, lobHandleStringLen) < 0)
    return -1;

  char lLobHandle[4096];
  genLOBhandle(uid, lobNum, lobType, descPartnKey, descSyskey, flags,
               schNameLen, schNameLenBuf, handleLen, lLobHandle);
  if (handleLen > lobHandleLen)
    {
      return -1;
    }

  str_cpy_all(lobHandle, lLobHandle, handleLen);
  lobHandleLen = handleLen;

  return 0;
}

Lng32 ExpLOBoper::genLOBhandleFromHandleStringV2(char * lobHandleString,
                                                 Lng32 lobHandleStringLen,
                                                 char * lobHandle,
                                                 Lng32 &lobHandleLen)
{

  Int16 flags;  
  Int32 lobType;
  Lng32 lobNum;
  Int64 objectUID;
  Int64 lobUID;
  Int64 lobSeq;
  Lng32 handleLen;

  if (extractFromLOBstringV2(&flags, &lobType, &lobNum, &objectUID, &lobUID, 
                             &lobSeq, lobHandleString, lobHandleStringLen) < 0)
    return -1;
  
  char lLobHandle[4096];
  genLOBhandleV2(objectUID, lobNum, lobType, lobUID, lobSeq, flags,
                 handleLen, lLobHandle);
  if (handleLen > lobHandleLen)
    {
      return -1;
    }

  str_cpy_all(lobHandle, lLobHandle, handleLen);
  lobHandleLen = handleLen;

  return 0;
}

Long ExpLOBoper::pack(void * space)
{
  return packClause(space, sizeof(ExpLOBoper));
}

Lng32 ExpLOBoper::unpack (void * base, void * reallocator)
{
  return unpackClause(base, reallocator);
}

Lng32 ExpLOBoper::initClause()
{
  requestTag_ = -1;

  return 0;
}

///////////////////////////////////////////////////////////////////////
//
///////////////////////////////////////////////////////////////////////

Lng32 ExpLOBoper::checkLobOperStatus()
{
  Lng32 lobOperStatus = 0;

  if (requestTag_ == -1) // this clause has not been evaluated yet
    lobOperStatus = START_LOB_OPER_;   // start lob process
  else if (requestTag_ == -2) // this request has completed
    lobOperStatus = DO_NOTHING_; // do nothing
  else // a valid request tag
    lobOperStatus = CHECK_STATUS_; // check status for this clause
  
  return lobOperStatus;
}

////////////////////////////////////////////////////////
// ExpLOBiud
////////////////////////////////////////////////////////
ExpLOBiud::ExpLOBiud(){};
ExpLOBiud::ExpLOBiud(OperatorTypeEnum oper_type,
		     Lng32 numAttrs,
		     Attributes ** attr, 
		     Int64 objectUID,
		     short descSchNameLen,
		     char * descSchName,
		     Space * space)
  : ExpLOBoper(oper_type, numAttrs, attr, space),
    objectUID_(objectUID),
    liudFlags_(0)
{
  str_cpy_and_null(descSchName_, descSchName, descSchNameLen, 
		   '\0', ' ', TRUE);

  setDescSchNameLen(descSchNameLen);
};

////////////////////////////////////////////////////////
// ExpLOBinsert
////////////////////////////////////////////////////////
ExpLOBinsert::ExpLOBinsert(){};
ExpLOBinsert::ExpLOBinsert(OperatorTypeEnum oper_type,
			   Lng32 numAttrs,
			   Attributes ** attr,			   
			   Int64 objectUID,
			   short descSchNameLen,
			   char * descSchName,
			   Space * space)
  : ExpLOBiud(oper_type, numAttrs, attr, objectUID, descSchNameLen, descSchName, space),
    //    objectUID_(objectUID),
    //    descSchNameLen_(descSchNameLen),
    liFlags_(0)
{
};

void ExpLOBinsert::displayContents(Space * space, const char * displayStr, 
				   Int32 clauseNum, char * constsArea)

{
  ExpLOBoper::displayContents(space, "ExpLOBinsert", clauseNum, constsArea);

  char buf[100];

  str_sprintf(buf, "    liFlags_ = %d", liFlags_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));

  str_sprintf(buf, "    liudFlags_ = %d", liudFlags_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
}

ex_expr::exp_return_type ExpLOBiud::insertDesc(char *op_data[],
                                               char *lobData,
                                               Int64 lobLen,
                                               Int64 lobHdfsDataOffset,
                                               Int64 descTS,
                                               char *&result,
                                               CollHeap*h,
                                               ComDiagsArea** diagsArea)
{
  Lng32 rc=0; 
  Lng32 handleLen = 0;
  Int32 inlineDataLimit = getLobInlineLimit();
  char *lobHandleBuf = new (h) char[LOB_HANDLE_LEN+inlineDataLimit];
  if (descTS <= 0) //  timestamp not passed, generate one
    descTS = NA_JulianTimestamp();

  char * lobHandle = NULL; 
  lobHandle = lobHandleBuf;
  ExpLOBoper::genLOBhandle(objectUID_, lobNum(), (short)lobStorageType(),
                           -1, descTS, 0,
                           descSchNameLen_, descSchName(),
                           handleLen, lobHandle);
 

  // get the lob name where data need to be inserted
  if (lobNum() == -1)
    {
      Int32 intparam = LOB_PTR_ERROR;
      Int32 detailError = 0;
     
      ExRaiseSqlError(h, diagsArea, 
		      (ExeErrorCode)(8434));
      return ex_expr::EXPR_ERROR;
    }

  char tgtLobNameBuf[LOB_NAME_LEN];
  char *tgtLobName = NULL;
  tgtLobName = ExpGetLOBname(objectUID_, lobNum(),
                             descTS, getNumLOBdatafiles(),
                             tgtLobNameBuf, LOB_NAME_LEN);    
  if (tgtLobName == NULL)
    return ex_expr::EXPR_ERROR;

  // call function with the lobname and source value
  // to insert it in the LOB.
  // Get back offset and len of the LOB.
  Int64 descSyskey = 0;

  
  LobsSubOper so = Lob_None;
  if (fromFile())
    so = Lob_File;
  else if (fromString() || fromLoad())
    so = Lob_Memory;
  else if (fromLob())
    so = Lob_Lob;
  else if (fromLobExternal())
    so = Lob_External_Lob;
  else if (fromBuffer())
    so = Lob_Buffer;
  else if (fromExternal())
    so = Lob_External_File;
  else if (fromEmpty())
    {
      so = Lob_Memory;
    }

  Lng32 waitedOp = 0;
  waitedOp = 1;
  
  // until SQL_EXEC_LOBcliInterface is changed to allow for unlimited
  // black box sizes, we have to prevent over-sized file names from
  // being stored
  if ((so == Lob_External_File) && (lobLen > MAX_LOB_FILE_NAME_LEN))
    {
      ExRaiseSqlError(h, diagsArea, 
		      (ExeErrorCode)(8557));
      return ex_expr::EXPR_ERROR;
    }
  
  blackBoxLen_ = 0;
  if (fromExternal())
    {
      blackBoxLen_ = lobLen;
      str_cpy_and_null(blackBox_, lobData, (Lng32)blackBoxLen_,
		       '\0', ' ', TRUE);
    }

  Lng32 cliError = 0;
  LobsOper lo ;
 
  if (lobHandle == NULL)       
    lo = Lob_InsertDataSimple;
  else
    lo = Lob_InsertDesc;
  Int64 lobMaxSize = 0;
  if (getLobSize() > 0)
    {
      lobMaxSize = MINOF(getLobSize(), getLobMaxSize());
    }
  else
    lobMaxSize = getLobMaxSize();
    
  if ((so == Lob_Lob) || (so == Lob_External_Lob))
    {
      Lng32 srcLobType;
      Int64 srcUid = 0;
      Lng32 srcLobNum = 0;
      Int64 srcDescPartnKey = 0;
      Int64 srcDescSysKey = 0;
      Int64 srcDescTS = 0;
      Int16 srcFlags = 0;
      Int16 srcSchNameLen = 0;
      char srcSchName[1024];
      long srcHandleLen = 0;
      Int32 srcInlineDataLength = 0;
      char *srcInlineData = 0;
 
      ExpLOBoper::extractFromLOBhandle(&srcFlags, &srcLobType, &srcLobNum, &srcUid,
                                       &srcDescSysKey, &srcDescTS, 
                                       &srcSchNameLen, srcSchName,
                                       lobData);
      // First retrieve length of the lob pointed to by source (input handle) 
      if (srcLobType == Lob_Inline)
        {
          //lobData points to the source handle 
          Int32 inputLobDataLen = ExpLOBoper::lobHandleGetInlineDataLength(lobData);
          char      *inputData      = lobHandleGetInlineDataPosition(lobData)+sizeof(Int32);
          ExpLOBoper::lobHandleWriteData(inputLobDataLen, inputData, lobHandle);
          ExpLOBoper::updLOBhandleType(Lob_Inline,lobHandle);
          ExpLOBoper::updLOBhandleDescKey(0,lobHandle);//to indicate this is inline and not an empty lob
          handleLen = handleLen+sizeof(Int32)+inputLobDataLen;
          outHandleLen_ = 0;
        }
      else
        rc = ExpLOBInterfaceInsertSelect
          (getExeGlobals()->getExLobGlobal(), 
           (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
           getLobHdfsServer(), getLobHdfsPort(),
           tgtLobName, 
           so,
           lobStorageLocation(), srcLobStorageLocation(), lobStorageType(),
           -1,
           handleLen, lobHandle,  &outHandleLen_, outLobHandle_,            
           lobData, lobLen, 
           blackBox_, blackBoxLen_,lobMaxSize, getLobMaxChunkMemSize(),
           getLobGCLimit(),
           0,0,0,
           (areSrcMultiDatafiles() ? getSrcNumLOBdatafiles() : -1)
           );
    }

 else
    {
      rc = ExpLOBInterfaceInsert
        (getExeGlobals()->getExLobGlobal(), 
         (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
         tgtLobName, 
         lobStorageLocation(),
         lobStorageType(),
         getLobHdfsServer(), getLobHdfsPort(),
         
         handleLen, lobHandle,
         &outHandleLen_, outLobHandle_,
         blackBoxLen_, blackBox_,
         lobHdfsDataOffset,
         -1,
         descSyskey,
         lo,
         &cliError,
         so,
         waitedOp,
         lobData, lobLen, lobMaxSize, getLobMaxChunkMemSize(),getLobGCLimit());
    }
  if (rc == LOB_ACCESS_PREEMPT)
    {
      // save the handle so it could be used after return from preempt
      lobHandleLenSaved_ = handleLen;
      str_cpy_all(lobHandleSaved_, lobHandle, handleLen);
      
      return ex_expr::EXPR_PREEMPT;
    }

  if (rc < 0)
    {
      Lng32 intParam1 = -rc;
      ExRaiseSqlError(h, diagsArea, 
		      (ExeErrorCode)(8442), NULL, &intParam1, 
		      &cliError, NULL, (char*)"ExpLOBInterfaceInsert",
		      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
      return ex_expr::EXPR_ERROR;
    }

  // extract and update lob handle with the returned values
  if (outHandleLen_ > 0)
    {
      Int32 lobType = 0;
      ExpLOBoper::extractFromLOBhandle(NULL, &lobType, NULL, NULL, &descSyskey,
				       NULL, NULL, NULL, outLobHandle_);
      
      ExpLOBoper::updLOBhandleDescKey(descSyskey, lobHandle); 
      ExpLOBoper::updLOBhandleType(lobType,lobHandle);
    }
  
  str_cpy_all(result, lobHandle, handleLen);
  getOperand(0)->setVarLength(handleLen, op_data[-MAX_OPERANDS]);

  NADELETEBASIC(lobHandle,h);
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBiud::insertData(Lng32 handleLen,
                                               char *&handle,
                                               char *lobData,
                                               Int64 lobLen,
                                               Int64 descTS,
                                               Int32 numDataFiles,
                                               Int64 &hdfsDataOffset,
                                               CollHeap*h,
                                               ComDiagsArea** diagsArea)
{
  Lng32 rc = 0;

  Lng32 lobType;
  Int64 uid;
  Lng32 lobColNum;
  
 
  Int64 descSyskey = -1;
 
  short numChunks = 0;
 
  
   // get the lob name where data need to be inserted
  char tgtLobNameBuf[LOB_NAME_LEN];
  char *tgtLobName = NULL;
  tgtLobName = ExpGetLOBname(objectUID_, lobNum(),
                                       descTS, numDataFiles, 
                                       tgtLobNameBuf, LOB_NAME_LEN);
  if (tgtLobName == NULL)
    return ex_expr::EXPR_ERROR;
  
  if (fromExternal() || fromLob() || fromLobExternal())
    {
      //no need to insert any data. All data resides in the external file
      // for external LOB and it has already been read/inserted during 
      // ::insertDesc for insert from another  LOB
      return ex_expr::EXPR_OK;
    }
 
  LobsOper lo ;
 
  if (handle == NULL)       
    lo = Lob_InsertDataSimple;
  else
    lo = Lob_InsertData;

  LobsSubOper so = Lob_None;
  if (fromFile())    
    so = Lob_File;       
  else if (fromString() || fromLoad())
    so = Lob_Memory;
  else if (fromLob())
    so = Lob_Lob;
  else if(fromBuffer())
    so = Lob_Buffer;
  else if (fromExternal())
    so = Lob_External_File;

 
  Lng32 waitedOp = 0;
  waitedOp = 1;

  Lng32 cliError = 0;

  blackBoxLen_ = 0;
  
  rc = ExpLOBInterfaceInsert(getExeGlobals()->getExLobGlobal(),
                             (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
                             tgtLobName, 
                             lobStorageLocation(),
                             lobType,
                             getLobHdfsServer(), getLobHdfsPort(),
                             
                             handleLen, handle,
                             &outHandleLen_, outLobHandle_,
                             
                             blackBoxLen_, blackBox_,
                             
                             hdfsDataOffset,
                             -1,
                             
                             descSyskey, 
                             lo,
                             &cliError,
                             so,
                             waitedOp,				 
                             lobData,
                             lobLen,getLobMaxSize(),
                             getLobMaxChunkMemSize(),
                             getLobGCLimit());
  
  if (rc == LOB_ACCESS_PREEMPT)
    {
      return ex_expr::EXPR_PREEMPT;
    }

  if (rc < 0)
    {
      Lng32 intParam1 = -rc;
      if (intParam1 == LOB_DATA_WRITE_ERROR_RETRY)
        {
          ExRaiseSqlError(h, diagsArea, 
                          (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY), NULL, &intParam1, 
                          &cliError, NULL, (char*)"ExpLOBInterfaceInsert",
                          getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
          return ex_expr::EXPR_ERROR;
        }
      else
        {

          ExRaiseSqlError(h, diagsArea, 
                          (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                          &cliError, NULL, (char*)"ExpLOBInterfaceInsert",
                          getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
          return ex_expr::EXPR_ERROR;
        }
    }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBinsert::eval(char *op_data[],
					    CollHeap*h,
					    ComDiagsArea** diagsArea)
{
  if (lobV2())
    return evalV2(op_data, h, diagsArea);

  if (srcLobV2()) // insert into V1 from V2
    {
      char *result = NULL;
      return insertSelectV1fromV2(op_data, h, diagsArea);
    }

  char timeBuf[1024] = "";
  ex_expr::exp_return_type err;
  Int32 retcode = 0;
  Int32 cliError = 0;
  Int64 lobHdfsOffset = 0;  
  char * result = op_data[0];
  Int64 lobLen = 0; 
  char *lobData = NULL;
  Int64 chunkMemSize = 0;
  char *inputAddr = NULL;
  Int64 sourceFileSize = 0; // If the input is from a file get total file size
  Int64 sourceFileReadOffset = 0; 
  char *retBuf = 0;
  Int64 retReadLen = 0;
  Lng32 sLobType;
  Int64 sUid;
  Lng32 sLobNum;
  Int64 sDescSyskey = -1;
  Int64 sDescTS = -1;
  Int16 sFlags;
  short sSchNameLen = 0;
  char sSchName[500];
  char tgtLobNameBuf[LOB_NAME_LEN];
  char *tgtLobName = NULL;
  Int32 handleLen = 0;
  Int64 inlineDataLimit = getLobInlineLimit();
  if (lobNum() == -1)
    {
      Int32 intparam = LOB_PTR_ERROR;
      Int32 detailError = 0;
     
      ExRaiseSqlError(h, diagsArea, 
		      (ExeErrorCode)(8434));
      return ex_expr::EXPR_ERROR;
    }
     
  if (fromExternal())
    {
      Int32 fileNameLen = getOperand(1)->getLength(
           op_data[1] - getOperand(1)->getVCIndicatorLength());
      if (fileNameLen > MAX_LOB_FILE_NAME_LEN)
        {
          ExRaiseSqlError(h, diagsArea, 
                          (ExeErrorCode)(8557));
          return ex_expr::EXPR_ERROR;
        }
    }

  char llid[LOB_LOCK_ID_SIZE];
  if (lobLocking())
    {
      ExpLOBoper::genLobLockId(objectUID_,lobNum(),llid);;
      NABoolean found = FALSE;
      retcode = SQL_EXEC_CheckLobLock(llid, &found);
      if (! retcode && !found) 
        {    
          retcode = SQL_EXEC_SetLobLock(llid);
        }
      else if (found)
        {
          Int32 lobError = LOB_DATA_FILE_LOCK_ERROR;
          ExRaiseSqlError(h, diagsArea, 
                          (ExeErrorCode)(8558), NULL,(Int32 *)&lobError, 
                          NULL, NULL, (char*)"ExpLOBInterfaceInsert",
                          getLobErrStr(LOB_DATA_FILE_LOCK_ERROR),NULL);
          return ex_expr::EXPR_ERROR;
        }
    }
 
  getLobInputData(op_data,lobData,lobLen,h);
  err = getLobInputLength(op_data, lobLen,h,diagsArea);
  if (err != ex_expr::EXPR_OK)
    return err;
  Int64 lobMaxSize = 0;
  if (getLobSize() > 0)
    {
      lobMaxSize = MINOF(getLobSize(), getLobMaxSize());
        }
  else
    lobMaxSize = getLobMaxSize();

  if (lobLen > lobMaxSize)
    {
      Int32 lobError = LOB_MAX_LIMIT_ERROR;
      ExRaiseSqlError(h, diagsArea, 
                          (ExeErrorCode)(8442), NULL,(Int32 *)&lobError, 
                          NULL, NULL, (char*)"ExpLOBInterfaceInsert",
                          getLobErrStr(LOB_MAX_LIMIT_ERROR),NULL);
      return ex_expr::EXPR_ERROR;
    }
  long inputSize = lobLen;  
  inputAddr = lobData ;
 
  if ((inlineDataLimit !=0) && (lobLen <=  inlineDataLimit ) && !fromEmpty())
    {
      LOBInlineStatus retStatus;
      if (fromFile())
        {
          // Extract the data into memory. We already know it fits into 
          //the inline lob limit
          retcode= ExpLOBInterfaceReadSourceFile(getExeGlobals()->getExLobGlobal(),
                                                 lobData, 
                                                 getLobHdfsServer(),
                                                 getLobHdfsPort(),
                                                 0,
                                                 inputSize,
                                                 retBuf,
                                                 retReadLen);
          if (retcode < 0)
            {
              if (lobLocking())
                retcode = SQL_EXEC_ReleaseLobLock(llid);
              Lng32 intParam1 = -retcode;
              ExRaiseSqlError(h, diagsArea, 
                              (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                              &cliError, NULL, (char*)"ExpLOBInterfaceReadSourceFile",
                              getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
              return ex_expr::EXPR_ERROR;
            }

         
          inputAddr = retBuf;
        }
      retStatus= insertInlineData(op_data, inputAddr, inputSize,h,diagsArea);   
      if (retStatus == LOBInlineStatus::IL_ERROR)          
        return ex_expr::EXPR_ERROR;
      else
        return ex_expr::EXPR_OK;
    }


  chunkMemSize = MINOF(getLobMaxChunkMemSize(), inputSize);

  Int64 descTS = (areMultiDatafiles() ? NA_JulianTimestamp() : 0);
  Int32 numDataFiles = getNumLOBdatafiles();
  if(!fromEmpty())
    {
     
      if (fromFile())
        {
          //read a chunk of the file input data
          retcode= ExpLOBInterfaceReadSourceFile(getExeGlobals()->getExLobGlobal(),
                                                 lobData, 
                                                 getLobHdfsServer(),
                                                 getLobHdfsPort(),
                                                 sourceFileReadOffset,
                                                 chunkMemSize,
                                                 retBuf,
                                                 retReadLen);
          if (retcode < 0)
            {
              if (lobLocking())
                retcode = SQL_EXEC_ReleaseLobLock(llid);
              Lng32 intParam1 = -retcode;
              ExRaiseSqlError(h, diagsArea, 
                              (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                              &cliError, NULL, (char*)"ExpLOBInterfaceReadSourceFile",
                              getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
              return ex_expr::EXPR_ERROR;
            }

         
          inputAddr = retBuf;
                                               
        }
      char * handle = op_data[0];
      handleLen = getOperand(0)->getLength();
      err = insertData(handleLen, handle,inputAddr, chunkMemSize,
                       descTS, numDataFiles,
                       lobHdfsOffset, h, diagsArea);
      if (err == ex_expr::EXPR_ERROR)  
        {
          if (lobLocking())
            retcode = SQL_EXEC_ReleaseLobLock(llid);
          return err;      
        }
    }
  
  err = insertDesc(op_data,inputAddr, chunkMemSize,lobHdfsOffset, descTS,
                   result,h, diagsArea);
  if (err == ex_expr::EXPR_ERROR)  
    {
      if (lobLocking())
        retcode = SQL_EXEC_ReleaseLobLock(llid);
          return err;      
    }
  
  inputSize -= chunkMemSize;
  if (fromFile())
    sourceFileReadOffset +=chunkMemSize;       
  inputAddr += chunkMemSize;
  
  if(inputSize > 0)
    {
      char * lobHandle = op_data[0];
      handleLen = getOperand(0)->getLength();
      extractFromLOBhandle(&sFlags, &sLobType, &sLobNum, &sUid,
                           &sDescSyskey, &sDescTS, 
                           &sSchNameLen, sSchName,
                           lobHandle); 
     
       tgtLobName = ExpGetLOBname(sUid, sLobNum, tgtLobNameBuf, LOB_NAME_LEN);
    }
  while(inputSize > 0) // chunk rest of the input into lobMaxChunkMemSize chunks and append.
    {
      chunkMemSize = MINOF(getLobMaxChunkMemSize(), inputSize);
      if(!fromEmpty())
        {
     
          if (fromFile())
            {
              //read a chunk of the file input data
              retcode= ExpLOBInterfaceReadSourceFile(getExeGlobals()->getExLobGlobal(),
                                                     lobData, 
                                                     getLobHdfsServer(),
                                                     getLobHdfsPort(),
                                                     sourceFileReadOffset,
                                                     chunkMemSize,
                                                     retBuf,
                                                     retReadLen);
              if (retcode < 0)
                {
                  if (lobLocking())
                    retcode = SQL_EXEC_ReleaseLobLock(llid);
                  Lng32 intParam1 = -retcode;
                  ExRaiseSqlError(h, diagsArea, 
                              (ExeErrorCode)(8442), NULL, &intParam1, 
                              &cliError, NULL, (char*)"ExpLOBInterfaceReadSourceFile",
                              getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                  return ex_expr::EXPR_ERROR;
                }
              inputAddr = retBuf;
                                               
            }
          char * handle = op_data[0];
          handleLen = getOperand(0)->getLength();
            LobsSubOper so = Lob_None;
          if (fromFile())
            so = Lob_Memory; //It's already been read into memory above
          else if (fromString()) {
            if (getOperand(1)->getVCIndicatorLength() > 0)
              lobLen = getOperand(1)->getLength(op_data[1]-getOperand(1)->getVCIndicatorLength());
            so = Lob_Memory;
          }
          else if (fromLob())
            so = Lob_Lob;
          else if (fromBuffer())
            so= Lob_Buffer;
          else if (fromExternal())
          so = Lob_External_File;
         

          retcode = ExpLOBInterfaceUpdateAppend
            (getExeGlobals()->getExLobGlobal(), 
             (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
             getLobHdfsServer(),
             getLobHdfsPort(),
             tgtLobName, 
             lobStorageLocation(),
             handleLen, handle,
             &outHandleLen_, outLobHandle_,
             lobHdfsOffset,
             -1,	 
             0,
             0,
             so,
             sDescSyskey,
             chunkMemSize, 
             inputAddr,
             NULL, 0, NULL,
             -1, 0,
             getLobMaxSize(), getLobMaxChunkMemSize(),getLobGCLimit());
          if (retcode < 0)
            {
               if (lobLocking())
                 retcode = SQL_EXEC_ReleaseLobLock(llid);
                Lng32 intParam1 = -retcode;
               if (intParam1 == LOB_DATA_WRITE_ERROR_RETRY)
                 {
                   ExRaiseSqlError(h, diagsArea, 
                                   (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY), NULL, &intParam1, 
                                   &cliError, NULL, (char*)"ExpLOBInterfaceUpdateAppend",
                                   getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                   return ex_expr::EXPR_ERROR;
                 }
               else
                 {
                   ExRaiseSqlError(h, diagsArea, 
                                   (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                                   &cliError, NULL, (char*)"ExpLOBInterfaceUpdateAppend",
                                   getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                   return ex_expr::EXPR_ERROR;
                 }
            }
          inputSize -= chunkMemSize;
          if (fromFile())
            {
              sourceFileReadOffset +=chunkMemSize; 
              getExeGlobals()->getExLobGlobal()->getHeap()->deallocateMemory(inputAddr);
              
            }
                        
          inputAddr += chunkMemSize;
        }
    }

  if (lobLocking())
    retcode = SQL_EXEC_ReleaseLobLock(llid);
  return err;
}

////////////////////////////////////////////////////////
// ExpLOBdelete
////////////////////////////////////////////////////////
ExpLOBdelete::ExpLOBdelete(){};
ExpLOBdelete::ExpLOBdelete(OperatorTypeEnum oper_type,
			   Attributes ** attr, 
			   Space * space)
  : ExpLOBiud(oper_type, 2, attr, -1, 0, NULL, space),
    ldFlags_(0)
{
};

void ExpLOBdelete::displayContents(Space * space, const char * displayStr, 
				   Int32 clauseNum, char * constsArea)

{
  ExpLOBoper::displayContents(space, "ExpLOBdelete", clauseNum, constsArea);

}

ex_expr::exp_return_type ExpLOBdelete::eval(char *op_data[],
					    CollHeap*h,
					    ComDiagsArea** diagsArea)
{
  if (lobV2())
    return evalV2(op_data, h, diagsArea);

  Lng32 rc = 0;

  // null processing need to be done in eval
  if ((! isNullRelevant()) &&
      (getOperand(1)->getNullFlag()) && 
      (! op_data[-(2 * MAX_OPERANDS) + 1])) // missing value (is a null value)
    {
      // nothing to delete if lobHandle is null
      return ex_expr::EXPR_OK;
    }

  Lng32 lobOperStatus = checkLobOperStatus();
  if (lobOperStatus == DO_NOTHING_)
    return ex_expr::EXPR_OK;

  char * result = op_data[0];

  Lng32 lobType;
  Int64 uid;
  Lng32 lobNum;
  Int64 descSyskey;
  Int64 descTS = -1;
  extractFromLOBhandle(NULL, &lobType, &lobNum, &uid,
		       &descSyskey, NULL, //descTS, 
		       NULL, NULL,
		       op_data[1]);
  
  Lng32 handleLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS+1]);
  if (handleLen == 0)
   {
     ExRaiseSqlError(h, diagsArea, 
                     (ExeErrorCode)(8443), NULL, NULL);
     
     return ex_expr::EXPR_ERROR;
   }

  // get the lob name where data need to be deleted
  char lobNameBuf[LOB_NAME_LEN];
  char * lobName = ExpGetLOBname(uid, lobNum, lobNameBuf, LOB_NAME_LEN);
  if (lobName == NULL)
    return ex_expr::EXPR_ERROR;

  Lng32 cliError = 0;

  // call function with the lobname and offset to delete it.
  rc = ExpLOBInterfaceDelete
    (
     getExeGlobals()->getExLobGlobal(),
     (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
     getLobHdfsServer(),
     getLobHdfsPort(),
     lobName, 
     lobStorageLocation(),
     handleLen, op_data[1],
     requestTag_,
     -1,
     descSyskey,
     //     (getExeGlobals()->lobGlobals()->getCurrLobOperInProgress() ? 1 : 0),
     (lobOperStatus == CHECK_STATUS_ ? 1 : 0),
     0);
  
  if (rc == LOB_ACCESS_PREEMPT)
    {
      return ex_expr::EXPR_PREEMPT;
    }

  if (rc < 0)
    {
      Lng32 intParam1 = -rc;
      ExRaiseSqlError(h, diagsArea, 
		      (ExeErrorCode)(8442), NULL, &intParam1, 
		      &cliError, NULL, (char*)"ExpLOBInterfaceDelete",
		      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
      return ex_expr::EXPR_ERROR;
    }

  return ex_expr::EXPR_OK;
}

////////////////////////////////////////////////////////
// ExpLOBupdate
////////////////////////////////////////////////////////
ExpLOBupdate::ExpLOBupdate(){};
ExpLOBupdate::ExpLOBupdate(OperatorTypeEnum oper_type,
			   Lng32 numAttrs,
			   Attributes ** attr, 
			   Int64 objectUID,
			   short descSchNameLen,
			   char * descSchName,
			   Space * space)
  : ExpLOBiud(oper_type, numAttrs, attr, objectUID, 
	      descSchNameLen, descSchName, space),
    luFlags_(0),
    nullHandle_(0)
{
};

void ExpLOBupdate::displayContents(Space * space, const char * displayStr, 
				   Int32 clauseNum, char * constsArea)

{
  ExpLOBoper::displayContents(space, "ExpLOBupdate", clauseNum, constsArea);

  char buf[100];

  str_sprintf(buf, "    luFlags_ = %d", luFlags_);
  space->allocateAndCopyToAlignedSpace(buf, str_len(buf), sizeof(short));
}

ex_expr::exp_return_type 
ExpLOBupdate::processNulls(char *null_data[], CollHeap *heap,
			   ComDiagsArea **diagsArea
			   )
{
  if (lobV2())
    return processNullsV2(null_data, heap, diagsArea);

  nullHandle_ = 0;

  if ((getOperand(1)->getNullFlag()) &&    // nullable
      (! null_data[1]))                    // missing value 
    {
      ExpTupleDesc::setNullValue( null_data[0],
				  getOperand(0)->getNullBitIndex(),
				  getOperand(0)->getTupleFormat() );

      return ex_expr::EXPR_NULL;
    }
  
  if ((getOperand(2)->getNullFlag()) &&    // nullable
      (! null_data[2]))                    // missing value 
    {
      nullHandle_ = 1;
    }

  // move 0 to the null bytes of result
  if (getOperand(0)->getNullFlag())
    {
      ExpTupleDesc::clearNullValue( null_data[0],
                                    getOperand(0)->getNullBitIndex(),
                                    getOperand(0)->getTupleFormat() );
    }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type 
ExpLOBupdate::processNullsV2(char *null_data[], CollHeap *heap,
                             ComDiagsArea **diagsArea
                             )
{
  nullHandle_ = 0;

  if ((getOperand(2)->getNullFlag()) &&    // nullable
      (! null_data[2]))                    // missing value 
    {
      nullHandle_ = 1;
    }

  if ((getOperand(1)->getNullFlag()) &&    // nullable
      (! null_data[1]) &&                  // null/missing value 
      (nullHandle_))                         // no existing handle
    {
      ExpTupleDesc::setNullValue( null_data[0],
				  getOperand(0)->getNullBitIndex(),
				  getOperand(0)->getTupleFormat() );

      return ex_expr::EXPR_NULL;
    }
  
  // move 0 to the null bytes of result
  if (getOperand(0)->getNullFlag())
    {
      ExpTupleDesc::clearNullValue( null_data[0],
                                    getOperand(0)->getNullBitIndex(),
                                    getOperand(0)->getTupleFormat() );
    }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBupdate::eval(char *op_data[],
					    CollHeap*h,
					    ComDiagsArea** diagsArea)
{
  if (lobV2())
    return evalV2(op_data, h, diagsArea);

  Lng32 rc, retcode = 0;
  LOBInlineStatus retStatus;
  char * result = op_data[0];
  Int64 lobHdfsOffset = 0;  
  char * lobHandle = NULL;
  Lng32 handleLen = 0;
  char *inputAddr = NULL;
  Int64 lobLen = 0;
  char *lobData = NULL;
  Int64 sourceFileSize = 0; // If the input is from a file get total file size
  Int64 sourceFileReadOffset = 0; 
  Int64 chunkMemSize = 0;
  char *retBuf = 0;
  Int64 retReadLen = 0;
  Int32 cliError = 0;
  Lng32 sLobType;
  Int64 sUid;
  Lng32 sLobNum;
  Int64 sDescSyskey = -1;
  Int64 sDescTS = -1;
  Int16 sFlags;
  short sSchNameLen = 0;
  char sSchName[500];
  char tgtLobNameBuf[LOB_NAME_LEN];
  char *tgtLobName = NULL;
  Int64 inlineDataLimit = getLobInlineLimit();

  if (fromExternal())
    {
      Int32 fileNameLen = getOperand(1)->getLength(
           op_data[1] - getOperand(1)->getVCIndicatorLength());
      if (fileNameLen > MAX_LOB_FILE_NAME_LEN)
        {
          ExRaiseSqlError(h, diagsArea, 
                          (ExeErrorCode)(8557));
          return ex_expr::EXPR_ERROR;
        }
    }

  lobLen = getOperand(1)->getLength();
  char llid[LOB_LOCK_ID_SIZE];
  if (lobLocking())
    {
      ExpLOBoper::genLobLockId(objectUID_,lobNum(),llid);;
      NABoolean found = FALSE;
      retcode = SQL_EXEC_CheckLobLock(llid, &found);
      if (! retcode && !found) 
        {    
          retcode = SQL_EXEC_SetLobLock(llid);
        }
      else if (found)
        {
          Int32 lobError = LOB_DATA_FILE_LOCK_ERROR;
          ExRaiseSqlError(h, diagsArea, 
                          (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL,(Int32 *)&lobError, 
                          NULL, NULL, (char*)"LOB_LOCK",
                          getLobErrStr(LOB_DATA_FILE_LOCK_ERROR),NULL);
          return ex_expr::EXPR_ERROR;
        }
    }
 
 
  ex_expr::exp_return_type re;
  re = getLobInputLength(op_data,lobLen,h,diagsArea);
  if (re != ex_expr::EXPR_OK)
    return re;
  getLobInputData(op_data, lobData,lobLen,h);
  Int64 lobMaxSize = 0;
  if (getLobSize() > 0)
    {
      lobMaxSize = MINOF(getLobSize(), getLobMaxSize());
    }
  else
    lobMaxSize = getLobMaxSize();
  if (lobLen > lobMaxSize)
    {
      Int32 lobError = LOB_MAX_LIMIT_ERROR;
      ExRaiseSqlError(h, diagsArea, 
                          (ExeErrorCode)(8442), NULL,(Int32 *)&lobError, 
                          NULL, NULL, (char*)"ExpLOBInterfaceUpdate",
                          getLobErrStr(LOB_MAX_LIMIT_ERROR),NULL);
          return ex_expr::EXPR_ERROR;
    }
  long inputSize = lobLen;
  inputAddr = lobData ;
  
  if (getOperand(2)->getNullFlag() &&
      nullHandle_)
    {
     
       chunkMemSize = MINOF(getLobMaxChunkMemSize(), inputSize);
       lobHandle = op_data[0];
       handleLen = getOperand(0)->getLength();
       if (handleLen == 0)
         {
           ExRaiseSqlError(h, diagsArea, 
                           (ExeErrorCode)(8443), NULL, NULL);
          
           return ex_expr::EXPR_ERROR;
         }
       if(fromFile())
         {
           //read a chunk of the file input data
           retcode= ExpLOBInterfaceReadSourceFile(getExeGlobals()->getExLobGlobal(),
                                                  lobData, 
                                                  getLobHdfsServer(),
                                                  getLobHdfsPort(),
                                                  sourceFileReadOffset,
                                                  chunkMemSize,
                                                  retBuf,
                                                  retReadLen);
           if (retcode < 0)
             {
               if (lobLocking())
                 retcode = SQL_EXEC_ReleaseLobLock(llid);
               Lng32 intParam1 = -retcode;
               ExRaiseSqlError(h, diagsArea, 
                               (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                               &cliError, NULL, (char*)"ExpLOBInterfaceReadSourceFile",
                               getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
               return ex_expr::EXPR_ERROR;
             }
           inputAddr = retBuf;
                                               
         } // fromFile
       //If  can be inlined, insert inline data
      if ((inlineDataLimit !=0) && (inputSize <=  inlineDataLimit ) && !fromEmpty())
         {
           LOBInlineStatus retStatus;
           retStatus= insertInlineData(op_data, inputAddr, inputSize,h,diagsArea);
           if (retStatus == LOBInlineStatus::IL_ERROR)
             return ex_expr::EXPR_ERROR;              
           else
             return ex_expr::EXPR_OK;           
         }
      
       Int64 descTS = (areMultiDatafiles() ? NA_JulianTimestamp() : 0);
       Int32 numDataFiles = getNumLOBdatafiles();
       ex_expr::exp_return_type err = insertData(handleLen, lobHandle, inputAddr, chunkMemSize, 
                                                 descTS, numDataFiles,
                                                 lobHdfsOffset, h, diagsArea);;
       if (err == ex_expr::EXPR_ERROR)
         return err;
       err = insertDesc(op_data, inputAddr,chunkMemSize, lobHdfsOffset, descTS,
                        result,h, diagsArea);
       if (err == ex_expr::EXPR_ERROR)
         return err;

       inputSize -= chunkMemSize;
       if (fromFile())
         sourceFileReadOffset +=chunkMemSize;       
       inputAddr += chunkMemSize;
         
       if(inputSize > 0)
         {
           char * lobHandle = op_data[0];
           handleLen = getOperand(0)->getLength();
           extractFromLOBhandle(&sFlags, &sLobType, &sLobNum, &sUid,
                                &sDescSyskey, &sDescTS, 
                                &sSchNameLen, sSchName,
                                lobHandle); 
     
           tgtLobName = ExpGetLOBname(sUid, sLobNum, tgtLobNameBuf, LOB_NAME_LEN);
         }
       while(inputSize > 0) // chunk rest of the input into lobMaxChunkMemSize chunks and append.
        {
          chunkMemSize = MINOF(getLobMaxChunkMemSize(), inputSize);
          if(!fromEmpty())
            {
     
              if (fromFile())
                {
                  //read a chunk of the file input data
                  retcode= ExpLOBInterfaceReadSourceFile(getExeGlobals()->getExLobGlobal(),
                                                         lobData, 
                                                         getLobHdfsServer(),
                                                         getLobHdfsPort(),
                                                         sourceFileReadOffset,
                                                         chunkMemSize,
                                                         retBuf,
                                                         retReadLen);
                  if (retcode < 0)
                    {
                      if (lobLocking())
                        retcode = SQL_EXEC_ReleaseLobLock(llid);
                      Lng32 intParam1 = -retcode;
                      ExRaiseSqlError(h, diagsArea, 
                                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                                      &cliError, NULL, (char*)"ExpLOBInterfaceReadSourceFile",
                                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                      return ex_expr::EXPR_ERROR;
                    }
                  inputAddr = retBuf;
                                               
                }
              char * handle = op_data[0];
              handleLen = getOperand(0)->getLength();
              LobsSubOper so = Lob_None;
              if (fromFile())
                so = Lob_Memory; // It's already been read into memory above.
              else if (fromString()) {
                if (getOperand(1)->getVCIndicatorLength() > 0)
                  lobLen = getOperand(1)->getLength(op_data[1]-getOperand(1)->getVCIndicatorLength());
                so = Lob_Memory;
              }
              else if (fromLob())
                so = Lob_Lob;
              else if (fromBuffer())
                so= Lob_Buffer;
              else if (fromExternal())
              so = Lob_External_File;
                
              retcode = ExpLOBInterfaceUpdateAppend
                (getExeGlobals()->getExLobGlobal(), 
                 (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
                 getLobHdfsServer(),
                 getLobHdfsPort(),
                 tgtLobName, 
                 lobStorageLocation(),
                 handleLen, handle,
                 &outHandleLen_, outLobHandle_,
                 lobHdfsOffset,
                 -1,	 
                 0,
                 0,
                 so,
                 sDescSyskey,
                 chunkMemSize, 
                 inputAddr,
                 NULL, 0, NULL,
                 -1, 0,
                 getLobMaxSize(), getLobMaxChunkMemSize(),getLobGCLimit());
              if (retcode < 0)
                {
                  if (lobLocking())
                    retcode = SQL_EXEC_ReleaseLobLock(llid);
                  Lng32 intParam1 = -retcode;
                  if (intParam1 == LOB_DATA_WRITE_ERROR_RETRY)
                    {
                      ExRaiseSqlError(h, diagsArea, 
                                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY), NULL, &intParam1, 
                                      &cliError, NULL, (char*)"ExpLOBInterfaceUpdateAppend",
                                  getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                      return ex_expr::EXPR_ERROR;
                    }
                  else
                    {
                      ExRaiseSqlError(h, diagsArea, 
                                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                                      &cliError, NULL, (char*)"ExpLOBInterfaceUpdateAppend",
                                  getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                      return ex_expr::EXPR_ERROR;
                    }
                }
              inputSize -= chunkMemSize;
              if (fromFile())
                {
                  sourceFileReadOffset +=chunkMemSize;
                  getExeGlobals()->getExLobGlobal()->getHeap()->deallocateMemory(inputAddr);
                }     
              inputAddr += chunkMemSize;
            }
        }


    }
  else
    {
      lobHandle = op_data[2];

      handleLen = getOperand(2)->getLength(op_data[-MAX_OPERANDS+2]);
    
     
      extractFromLOBhandle(&sFlags, &sLobType, &sLobNum, &sUid,
                           &sDescSyskey, &sDescTS, 
                           &sSchNameLen, sSchName,
                           lobHandle); //op_data[2]);
      
  
      // get the lob name where data need to be updated
      char tgtLobNameBuf[LOB_NAME_LEN];
      char *tgtLobName = NULL;
      tgtLobName = ExpGetLOBname(sUid, sLobNum,
                                 sDescTS, getNumLOBdatafiles(),
                                 tgtLobNameBuf, LOB_NAME_LEN);    
      if (tgtLobName == NULL)
        return ex_expr::EXPR_ERROR;

      char fromLobNameBuf[LOB_NAME_LEN];
      char * fromLobName = NULL;
      Int64 fromDescKey = 0;
      Int64 fromDescTS = 0;
      short fromSchNameLen = 0;
      char  fromSchName[ComAnsiNamePart::MAX_IDENTIFIER_EXT_LEN+1];
 
      LobsSubOper so = Lob_None;
      if (fromFile())
        so = Lob_File;
      else if (fromString()) {
        so = Lob_Memory;
      }
      else if (fromLob())
        so = Lob_Lob;
      else if (fromBuffer())
        so= Lob_Buffer;
      else if (fromExternal())
        so = Lob_External_File;
 
      Int64 lobMaxSize = 0;
      if (getLobSize() > 0)
        {
          lobMaxSize = MINOF(getLobSize(), getLobMaxSize());
        }
      else
        lobMaxSize = getLobMaxSize();
      Lng32 waitedOp = 0;
      waitedOp = 1;

      Lng32 cliError = 0;

      if(fromEmpty())
        {
          inputSize = lobLen = 0;
          so = Lob_Memory;
        }
          
      if (isAppend() && !fromEmpty())
        {
          chunkMemSize = MINOF(getLobMaxChunkMemSize(), inputSize);
          while(inputSize >0)
            {
              if(fromFile())
                {
                  //read a chunk of the file input data
                  retcode= ExpLOBInterfaceReadSourceFile(getExeGlobals()->getExLobGlobal(),
                                                         lobData, 
                                                         getLobHdfsServer(),
                                                         getLobHdfsPort(),
                                                         sourceFileReadOffset,
                                                         chunkMemSize,
                                                         retBuf,
                                                         retReadLen);
                  if (retcode < 0)
                    {
                      if (lobLocking())
                        retcode = SQL_EXEC_ReleaseLobLock(llid);
                      Lng32 intParam1 = -retcode;
                      ExRaiseSqlError(h, diagsArea, 
                                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                                      &cliError, NULL, (char*)"ExpLOBInterfaceReadSourceFile",
                                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                      return ex_expr::EXPR_ERROR;
                    }

                  so = Lob_Memory; // It's already been read into memory above.
                  inputAddr = retBuf;
                  chunkMemSize = retReadLen;
                                               
                } // fromFile
              if (sLobType == Lob_Inline)
                {
                  if (lobLen <inlineDataLimit )
                    {
                      Int32 currLen = lobHandleGetInlineDataLength(lobHandle);
                      if (currLen+lobLen > lobMaxSize)
                        {
                          Int32 lobError = LOB_MAX_LIMIT_ERROR;
                          ExRaiseSqlError(h, diagsArea, 
                                          (ExeErrorCode)(8442), NULL,(Int32 *)&lobError, 
                          NULL, NULL, (char*)"ExpLOBInterfaceInsert",
                                          getLobErrStr(LOB_MAX_LIMIT_ERROR),NULL);
                          return ex_expr::EXPR_ERROR;
                        }
                      retStatus = appendInlineData(op_data,inputAddr,chunkMemSize,diagsArea);
                      if (retStatus == LOBInlineStatus::IL_ERROR)          
                        return ex_expr::EXPR_ERROR;
                      else
                        if (retStatus == LOBInlineStatus::IL_TRANSITION_TO_OL)
                          {
                            //transition to outline
                          
                            // This updates the lobHandle(op_data[2])
                            re = moveInlineToOutlineData(op_data,h,diagsArea);
                            if (re == ex_expr::EXPR_ERROR)
                              return re;
                            //continue appending the new data. 
                            lobHandle = op_data[0];
                            handleLen = getOperand(0)->getLength(op_data[-MAX_OPERANDS]);
                          }
                        else
                          return ex_expr::EXPR_OK;
                    }
                  else 
                    {
                      //transition to outline
                      // This updates the lobHandle(op_data[0])
                      re = moveInlineToOutlineData(op_data,h,diagsArea);
                      lobHandle = op_data[0];
                      handleLen = getOperand(0)->getLength(op_data[-MAX_OPERANDS]);
                      // continue appending the new data
                      if (re == ex_expr::EXPR_ERROR)
                        return re;
                    }
                }
            
              rc = ExpLOBInterfaceUpdateAppend
                (getExeGlobals()->getExLobGlobal(), 
                 (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
                 getLobHdfsServer(),
                 getLobHdfsPort(),
                 tgtLobName, 
                 lobStorageLocation(),
                 handleLen, lobHandle,
                 &outHandleLen_, outLobHandle_,
                 requestTag_,
                 -1,
	 
                 0,
                 waitedOp,
                 so,
                 sDescSyskey,
                 chunkMemSize, 
                 inputAddr,
                 fromLobName, fromSchNameLen, fromSchName,
                 fromDescKey, fromDescTS,
                 lobMaxSize, getLobMaxChunkMemSize(),getLobGCLimit());
            
              if (retcode < 0)
                {
                  if (lobLocking())
                    retcode = SQL_EXEC_ReleaseLobLock(llid);
                  Lng32 intParam1 = -retcode;
                  if (intParam1 == LOB_DATA_WRITE_ERROR_RETRY)
                    {
                      ExRaiseSqlError(h, diagsArea, 
                                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY), NULL, &intParam1, 
                                      &cliError, NULL, (char*)"ExpLOBInterfaceUpdateAppend",
                                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                      return ex_expr::EXPR_ERROR;
                    }
                  else
                    {
                      ExRaiseSqlError(h, diagsArea, 
                                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                                      &cliError, NULL, (char*)"ExpLOBInterfaceUpdateAppend",
                                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                      return ex_expr::EXPR_ERROR;
                    }
                }
              inputSize -= chunkMemSize;
              if (fromFile())
                {
                  sourceFileReadOffset +=chunkMemSize;
                  getExeGlobals()->getExLobGlobal()->getHeap()->deallocateMemory(inputAddr);
                }     
              inputAddr += chunkMemSize;
            }//while
        }
      else
        {
          chunkMemSize = MINOF(getLobMaxChunkMemSize(), inputSize);
        
          if(fromFile())
            {
              //read a chunk of the file input data
              retcode= ExpLOBInterfaceReadSourceFile(getExeGlobals()->getExLobGlobal(),
                                                     lobData, 
                                                     getLobHdfsServer(),
                                                     getLobHdfsPort(),
                                                     sourceFileReadOffset,
                                                     chunkMemSize,
                                                     retBuf,
                                                     retReadLen);
              if (retcode < 0)
                {
                  if (lobLocking())
                    retcode = SQL_EXEC_ReleaseLobLock(llid);
                  Lng32 intParam1 = -retcode;
                  ExRaiseSqlError(h, diagsArea, 
                                  (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                                  &cliError, NULL, (char*)"ExpLOBInterfaceReadSourceFile",
                                  getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                  return ex_expr::EXPR_ERROR;
                }

              so = Lob_Memory; // It's already been read into memory above.
              inputAddr = retBuf;
              chunkMemSize = retReadLen;
                                               
            } // fromFile
          if (sLobType == Lob_Inline)
            {
              //See if the data can be updated inline
              if ((inlineDataLimit !=0) && !fromEmpty())
                {
                  //See if the new length can be updated inline
             
                  if (inputSize <inlineDataLimit )
                    {
                      retStatus = updateInlineData(op_data,inputAddr,chunkMemSize,diagsArea);
                      if (retStatus == LOBInlineStatus::IL_ERROR)          
                        return ex_expr::EXPR_ERROR;
                      else
                        return ex_expr::EXPR_OK;
                   
                    }
                
                  else
                    {
                      //transition to outline. 
                      // This updates the lobHandle(op_data[0])
                      re = updateInlineToOutlineData(op_data,h,diagsArea);
                      if (re == ex_expr::EXPR_ERROR)
                        return re;
                       lobHandle = op_data[0];
                       handleLen = getOperand(0)->getLength(op_data[-MAX_OPERANDS]);
                    }
                }
              else if (fromEmpty())
                {
                  //update the handle 
                  char *sourceData    = lobHandleGetInlineDataPosition(lobHandle);
                  //get curr len
                  Int32 currLen = *((Int32 *)sourceData);
                  handleLen = handleLen - currLen -sizeof(Int32);
                  updLOBhandleType( Lob_Outline,lobHandle);
                }
            }
          else
            {
              // If data is outline , see if it can be inlined now
               //See if the data can be updated inline
              if ((inlineDataLimit !=0) && !fromEmpty())
                {
                  //See if the new length can be updated inline
                   if (inputSize <inlineDataLimit )
                     {
                       re = updateOutlineToInlineData(op_data, 
                                                      inputAddr,
                                                      inputSize,h, diagsArea);
    
                       lobHandle = op_data[0];
                       handleLen = getOperand(0)->getLength(op_data[-MAX_OPERANDS]);
                       return re;
                     }
                }
        
            }
              
          rc = ExpLOBInterfaceUpdate
            (getExeGlobals()->getExLobGlobal(), 
             (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
             getLobHdfsServer(),
             getLobHdfsPort(),
             tgtLobName, 
             lobStorageLocation(),
             handleLen, lobHandle,
             &outHandleLen_, outLobHandle_,
             requestTag_,
             -1,
	 
             0,
             waitedOp,
             so,
             sDescSyskey,
             chunkMemSize, 
             inputAddr,
             fromLobName, fromSchNameLen, fromSchName,
             fromDescKey, fromDescTS,
             lobMaxSize, getLobMaxChunkMemSize(),getLobGCLimit());
         
          if (lobLocking())
            retcode = SQL_EXEC_ReleaseLobLock(llid);


          if (rc  < 0)
            {
              Lng32 intParam1 = -rc;
              if (intParam1 == LOB_DATA_WRITE_ERROR_RETRY)
                {
                  ExRaiseSqlError(h, diagsArea, 
                                  (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY), NULL, &intParam1, 
                                  &cliError, NULL, (char*)"ExpLOBInterfaceUpdate",
                                  getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                  return ex_expr::EXPR_ERROR; 
                }
              else
                {
                  ExRaiseSqlError(h, diagsArea, 
                                  (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                                  &cliError, NULL, (char*)"ExpLOBInterfaceUpdate",
                                  getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                  return ex_expr::EXPR_ERROR;
                }
            }
          inputSize -= chunkMemSize;
          if (fromFile())
            sourceFileReadOffset +=chunkMemSize;       
          inputAddr += chunkMemSize;
         
          while(inputSize > 0) // chunk rest of the input into lobMaxChunkMemSize chunks and append.
            {
              chunkMemSize = MINOF(getLobMaxChunkMemSize(), inputSize);  
              if (fromFile())
                {
                  //read a chunk of the file input data
                  retcode= ExpLOBInterfaceReadSourceFile(getExeGlobals()->getExLobGlobal(),
                                                         lobData, 
                                                         getLobHdfsServer(),
                                                         getLobHdfsPort(),
                                                         sourceFileReadOffset,
                                                         chunkMemSize,
                                                         retBuf,
                                                         retReadLen);
                  if (retcode < 0)
                    {
                      if (lobLocking())
                        retcode = SQL_EXEC_ReleaseLobLock(llid);
                      Lng32 intParam1 = -retcode;
                      ExRaiseSqlError(h, diagsArea, 
                                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                                      &cliError, NULL, (char*)"ExpLOBInterfaceReadSourceFile",
                                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                      return ex_expr::EXPR_ERROR;
                    }
                  inputAddr = retBuf;
                                               
                }
              /* char * handle = op_data[0];
                 handleLen = getOperand(0)->getLength();*/
              LobsSubOper so = Lob_None;
              if (fromFile())
                so = Lob_Memory; // It's already been read into memory above.
              else if (fromString()) {
                if (getOperand(1)->getVCIndicatorLength() > 0)
                  lobLen = getOperand(1)->getLength(op_data[1]-getOperand(1)->getVCIndicatorLength());
                so = Lob_Memory;
              }
              else if (fromLob())
                so = Lob_Lob;
              else if (fromBuffer())
                so= Lob_Buffer;
              else if (fromExternal())
                so = Lob_External_File;
                
              retcode = ExpLOBInterfaceUpdateAppend
                (getExeGlobals()->getExLobGlobal(), 
                 (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
                 getLobHdfsServer(),
                 getLobHdfsPort(),
                 tgtLobName, 
                 lobStorageLocation(),
                 handleLen, lobHandle,
                 &outHandleLen_, outLobHandle_,
                 lobHdfsOffset,
                 -1,	 
                 0,
                 0,
                 so,
                 sDescSyskey,
                 chunkMemSize, 
                 inputAddr,
                 NULL, 0, NULL,
                 -1, 0,
                 getLobMaxSize(), getLobMaxChunkMemSize(),getLobGCLimit());
              if (retcode < 0)
                {
                  if (lobLocking())
                    retcode = SQL_EXEC_ReleaseLobLock(llid);
                  Lng32 intParam1 = -retcode;
                  if (intParam1 == LOB_DATA_WRITE_ERROR_RETRY)
                    {
                      ExRaiseSqlError(h, diagsArea, 
                                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY), NULL, &intParam1, 
                                      &cliError, NULL, (char*)"ExpLOBInterfaceUpdateAppend",
                                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                      return ex_expr::EXPR_ERROR;
                    }
                  else
                    {
                      ExRaiseSqlError(h, diagsArea, 
                                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                                      &cliError, NULL, (char*)"ExpLOBInterfaceUpdateAppend",
                                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
                      return ex_expr::EXPR_ERROR;
                    }
                }
              inputSize -= chunkMemSize;
              if (fromFile())
                {
                  sourceFileReadOffset +=chunkMemSize;
                  getExeGlobals()->getExLobGlobal()->getHeap()->deallocateMemory(inputAddr);
                }     
              inputAddr += chunkMemSize;
            }//while
        }

    }    
        
        
  // update lob handle with the returned values
  str_cpy_all(result, lobHandle, handleLen);
 
  getOperand(0)->setVarLength(handleLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}


////////////////////////////////////////////////////////
// ExpLOBselect
////////////////////////////////////////////////////////
ExpLOBselect::ExpLOBselect(){};
ExpLOBselect::ExpLOBselect(OperatorTypeEnum oper_type,
			   Attributes ** attr, 
			   Space * space)
  : ExpLOBoper(oper_type, 2, attr, space),
    lsFlags_(0)
   
{
  tgtLocation_[0] = 0;
  tgtFile_[0] = 0;
};
void ExpLOBoper::lobHandleWriteData(Int64  dataLen,
                                    char  *data,
                                    char  *handlePtr)
{
  if (handlePtr == NULL)
      return;
  char      *dest      = lobHandleGetInlineDataPosition(handlePtr);
  Lng32      lobLength = (Lng32)dataLen;
  str_cpy_all(dest, (const char*)&lobLength, sizeof(Lng32));
  if (data != NULL && lobLength > 0)
    str_cpy_all(dest + sizeof(Int32), data, lobLength);
}

void ExpLOBoper::lobHandleReadData(Int32 dataLen,
                                   char  *data,
                                   char  *handlePtr,
                                   Int32 &readLen)
{
  if (handlePtr == NULL)
      return;
  char      *source    = lobHandleGetInlineDataPosition(handlePtr);
  Int32 lobDataLen = *((Int32 *)source);
  readLen = MINOF(dataLen,lobDataLen);
   
  str_cpy_all(data,source+sizeof(Int32),readLen);
}
Int32 ExpLOBoper::lobHandleGetInlineDataLength(char *handlePtr)
{
  Int32      lobLength = 0;

  LOBHandle *lobHandle = (LOBHandle*)handlePtr;
  LOBHandleV2 *lobHandleV2 = (LOBHandleV2*)handlePtr;
  if (isLOBHflagSet(lobHandle->flags_, LOBH_VERSION2))
    lobLength = lobHandleV2->inlineDataLen_;
  else
    {
      char      *source    = lobHandleGetInlineDataPosition(handlePtr);
      str_cpy_all((char *)&lobLength, source, sizeof(Lng32));
    }

  return lobLength;
}
Int32 ExpLOBoper::lobHandleGetLobType(char  *handlePtr)
{
    LOBHandle *lobHandle = (LOBHandle*)handlePtr;
    return lobHandle->lobType_;
}

ex_expr::exp_return_type ExpLOBiud::getLobInputLength(char *op_data[], Int64 &lobLen,
                                                      CollHeap *h,ComDiagsArea **diagsArea)
{
  Int32 retcode = 0;
  char *sourceFileName = NULL;
  Int64 sourceFileSize = 0;
  if (!fromEmpty())
    {
      lobLen = getOperand(1)->getLength();
      //If source is a varchar, find the actual length
      if (fromString() && ((getOperand(1)->getVCIndicatorLength() >0)))
        lobLen = getOperand(1)->getLength(op_data[1]- 
                                          getOperand(1)->getVCIndicatorLength());
    }

  if(fromFile())
    {
      lobLen = getOperand(1)->getLength(op_data[1]- 
                                          getOperand(1)->getVCIndicatorLength());
      sourceFileName = new (h) char[lobLen+1];  
      str_cpy_and_null(sourceFileName,op_data[1],lobLen,'\0',' ',TRUE);
      retcode = ExpLOBInterfaceGetFileSize(getExeGlobals()->getExLobGlobal(), 
                                           sourceFileName, 
                                           getLobHdfsServer(),
                                           getLobHdfsPort(),
                                           sourceFileSize);
      if (retcode < 0)
        {
          Lng32 intParam1 = -retcode;
          ExRaiseSqlError(h, diagsArea, 
                          (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), NULL, &intParam1, 
                          0, NULL, (char*)"ExpLOBInterfaceGetFileSize",
                          getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
          return ex_expr::EXPR_ERROR;
        }
      lobLen = sourceFileSize;
    }

   if (fromBuffer())   
      memcpy(&lobLen, op_data[2],sizeof(Int64)); // user specified buffer length
    
   return ex_expr::EXPR_OK;
}

void ExpLOBiud::getLobInputData(char *op_data[], char *&lobData,Int64 lobLen,CollHeap *h)
{
   if(fromFile())
    {
      lobLen = getOperand(1)->getLength(op_data[1]- 
                                          getOperand(1)->getVCIndicatorLength());
      //lobLen = getOperand(1)->getLength();
      lobData = new (h) char[lobLen+1];  
      str_cpy_and_null(lobData,op_data[1],lobLen,'\0',' ',TRUE);
    }
  else if (fromBuffer())
    {
      Int64 userBufAddr = 0;
      memcpy(&userBufAddr,op_data[1],sizeof(Int64));
      lobData = (char *)userBufAddr;
    }
  else
     lobData = op_data[1];
}                                    
LOBInline_status ExpLOBiud::insertInlineData(char *op_data[],
                                             char *lobData,
                                             Int64 lobLen,
                                             CollHeap *h,
                                             ComDiagsArea **diagsArea)
{
  Int32 lobType = Lob_Inline;
  Int64  descTS = NA_JulianTimestamp();
  Int64  descSyskey = 0;
  Lng32  handleLen = 0;
  Int32 rc = 0;
  Int32 cliError =0 ;
  Int64 lobHdfsDataOffset = 0;
  Int64 lobMaxSize = getLobMaxSize();
  char   tgtLobNameBuf[LOB_NAME_LEN];
  char  *tgtLobName = ExpGetLOBname(objectUID_, lobNum(), tgtLobNameBuf, 
                                    LOB_NAME_LEN);
  
 
  if (tgtLobName == NULL)
     return LOBInlineStatus::IL_ERROR;
  
   char  *lobHandle = op_data[0];
   ExpLOBoper::genLOBhandle(objectUID_, lobNum(), lobType,
                           descSyskey, descTS, 0,
                           descSchNameLen_, descSchName(),
                           handleLen, lobHandle);
  
  
   ExpLOBoper::lobHandleWriteData(lobLen, lobData, lobHandle);
 
   getOperand(0)->setVarLength(handleLen + sizeof(Lng32) + lobLen, 
                              op_data[-MAX_OPERANDS]);
   return LOBInlineStatus::IL_SUCCESS;
}
LOBInline_status ExpLOBiud::updateInlineData(char *op_data[],
                                             char *lobData,
                                             Int64 lobLen,
                                             ComDiagsArea **diagsArea)
{
  
  char * result = op_data[0];
  char *lobHandle = op_data[2];
  Int32 handleLen = getOperand(2)->getLength(op_data[-MAX_OPERANDS+2]);
  char      *target    = lobHandleGetInlineDataPosition(lobHandle);
  //save old len
  Int32 currLen = *((Int32 *)target);
  //write the new length
  str_cpy_all(target, (char *)&lobLen, sizeof(Int32));
  str_cpy_all((char *)(target+sizeof(Int32)), lobData,lobLen);

  Int32 newHandleLen = handleLen-currLen+lobLen;
  // update lob handle with the returned values
  str_cpy_all(result, lobHandle, newHandleLen);
 
  getOperand(0)->setVarLength(newHandleLen, 
                              op_data[-MAX_OPERANDS]);
  return LOBInlineStatus::IL_SUCCESS;
}

LOBInline_status ExpLOBiud::appendInlineData(char *op_data[],
                                             char *lobData,
                                             Int64 lobLen,
                                             ComDiagsArea **diagsArea)
{
  char * result = op_data[0];
  char *lobHandle = op_data[2];
  Int32 handleLen = getOperand(2)->getLength(op_data[-MAX_OPERANDS+2]);
  Int64 inlineDataLimit = getLobInlineLimit();
  char      *target    = lobHandleGetInlineDataPosition(lobHandle);
  //get old len
  Int32 currLen = *((Int32 *)target);
  Int32 newDataLen = currLen+lobLen;

  if (newDataLen < inlineDataLimit)
    {
      //write the new length
      str_cpy_all(target, (char *)&newDataLen, sizeof(Int32));
      str_cpy_all((char *)(target+sizeof(Int32)+currLen), lobData,lobLen);

      Int32 newHandleLen = handleLen+lobLen;
      // update lob handle with the returned values
      str_cpy_all(result, lobHandle, newHandleLen);
      getOperand(0)->setVarLength(newHandleLen, 
                                  op_data[-MAX_OPERANDS]);
    }
  else
    return LOBInlineStatus::IL_TRANSITION_TO_OL;
  return LOBInlineStatus::IL_SUCCESS;
}


ex_expr::exp_return_type ExpLOBiud::moveInlineToOutlineData(char *op_data[],CollHeap *h, ComDiagsArea **diagsArea )
{
  char *result = op_data[0];

  char *inlineLobHandle = op_data[2];
  Int32 inlineHandleLen = getOperand(2)->getLength(op_data[-MAX_OPERANDS+2]);
  char *sourceData    = lobHandleGetInlineDataPosition(inlineLobHandle);
  //get curr len
  Int32 currLen = *((Int32 *)sourceData);
  char *inlineLobData = sourceData+sizeof(Int32);
  Int64 lobHdfsOffset = 0;

  Int64 descTS = (areMultiDatafiles() ? NA_JulianTimestamp() : 0);
  Int32 numDataFiles = getNumLOBdatafiles();
  Int32 outlineHandleLen = inlineHandleLen-currLen-sizeof(Int32);
  char *outlineLobHandle = op_data[2];
  updLOBhandleType( Lob_Outline,outlineLobHandle);
  //save current source type flags
  Int32 currentIUDflags = liudFlags_;
  
  setFromString(TRUE);
  ex_expr::exp_return_type err = insertData(outlineHandleLen, outlineLobHandle, 
                                            inlineLobData, currLen, 
                                            descTS, numDataFiles,
                                            lobHdfsOffset, h, diagsArea);;
  if (err == ex_expr::EXPR_ERROR)
    return err;
  err = insertDesc(op_data, inlineLobData,currLen, lobHdfsOffset, descTS,
                   result,h, diagsArea);
  if (err == ex_expr::EXPR_ERROR)
    return err;
  //restore flags
  liudFlags_ = currentIUDflags;
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBiud::updateInlineToOutlineData(char *op_data[],CollHeap *h, ComDiagsArea **diagsArea )
{
  char *result = op_data[0];
  Int64 lobHdfsOffset = 0;
  Int64 descTS = (areMultiDatafiles() ? NA_JulianTimestamp() : 0);

  ex_expr::exp_return_type err =  insertDesc(op_data, 0,0, lobHdfsOffset, descTS,
                   result,h, diagsArea);
  if (err == ex_expr::EXPR_ERROR)
    return err;
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBiud::updateOutlineToInlineData(char *op_data[],char *data, Int32 lobLen,CollHeap *h, ComDiagsArea **diagsArea )
{
  char *result = op_data[0];
  Int32 rc = 0;
  char *outlineLOBHandle = op_data[2];

  Lng32 lobType;
  Int64 uid;
  Lng32 lobNum;
  Int64 descSyskey;
  Int64 descTS = -1;
  extractFromLOBhandle(NULL, &lobType, &lobNum, &uid,
		       &descSyskey,NULL, 
		       NULL, NULL,
		       op_data[2]);
  
  Lng32 handleLen = getOperand(2)->getLength(op_data[-MAX_OPERANDS+2]);
  if (handleLen == 0)
   {
     ExRaiseSqlError(h, diagsArea, 
                     (ExeErrorCode)(8443), NULL, NULL);
     
     return ex_expr::EXPR_ERROR;
   }
  // get the lob name where data need to be deleted
  char lobNameBuf[LOB_NAME_LEN];
  char * lobName = ExpGetLOBname(uid, lobNum, lobNameBuf, LOB_NAME_LEN);
  if (lobName == NULL)
    return ex_expr::EXPR_ERROR;


  Lng32 cliError = 0;
  rc = ExpLOBInterfaceDelete( getExeGlobals()->getExLobGlobal(),
                                   (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
                                   getLobHdfsServer(),
                                   getLobHdfsPort(),
                                   lobName, 
                                   lobStorageLocation(),
                                   handleLen, op_data[2],
                                   requestTag_,
                                   -1,
                                   descSyskey,
                                   0,
                                   0);

  if (rc < 0)
    {
      Lng32 intParam1 = -rc;
      ExRaiseSqlError(h, diagsArea, 
		      (ExeErrorCode)(8442), NULL, &intParam1, 
		      &cliError, NULL, (char*)"ExpLOBInterfaceDelete",
		      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
      return ex_expr::EXPR_ERROR;
    }
  
  //Update the lob type to inline and update the data inline
  
  updLOBhandleType( Lob_Inline,outlineLOBHandle);
  Int32 outlineHandleLen = getOperand(2)->getLength(op_data[-MAX_OPERANDS+2]);
  
  //inline the data
  str_cpy_all((char *)(outlineLOBHandle+outlineHandleLen+sizeof(Int32)),data,lobLen);
  str_cpy_all((char *)(outlineLOBHandle+outlineHandleLen),(char *)&lobLen,sizeof(Int32));
  Int32 newHandleLen = sizeof(Int32)+outlineHandleLen+lobLen;
  str_cpy_all(result,outlineLOBHandle,newHandleLen);
  getOperand(0)->setVarLength(newHandleLen, 
                              op_data[-MAX_OPERANDS]);
  return ex_expr::EXPR_OK;
}

void ExpLOBselect::displayContents(Space * space, const char * displayStr, 
				   Int32 clauseNum, char * constsArea)

{
  ExpLOBoper::displayContents(space, "ExpLOBselect", clauseNum, constsArea); 
}



ex_expr::exp_return_type ExpLOBselect::eval(char *op_data[
],
					    CollHeap*h,
					    ComDiagsArea** diagsArea)
{
  char * result = op_data[0];
  Lng32 rc = 0;
  Int64 uid;
  Lng32 lobType;
  Lng32 lobNum;
  Int64 descKey;
  Int16 flags;
  Int64 descTS = -1;
  short schNameLen = 0;
  char  schName[500];
  LobsSubOper so;
  Lng32 waitedOp = 0;
  Int64 lobLen = 0; 
  char *lobData = NULL;
  Lng32 cliError = 0;
  Lng32 lobOperStatus = checkLobOperStatus();
 
  Int32 handleLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS+1]);
  if (handleLen == 0)
   {
     ExRaiseSqlError(h, diagsArea, 
                     (ExeErrorCode)(8443), NULL, NULL);
     
     return ex_expr::EXPR_ERROR;
   }

  char * lobHandle = op_data[1];
  extractFromLOBhandle(&flags, &lobType, &lobNum, &uid,
		       &descKey, &descTS, 
		       &schNameLen, schName,
		       lobHandle);
  // select returns lobhandle only
  str_pad(result, getOperand(0)->getLength());
  str_cpy_all(result, lobHandle, strlen(lobHandle));
  
  return ex_expr::EXPR_OK;
}

////////////////////////////////////////////////////////
// ExpLOBconvert
////////////////////////////////////////////////////////
ExpLOBconvert::ExpLOBconvert(){};
ExpLOBconvert::ExpLOBconvert(OperatorTypeEnum oper_type,
					 Attributes ** attr, 
					 Space * space)
  : ExpLOBoper(oper_type, 2, attr, space),
    lcFlags_(0),
    convertSize_(0)
{
};

void ExpLOBconvert::displayContents(Space * space, const char * displayStr, 
				   Int32 clauseNum, char * constsArea)

{
  ExpLOBoper::displayContents(space, "ExpLOBconvert", clauseNum, constsArea);

}

ex_expr::exp_return_type ExpLOBconvert::eval(char *op_data[],
					     CollHeap*h,
					     ComDiagsArea** diagsArea)
{
  if (lobV2())
    return evalV2(op_data, h, diagsArea);

  Lng32 rc = 0;


  char * result = op_data[0];
  char * lobHandle = op_data[1];
  char *tgtFileName = NULL;
  Int32 handleLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS+1]);
  if (handleLen == 0)
   {
     ExRaiseSqlError(h, diagsArea, 
                     (ExeErrorCode)(8443), NULL, NULL);
     
     return ex_expr::EXPR_ERROR;
   }

  Int64 uid;
  Lng32 lobType;
  Lng32 lobNum;
  Int64 descKey;
  Int64 descTS;
  Int16 flags;
  short schNameLen = 0;
  char schName[500];
  LobsSubOper so;
  Lng32 cliError = 0;

  Lng32 waitedOp = 0;
  Int64 lobLen = 0; 
  char *lobData = NULL;
  waitedOp = 1;
  
  extractFromLOBhandle(&flags, &lobType, &lobNum, &uid,
                       &descKey, &descTS, 
                       &schNameLen, schName,
                       lobHandle);
  // get the lob name where data need to be inserted
  char lobNameBuf[LOB_NAME_LEN];
  char *lobName = NULL;
  lobName = ExpGetLOBname(uid, lobNum,
                          descTS, getNumLOBdatafiles(),
                          lobNameBuf, LOB_NAME_LEN);    
  if (lobName == NULL)
    return ex_expr::EXPR_ERROR;

  if (descKey == -1) //This is an empty_blob/clob
    {
      Int32 intParam1 = LOB_DATA_EMPTY_ERROR;
      Int32 cliError = 0;
      ExRaiseSqlError(h, diagsArea, 
			  (ExeErrorCode)(8442), NULL, &intParam1, 
                      &cliError, NULL, (char*)"ExpLOBInterfaceSelect",
                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
      return ex_expr::EXPR_ERROR;
    }
  if(toFile())


    {
      so = Lob_File;
      tgtFileName = tgtFileName_;
      rc = ExpLOBInterfaceSelect(getExeGlobals()->getExLobGlobal(), 
                                 (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
				 lobName, 
				 lobStorageLocation(),
				 lobType,
				 getLobHdfsServer(), getLobHdfsPort(),

				 handleLen, lobHandle,
				 requestTag_,
                                 so,
				 -1,
				 0,
 				 waitedOp,

				 0, lobLen, lobLen, tgtFileName,getLobMaxChunkMemSize());
    }
  else if (toString())
    {
      so = Lob_Memory;
      
      if (lobName == NULL)
	return ex_expr::EXPR_ERROR;
      
      
      lobLen = getConvertSize(); 
      lobData = new(h) char[(Lng32)lobLen];
      rc = ExpLOBInterfaceSelect(getExeGlobals()->getExLobGlobal(), 
                                 (getTcb()->getStatsEntry() != NULL ? getTcb()->getStatsEntry()->castToExHdfsScanStats() : NULL),
				 lobName, 
				 lobStorageLocation(),
				 lobType,
				 getLobHdfsServer(), getLobHdfsPort(),

				 handleLen, lobHandle,
				 requestTag_,
                                 so,
				 -1,
				 0,
 				 waitedOp,

				 0, lobLen, lobLen, lobData,getLobMaxChunkMemSize());

      if (rc == LOB_ACCESS_PREEMPT)
	{
	  return ex_expr::EXPR_PREEMPT;
	}
      
      if (rc < 0)
	{
	  Lng32 intParam1 = -rc;
	  ExRaiseSqlError(h, diagsArea, 
			  (ExeErrorCode)(8442), NULL, &intParam1, 
			  &cliError, NULL, (char*)"ExpLOBInterfaceSelect",
		          getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
	  return ex_expr::EXPR_ERROR;
	}

      // store the length of substring in the varlen indicator.
      if (getOperand(0)->getVCIndicatorLength() > 0)
	{
	  getOperand(0)->setVarLength((UInt32)lobLen, op_data[-MAX_OPERANDS] );
	  if (lobLen > 0) 
	    str_cpy_all(op_data[0], lobData, (Lng32)lobLen);
	}
      else
	{
	  str_pad(result, getOperand(0)->getLength());
	  str_cpy_all(result, lobData, strlen(lobData));
	}
      NADELETEBASIC(lobData, h);
    }
  else
    return ex_expr::EXPR_ERROR;

  return ex_expr::EXPR_OK;
}

////////////////////////////////////////////////////////
// ExpLOBconvertHandle
////////////////////////////////////////////////////////
ExpLOBconvertHandle::ExpLOBconvertHandle(){};
ExpLOBconvertHandle::ExpLOBconvertHandle(OperatorTypeEnum oper_type,
					 Attributes ** attr, 
					 Space * space)
  : ExpLOBoper(oper_type, 2, attr, space),
    lchFlags_(0)
{
};

void ExpLOBconvertHandle::displayContents(Space * space, const char * displayStr, 
				   Int32 clauseNum, char * constsArea)

{
  ExpLOBoper::displayContents(space, "ExpLOBconvertHandle", clauseNum, constsArea);

}


ex_expr::exp_return_type ExpLOBconvertHandle::eval(char *op_data[],
                                                   CollHeap *h,
						   ComDiagsArea** diagsArea)
{
  char * result = op_data[0];
  char * source = op_data[1];
  Int32 fullHandleLen = getOperand(1)->getLength();
  Lng32 lobType;
  Int64 uid = 0;
  Lng32 lobNum = 0;
  Int64 descPartnKey = 0;
  Int64 descSysKey = 0;
  Int64 descTS = 0;
  Int16 flags = 0;
  Int16 schNameLen = 0;
  char schName[1024];
  long handleLen = 0;
  Int32 inlineDataLen = 0;
  Int32 inlineDataOffset = 0;
  char *inlineData = 0;
  Int64 lobUID = 0;
  Int64 lobSeq = 0;

  NABoolean lobV2 = FALSE;
  if (toHandleString() || toHandleStringWithData())
    {
      flags = ((LOBHandle*)source)->flags_;
      lobV2 = isLOBHflagSet(flags, LOBH_VERSION2);
      
      if (lobV2)
        extractFromLOBhandleV2(&flags, &lobType, &lobNum, &uid,
                               &inlineDataLen, &inlineDataOffset,
                               &lobUID, &lobSeq,
                               source);
      else
        extractFromLOBhandle(&flags, &lobType, &lobNum, &uid,
                             &descSysKey, &descTS, 
                             &schNameLen, schName,
                             source);
    }

  if (toHandleString())
    {
      updLOBhandleInlineData(source, 0, NULL);

      char lobHandleBuf[LOB_HANDLE_LEN];
      if (lobV2)
        createLOBhandleStringV2(flags, lobType, lobNum, uid,
                                inlineDataLen, inlineDataOffset,
                                lobUID, lobSeq,
                                lobHandleBuf);
      else
        createLOBhandleString(flags, lobType, uid, lobNum, 
                              descSysKey, descTS, 
                              schNameLen, schName,
                              lobHandleBuf);        
      str_cpy_all(result, lobHandleBuf, strlen(lobHandleBuf));

      getOperand(0)->setVarLength(strlen(lobHandleBuf), op_data[-MAX_OPERANDS]);
    }
  else if (toHandleStringWithData() && lobV2)
    {
      char *lobHandleBuf = new (h)char[fullHandleLen];
      createLOBhandleStringV2(flags, lobType, lobNum, uid,
                              inlineDataLen, inlineDataOffset,
                              lobUID, lobSeq,
                              lobHandleBuf);
      
      // get the inline lob length and data pointers
      Int32 inlineLobLength =  ExpLOBoper::lobHandleGetInlineDataLength(source);
      char *inlineGetDataPosition = ExpLOBoper::lobHandleGetInlineDataPosition(source);

      str_cpy_all(result, lobHandleBuf, strlen(lobHandleBuf));
      str_cpy_all(result + strlen(lobHandleBuf), 
                  inlineGetDataPosition, inlineLobLength);

      getOperand(0)->setVarLength(strlen(lobHandleBuf) + inlineLobLength,
                                  op_data[-MAX_OPERANDS]);
      NADELETEBASIC(lobHandleBuf,h);
    }
  else if (toHandleStringWithData() && (NOT lobV2))
    {
      Int32 inlineDataLimit = getLobInlineLimit();
      char *lobHandleBuf = new (h)char[fullHandleLen];
      createLOBhandleString(flags, lobType, uid, lobNum, 
			    descSysKey, descTS, 
			    schNameLen, schName,
			    lobHandleBuf);

      str_cpy_all(result, lobHandleBuf, strlen(lobHandleBuf));
      Int32 inlineLobLength = 0;
      if (inlineDataLimit > 0)
        {
          // get the inline lob length and data pointers
          inlineLobLength =  ExpLOBoper::lobHandleGetInlineDataLength(source);
          char *inlineGetDataPosition = ExpLOBoper::lobHandleGetInlineDataPosition(source);
          
          str_cpy_all((char *)(result+strlen(lobHandleBuf)),(char *)&inlineLobLength, sizeof(Int32));
          str_cpy_all((char *)(result+strlen(lobHandleBuf)+sizeof(Int32)),inlineGetDataPosition+sizeof(Int32),inlineLobLength);

          getOperand(0)->setVarLength(strlen(lobHandleBuf)+sizeof(Int32)+inlineLobLength, op_data[-MAX_OPERANDS]);
        }
      else
        getOperand(0)->setVarLength(strlen(lobHandleBuf), op_data[-MAX_OPERANDS]);        

      NADELETEBASIC(lobHandleBuf,h);
    }
  else if (toLob())
    {
      Int64 addrOfLobHandleLen = (Int64)source-sizeof(short) ;  
      Int32 lobHandleLen = *(short *)addrOfLobHandleLen;
      if (strncmp(source,"LOBH",4)== 0)
	{
          flags = (Lng32)str_atoi(&source[4], 4);
          NABoolean lobV2 = isLOBHflagSet(flags, LOBH_VERSION2);  

          Lng32 handleLen = 0;
	  // This is the external string format of the handle
          if (lobV2)
            {
              genLOBhandleFromHandleStringV2(source, lobHandleLen,
                                             result, handleLen);
            }
          else
            {
              extractFromLOBstring(&uid, 
                                   &lobNum, 
                                   &descPartnKey,
                                   &descSysKey, 
                                   &flags,
                                   &lobType,
                                   &schNameLen,
                                   schName,
                                   &inlineDataLen,
                                   inlineData,
                                   source, 
                                   lobHandleLen);
              
              short lobType = 1;
              genLOBhandle(uid, lobNum, lobType, descPartnKey, descSysKey, flags,
                           schNameLen, schName,
                           handleLen,
                           result);
            }
            
	  getOperand(0)->setVarLength(handleLen, op_data[-MAX_OPERANDS]);
	}
      else 
	{
	  // Source is pointing to the internal packed format as stored on disk.
	  // Simply cast result to source so it can be interpreted/cast 
	  // as LOBhandle.
	  str_cpy_all(result, source, sizeof(Int64));
	  getOperand(0)->setVarLength(lobHandleLen, op_data[-MAX_OPERANDS]);
	}
    }

  return ex_expr::EXPR_OK;
}


//////////////////////////////////////////////////
// ExpLOBfunction
//////////////////////////////////////////////////
ExpLOBfunction::ExpLOBfunction(){};
ExpLOBfunction::ExpLOBfunction(OperatorTypeEnum oper_type,
			       short num_operands,
			       Attributes ** attr,
			       Space * space)
  : ExpLOBoper(oper_type, num_operands, attr, space),
    funcFlags_(0)
{};


//////////////////////////////////////////////////
// ExpLOBfuncSubstring
//////////////////////////////////////////////////
ExpLOBfuncSubstring::ExpLOBfuncSubstring(){};
ExpLOBfuncSubstring::ExpLOBfuncSubstring(OperatorTypeEnum oper_type,
					 short num_operands,
					 Attributes ** attr,
					 Space * space)
  : ExpLOBfunction(oper_type, num_operands, attr, space)
{};

void ExpLOBfuncSubstring::displayContents(Space * space, const char * displayStr, 
					  Int32 clauseNum, char * constsArea)
  
{
  ex_clause::displayContents(space, "ExpLOBfuncSubstring", clauseNum, constsArea);

}

ex_expr::exp_return_type ExpLOBfuncSubstring::eval(char *op_data[],
						   CollHeap *heap,
						   ComDiagsArea** diagsArea)
{

  Int32 len1_bytes = getOperand(1)->getLength(op_data[-MAX_OPERANDS+1]);
  Int32 startPos = *(Lng32 *)op_data[2];
  
  Int32 specifiedLen = len1_bytes;
  if (getNumOperands() == 4)
    specifiedLen = *(Lng32 *)op_data[3]; 

  return ex_expr::EXPR_OK;
}
  
//////////////////////////////////////////////////
// ExpLOBlength
//////////////////////////////////////////////////
ExpLOBlength::ExpLOBlength(){};
ExpLOBlength::ExpLOBlength(OperatorTypeEnum oper_type,
                           Attributes ** attr,
                           Space * space)
     : ExpLOBoper(oper_type, 2, attr, space)
{};

void ExpLOBlength::displayContents(Space * space, const char * displayStr, 
                                   Int32 clauseNum, char * constsArea)
  
{
  ex_clause::displayContents(space, "ExpLOBlength", clauseNum, constsArea);

}

ex_expr::exp_return_type ExpLOBlength::eval(char *op_data[],
                                            CollHeap *heap,
                                            ComDiagsArea** diagsArea)
{
  if (lobV2())
    return evalV2(op_data, heap, diagsArea);

  ExRaiseSqlError(heap, diagsArea, 
                  (ExeErrorCode)(4043));
  return ex_expr::EXPR_ERROR;
}
  
Lng32 LOBsql2loaderInterface
(
 /*IN*/     char * fileName,
 /*IN*/     Lng32  fileNameLen,
 /*IN*/     char * loaderInfo,
 /*IN*/     Lng32  loaderInfoLen,
 /*IN*/     char * handle,
 /*IN*/     Lng32  handleLen,
 /*IN*/     char * lobInfo,
 /*IN*/     Lng32  lobInfoLen
 )
{
  return 0;
}
