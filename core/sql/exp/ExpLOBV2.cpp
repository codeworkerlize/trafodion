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
 * Created:     
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
#include "ExpLobOperV2.h"
#include "ex_globals.h"
#include "ex_god.h"

static THREAD_P Int64 lobSeqNum = 0;

ex_expr::exp_return_type ExpLOBiud::insertV2
(
     char *op_data[],
     Int64 *lobUID,
     Int32 lobNum,
     Int32 chunkNum,
     Int64 lobLen,
     char *lobData,
     char *&result,
     CollHeap*h,
     ComDiagsArea** diagsArea
 )
{
  Lng32 retcode = 0;
  Int32 cliError = 0;

  Lng32 handleLen = 0;
  char lobHandleBuf[LOB_V2_INT_HANDLE_LEN];

  char * lobHandle = NULL; 
  lobHandle = lobHandleBuf;
  Int32 flags = 0;
  ExpLOBoper::setLOBHflag(flags, ExpLOBoper::LOBH_VERSION2);
  ExpLOBoper::genLOBhandleV2(objectUID_, lobNum, (short)lobStorageType(),
                             lobUID[0], lobUID[1], flags,
                             handleLen, lobHandle);

  Lng32 outHandleLen = getOperand(0)->getLength();
  retcode = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                                    lobHandle, handleLen,
                                    ExpLobOperV2::LOB_DML_INSERT,
                                    getOptions(),
                                    lobStorageLocation(),
                                    h,
                                    result, &outHandleLen,
                                    (Int64*)lobData,
                                    &lobLen);
  if (retcode < 0)
    {
      Lng32 intParam1 = -retcode;
      ExRaiseSqlError(h, diagsArea, 
                      (intParam1 == LOB_DATA_WRITE_ERROR_RETRY ?
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY) :
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE)), 
                      NULL, &intParam1, 
                      &cliError, NULL, (char*)"lobOperV2_.dmlInterface",
                      NULL, lobOperV2_.getQTypeStr(ExpLobOperV2::LOB_DML_INSERT));
      return ex_expr::EXPR_ERROR;  
    }

  getOperand(0)->setVarLength(outHandleLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBinsert::insertSelectV1fromV2
(
     char *op_data[],
     CollHeap*h,
     ComDiagsArea** diagsArea
 )
{
  Lng32 retcode = 0;
  Int32 cliError = 0;

  // get source handle pointed to by op_data[1]
  Int32 sourceHandleLen = getOperand(1)->getLength();
  char * sourceHandle = op_data[1];
  
  Int16 srcFlags = 0;
  Lng32 lobType;
  Lng32 srcLobNum = 0;
  Int64 uid = 0;
  Int64 lobUID = 0;
  Int64 lobSeq = 0;
  extractFromLOBhandleV2(&srcFlags, &lobType, &srcLobNum, &uid,
                         NULL, NULL,
                         &lobUID, &lobSeq,
                         sourceHandle);
  
  char sourceHandleStr[LOB_HANDLE_LEN];
  createLOBhandleStringV2(srcFlags, lobType, srcLobNum, uid,
                          0, 0,
                          lobUID, lobSeq,
                          sourceHandleStr);      

  // extract source lob length
  Int64 lengthOfLob = 0;
  CliGlobals *cliGlobals = GetCliGlobals();
  ContextCli   & currContext = *(cliGlobals->currContext());
  ExeCliInterface cliInterface(h, SQLCHARSETCODE_UTF8, &currContext, NULL);

  char query[4096];
  sprintf(query,"extract loblength (table uid '%020ld', lob '%s') LOCATION %lu ",
          uid, sourceHandleStr, (unsigned long)&lengthOfLob);
      
  retcode = cliInterface.executeImmediate(query);
  if (retcode < 0)
    {
      cliInterface.allocAndRetrieveSQLDiagnostics(*diagsArea);

      return ex_expr::EXPR_ERROR;
    }

  // retrieve lob data
  char *sourceLobData = new(h) char[lengthOfLob];
  Int64 dataLocationAddr = (Int64)sourceLobData;
  Int64 returnedLobLen = lengthOfLob;
  Int64 dataSizeAddr = (Int64)&returnedLobLen;
  sprintf(query,"extract lobtobuffer(table uid '%020ld', lob '%s', LOCATION %ld, SIZE %ld) ", 
          uid, sourceHandleStr, dataLocationAddr, dataSizeAddr);
  retcode = cliInterface.executeImmediate(query);
  if (retcode < 0)
    {
      NADELETEBASIC(sourceLobData, h);

      cliInterface.allocAndRetrieveSQLDiagnostics(*diagsArea);

      return ex_expr::EXPR_ERROR;
    }

  Lng32 outHandleLen = getOperand(0)->getLength();

  Int64 descTS = 0;
  char * handle = op_data[0];
  Int32 handleLen = getOperand(0)->getLength();
  Int64 lobHdfsOffset = 0;
  Int64 chunkMemSize = returnedLobLen;
  setFromBuffer(TRUE);
  setFromLob(FALSE);
  ex_expr::exp_return_type err = insertData(handleLen, handle, 
                                            sourceLobData, chunkMemSize,
                                            descTS, 0,
                                            lobHdfsOffset, h, diagsArea);
  if (err == ex_expr::EXPR_ERROR)  
    {
      return err;      
    }

  char * result = op_data[0];
  err = insertDesc(op_data, sourceLobData, chunkMemSize, lobHdfsOffset, descTS,
                   result,h, diagsArea);
  if (err == ex_expr::EXPR_ERROR)  
    {
      return err;      
    }

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBinsert::insertSelectV2fromV1
(
     char *op_data[],
     Int64 *lobUID,
     Int32 lobNum,
     char *&result,
     CollHeap*h,
     ComDiagsArea** diagsArea
 )
{
  Lng32 retcode = 0;
  Int32 cliError = 0;

  Lng32 handleLen = 0;
  char lobHandleBuf[LOB_V2_INT_HANDLE_LEN];

  // generate handle for target
  char * lobHandle = NULL; 
  lobHandle = lobHandleBuf;
  Int32 flags = 0;
  ExpLOBoper::setLOBHflag(flags, ExpLOBoper::LOBH_VERSION2);
  ExpLOBoper::genLOBhandleV2(objectUID_, lobNum, (short)lobStorageType(),
                             lobUID[0], lobUID[1], flags,
                             handleLen, lobHandle);
 
  // get source handle pointed to by op_data[1]
  Int32 sourceHandleLen = getOperand(1)->getLength();
  char * sourceHandle = op_data[1];
  
  Int16 srcFlags = 0;
  Lng32 lobType;
  Lng32 srcLobNum = 0;
  Int64 uid = 0;
  Int64 descSysKey = 0;
  Int64 descTS = 0;
  Int16 schNameLen = 0;
  char schName[1024];
  extractFromLOBhandle(&srcFlags, &lobType, &srcLobNum, &uid,
                       &descSysKey, &descTS, 
                       &schNameLen, schName,
                       sourceHandle);
  
  char sourceHandleStr[LOB_HANDLE_LEN];
  createLOBhandleString(srcFlags, lobType, uid, srcLobNum, 
                        descSysKey, descTS, 
                        schNameLen, schName,
                        sourceHandleStr);      

  // extract source lob length
  Int64 lengthOfLob = 0;
  CliGlobals *cliGlobals = GetCliGlobals();
  ContextCli   & currContext = *(cliGlobals->currContext());
  ExeCliInterface cliInterface(h, SQLCHARSETCODE_UTF8, &currContext, NULL);

  char query[4096];
  sprintf(query,"extract loblength (lob '%s') LOCATION %lu ",
          sourceHandleStr, (unsigned long)&lengthOfLob);
      
  retcode = cliInterface.executeImmediate(query);
  if (retcode < 0)
    {
      cliInterface.allocAndRetrieveSQLDiagnostics(*diagsArea);

      return ex_expr::EXPR_ERROR;
    }

  // retrieve lob data
  char *sourceLobData = new(h) char[lengthOfLob];
  Int64 dataLocationAddr = (Int64)sourceLobData;
  Int64 returnedLobLen = lengthOfLob;
  Int64 dataSizeAddr = (Int64)&returnedLobLen;
  sprintf(query,"extract lobtobuffer(lob '%s', LOCATION %ld, SIZE %ld) ", 
          sourceHandleStr, dataLocationAddr, dataSizeAddr);
  retcode = cliInterface.executeImmediate(query);
  if (retcode < 0)
    {
      NADELETEBASIC(sourceLobData, h);

      cliInterface.allocAndRetrieveSQLDiagnostics(*diagsArea);

      return ex_expr::EXPR_ERROR;
    }

  Lng32 outHandleLen = getOperand(0)->getLength();
  ExpLobOperV2::DMLQueryType qt = ExpLobOperV2::LOB_DML_INSERT_APPEND;
  Int64 currChunkLen = returnedLobLen;
  retcode = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                                    lobHandle, handleLen,
                                    ExpLobOperV2::LOB_DML_INSERT_APPEND,
                                    getOptions(),
                                    lobStorageLocation(),
                                    h,
                                    result, &outHandleLen,
                                    (Int64*)sourceLobData,
                                    &currChunkLen);  

  NADELETEBASIC(sourceLobData, h);
  if (retcode < 0)
    {
      Lng32 intParam1 = -retcode;
      ExRaiseSqlError(h, diagsArea, 
                      (intParam1 == LOB_DATA_WRITE_ERROR_RETRY ?
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY) :
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE)), 
                      NULL, &intParam1, 
                      &cliError, NULL, (char*)"lobOperV2_.dmlInterface",
                      NULL, lobOperV2_.getQTypeStr(qt));
      return ex_expr::EXPR_ERROR;
    }

  getOperand(0)->setVarLength(outHandleLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBinsert::insertSelectV2
(
     char *op_data[],
     Int64 *lobUID,
     Int32 lobNum,
     char *&result,
     CollHeap*h,
     ComDiagsArea** diagsArea
 )
{
  Lng32 retcode = 0;
  Int32 cliError = 0;

  Lng32 handleLen = 0;
  char lobHandleBuf[LOB_V2_INT_HANDLE_LEN];

  // generate handle for target
  char * lobHandle = NULL; 
  lobHandle = lobHandleBuf;
  Int32 flags = 0;
  ExpLOBoper::setLOBHflag(flags, ExpLOBoper::LOBH_VERSION2);
  ExpLOBoper::genLOBhandleV2(objectUID_, lobNum, (short)lobStorageType(),
                             lobUID[0], lobUID[1], flags,
                             handleLen, lobHandle);
 
  // get source handle pointed to by op_data[1]
  Int32 sourceHandleLen = getOperand(1)->getLength();
  char * sourceHandle = op_data[1];

  // Allocate memory to hold the lob data read from source lob
  char *sourceLobData = NULL;
  Int64 maxLobChunkSize = getLobMaxChunkMemSize();
  sourceLobData = (char *)(h->allocateMemory(maxLobChunkSize));
  ExpLobOperV2::DMLQueryType qt = ExpLobOperV2::LOB_DML_SELECT_OPEN;
  Int64 currChunkLen = maxLobChunkSize;
  retcode = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                                    sourceHandle, sourceHandleLen,
                                    ExpLobOperV2::LOB_DML_SELECT_OPEN,
                                    getSrcOptions(),
                                    srcLobStorageLocation(),
                                    h,
                                    NULL, NULL,
                                    NULL, &currChunkLen);
  
  Int64 totalReturnedLobLen = 0;
  Lng32 outHandleLen = getOperand(0)->getLength();
  while ((retcode >= 0) && (retcode != 100))
    {
      currChunkLen = maxLobChunkSize;
      qt = ExpLobOperV2::LOB_DML_SELECT_FETCH;
      retcode = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                                        sourceHandle, sourceHandleLen,
                                        ExpLobOperV2::LOB_DML_SELECT_FETCH,
                                        getSrcOptions(),
                                        srcLobStorageLocation(),
                                        h, 
                                        NULL, NULL,
                                        (Int64*)&sourceLobData, &currChunkLen);
      if (retcode < 0)
        break;
      
      if (retcode == 100)
        break;
      
      qt = ExpLobOperV2::LOB_DML_INSERT_APPEND;
      retcode = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                                        lobHandle, handleLen,
                                        ExpLobOperV2::LOB_DML_INSERT_APPEND,
                                        getOptions(),
                                        lobStorageLocation(),
                                        h,
                                        result, &outHandleLen,
                                        (Int64*)sourceLobData,
                                        &currChunkLen);
      if (retcode < 0)
        break;
    } // while
  
  Lng32 tempRC = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                                         sourceHandle, sourceHandleLen,
                                         ExpLobOperV2::LOB_DML_SELECT_CLOSE,
                                         getSrcOptions(),
                                         srcLobStorageLocation(),
                                         h,
                                         NULL, NULL,
                                         NULL, NULL);

  NADELETEBASIC(sourceLobData, h);
  if ((retcode >= 0) && (tempRC < 0))
    {
      qt = ExpLobOperV2::LOB_DML_INSERT_APPEND;
      retcode = tempRC;
    }
  if (retcode < 0)
    {
      Lng32 intParam1 = -retcode;
      ExRaiseSqlError(h, diagsArea, 
                      (intParam1 == LOB_DATA_WRITE_ERROR_RETRY ?
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY) :
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE)), 
                      NULL, 
                      &intParam1,  // int0
                      &cliError,   // int1
                      NULL,        // int2
                      (char*)"lobOperV2_.dmlInterface", // string0
                      NULL,                             // string1
                      lobOperV2_.getQTypeStr(qt));      // string2
      return ex_expr::EXPR_ERROR;
    }

  //  str_cpy_all(result, lobHandle, handleLen);
  getOperand(0)->setVarLength(outHandleLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

// if inLobHandle is passed in, use it. This is for updates from a file.
// Otherwise generate a new lobhandle
ex_expr::exp_return_type ExpLOBiud::insertSelectFileV2
(
     char * inLobHandle, 
     Lng32  inLobHandleLen,
     char *op_data[],
     Int64 *lobUID,
     Int32 lobNum,
     Int64 srcDataSize,
     char *&result,
     NABoolean forUpdate,
     CollHeap*h,
     ComDiagsArea** diagsArea
 )
{
  Lng32 retcode = 0;
  Int32 cliError = 0;

  Lng32 handleLen = 0;
  char * lobHandle = NULL; 
  char lobHandleBuf[LOB_V2_INT_HANDLE_LEN];
  if (inLobHandle)
    {
      lobHandle = inLobHandle;
      handleLen =  inLobHandleLen;
    }
  else
    {
      // generate handle for target
      lobHandle = lobHandleBuf;
      Int32 flags = 0;
      ExpLOBoper::setLOBHflag(flags, ExpLOBoper::LOBH_VERSION2);
      ExpLOBoper::genLOBhandleV2(objectUID_, lobNum, (short)lobStorageType(),
                                 lobUID[0], lobUID[1], flags,
                                 handleLen, lobHandle);
    }

  // Allocate memory to hold the lob data read from source lob
  char *sourceLobData = NULL;
  Int64 maxLobChunkSize = MINOF(srcDataSize, getLobMaxChunkMemSize());
  sourceLobData = (char *)(h->allocateMemory(maxLobChunkSize));
  ExpLobOperV2::DMLQueryType qt;

  if (forUpdate)
    qt = ExpLobOperV2::LOB_DML_UPDATE_APPEND;
  else
    qt = ExpLobOperV2::LOB_DML_INSERT_APPEND;

  // get source file pointed to by op_data[1]
  NAString sourceFileName(op_data[1], getOperand(1)->getLength());

  ExLobV2 lobPtr(getExeGlobals()->getExLobGlobal()->getHeap(), NULL);
  lobPtr.lobGlobalHeap_ = getExeGlobals()->getExLobGlobal()->getHeap();
  
  Int64 totalReturnedLobLen = 0;
  Lng32 outHandleLen = getOperand(0)->getLength();
  Int64 fileOffset = 0;
  Int64 remainingBytes = srcDataSize;
  retcode = 0;
  Int32 readDetailError = 0;
  while ((retcode >= 0) && (retcode != 100))
    {
      Int64 currChunkLen = MINOF(remainingBytes, maxLobChunkSize);

      char *fileData = NULL;
      readDetailError = 0;
      retcode = lobPtr.readSourceFile((char*)sourceFileName.data(),
                                      fileData, currChunkLen,
                                      fileOffset,
                                      &readDetailError);
      if (retcode != LOB_OPER_OK)
        {
          retcode = -retcode;
          break;
        }

      if (fileData)
        {
          memcpy(sourceLobData, fileData, currChunkLen);
          lobPtr.getLobGlobalHeap()->deallocateMemory(fileData);
          fileData = NULL;
        }

      if (retcode < 0)
        break;
      
      if (currChunkLen <= 0)
        {
          retcode = 100;
          break;
        }

      retcode = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                                        lobHandle, handleLen,
                                        (forUpdate 
                                         ? ExpLobOperV2::LOB_DML_UPDATE_APPEND
                                         : ExpLobOperV2::LOB_DML_INSERT_APPEND),
                                        getOptions(),
                                        lobStorageLocation(),
                                        h,
                                        result, &outHandleLen,
                                        (Int64*)sourceLobData,
                                        &currChunkLen);
      if (retcode < 0)
        break;

      remainingBytes -= currChunkLen;
      if (remainingBytes <= 0) // all done
        retcode = 100;
      fileOffset += currChunkLen;
    } // while
  
  NADELETEBASIC(sourceLobData, h);
  if (retcode < 0)
    {
      Lng32 intParam1 = -retcode;
      ExRaiseSqlError(h, diagsArea, 
                      (intParam1 == LOB_DATA_WRITE_ERROR_RETRY ?
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY) :
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE)), 
                      NULL, 
                      &intParam1, 
                      &readDetailError,
                      NULL, 
                      (char*)"lobOperV2_.dmlInterface",
                      NULL, 
                      lobOperV2_.getQTypeStr(qt));
      return ex_expr::EXPR_ERROR;
    }

  getOperand(0)->setVarLength(outHandleLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

Int64 generateUniqueValueFast ();

ex_expr::exp_return_type ExpLOBinsert::evalV2(char *op_data[],
                                              CollHeap*h,
                                              ComDiagsArea** diagsArea)
{
  ex_expr::exp_return_type err = ex_expr::EXPR_OK;
  char * result = op_data[0];
  Int64 lobLen = 0; 
  char *lobData = NULL;
  char *inputAddr = NULL;

  if (lobNum() == -1)
    {
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

  err = getLobInputLengthV2(op_data, lobLen, h, diagsArea);
  if (err == ex_expr::EXPR_ERROR)
    return err;

  getLobInputData(op_data, lobData, lobLen, h);

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
                      NULL, NULL, (char*)"ExpLOBinsert:evalV2",
                      getLobErrStr(LOB_MAX_LIMIT_ERROR),NULL);
      return ex_expr::EXPR_ERROR;
    }

  Int64 lobUID[2];

  lobUID[0] = generateUniqueValueFast();

  lobSeqNum++;
  if (lobSeqNum == LLONG_MAX)
    lobSeqNum = 1;

  lobUID[1] = lobSeqNum;
  if (fromLob())
    {
      if (NOT srcLobV2()) // insert into V2 from V1
        err = insertSelectV2fromV1(op_data,
                                   lobUID, lobNum(),
                                   result,
                                   h, diagsArea);
      else
        err = insertSelectV2(op_data,
                             lobUID, lobNum(),
                             result,
                             h, diagsArea);
    }
  else if ((fromFile()) || (fromExternal()))
    {
      err = insertSelectFileV2(NULL, 0,
                               op_data,
                               lobUID, lobNum(),
                               lobLen, // source file size
                               result,
                               FALSE,
                               h, diagsArea);
    }
  else
    {
      err = insertV2(op_data,
                     lobUID, lobNum(), 1,
                     lobLen, lobData, 
                     result,
                     h, diagsArea);
    }

  if (err == ex_expr::EXPR_ERROR)  
    return err;      

  return err;
}

ex_expr::exp_return_type ExpLOBdelete::evalV2(char *op_data[],
                                              CollHeap*h,
                                              ComDiagsArea** diagsArea)
{
  Lng32 rc = 0;

  // null processing need to be done in eval
  if ((! isNullRelevant()) &&
      (getOperand(1)->getNullFlag()) && 
      (! op_data[-(2 * MAX_OPERANDS) + 1])) // missing value (is a null value)
    {
      // nothing to delete if lobHandle is null
      return ex_expr::EXPR_OK;
    }

  char * result = op_data[0];

  char * lobHandle = op_data[1];
  Int32 handleLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS+1]);
  if (handleLen == 0)
   {
     ExRaiseSqlError(h, diagsArea, 
                     (ExeErrorCode)(8443), NULL, NULL);
     
     return ex_expr::EXPR_ERROR;
   }

  Lng32 lobType;
  Int64 uid;
  Int64 lobUID;
  Lng32 lobNum;
  extractFromLOBhandleV2(NULL, &lobType, &lobNum, &uid,
                         NULL, NULL,
                         &lobUID, NULL,
                         lobHandle);
  
  if (lobUID == -1) //This is an empty_blob/clob
    {
      Int32 intParam1 = LOB_DATA_EMPTY_ERROR;
      Int32 cliError = 0;
      ExRaiseSqlError(h, diagsArea, 
                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), 
                      NULL, &intParam1, 
                      &cliError, NULL, (char*)"extractFromLOBhandleV2",
                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
      return ex_expr::EXPR_ERROR;
    }

  rc = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                               lobHandle, handleLen,
                               ExpLobOperV2::LOB_DML_DELETE,
                               getOptions(), 
                               lobStorageLocation(),
                               h,
                               NULL, NULL,
                               NULL, NULL);
  if (rc < 0)
    {
      Lng32 intParam1 = -rc;
      Lng32 cliError = 0;
      ExRaiseSqlError(h, diagsArea, 
                      (intParam1 == LOB_DATA_WRITE_ERROR_RETRY ?
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY) :
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE)), 
                      NULL, &intParam1, 
                      &cliError, NULL, (char*)"lobOperV2_.dmlInterface",
                      NULL, lobOperV2_.getQTypeStr(ExpLobOperV2::LOB_DML_DELETE));
      return ex_expr::EXPR_ERROR;
    }
  
  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBupdate::evalV2(char *op_data[],
                                              CollHeap*h,
                                              ComDiagsArea** diagsArea)
{
  ex_expr::exp_return_type err = ex_expr::EXPR_OK;
  Lng32 rc = 0;

  char * result = op_data[0];

  char *lobHandle = NULL;
  Int32 handleLen = 0;

  char lobHandleBuf[LOB_V2_INT_HANDLE_LEN];
  if (nullHandle_)
    {
      Int64 lobUID[2];

      lobUID[0] = generateUniqueValueFast();
      lobSeqNum++;
      if (lobSeqNum == LLONG_MAX)
        lobSeqNum = 1;
      lobUID[1] = lobSeqNum;

      lobHandle = lobHandleBuf;
      Int32 flags = 0;
      ExpLOBoper::setLOBHflag(flags, ExpLOBoper::LOBH_VERSION2);
      ExpLOBoper::genLOBhandleV2(objectUID_, lobNum(), (short)lobStorageType(),
                                 lobUID[0], lobUID[1], flags,
                                 handleLen, lobHandle);
    }
  else
    {
      lobHandle = op_data[2];
      handleLen = getOperand(2)->getLength(op_data[-MAX_OPERANDS+2]);
      if (handleLen == 0)
        {
          ExRaiseSqlError(h, diagsArea, 
                          (ExeErrorCode)(8443), NULL, NULL);
          
          return ex_expr::EXPR_ERROR;
        }
    }

  Lng32 lobType;
  Int64 uid;
  Int64 lobUID;
  Lng32 lobNum;
  extractFromLOBhandleV2(NULL, &lobType, &lobNum, &uid,
                         NULL, NULL, &lobUID, NULL,
                         lobHandle);
  
  if (lobUID == -1) //This is an empty_blob/clob
    {
      Int32 intParam1 = LOB_DATA_EMPTY_ERROR;
      Int32 cliError = 0;
      ExRaiseSqlError(h, diagsArea, 
                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), 
                      NULL, &intParam1, 
                      &cliError, NULL, (char*)"extractFromLOBhandleV2",
                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
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

  Int64 lobLen = 0;
  char *lobData = NULL;

  err = getLobInputLengthV2(op_data, lobLen, h, diagsArea);
  if (err == ex_expr::EXPR_ERROR)
    return err;

  getLobInputData(op_data, lobData, lobLen, h);

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
                      NULL, NULL, (char*)"ExpLOBupdate:evalV2",
                      getLobErrStr(LOB_MAX_LIMIT_ERROR),NULL);
      return ex_expr::EXPR_ERROR;
    }

  Lng32 outHandleLen = getOperand(0)->getLength();

  Int32 opNullIdx = -2 * MAX_OPERANDS;
  char * tgtNull = op_data[opNullIdx];
  char * src = op_data[opNullIdx + 1];
  NABoolean nullResult = FALSE;
  if ((getOperand(1)->getNullFlag()) &&    // nullable
      (! src))                             // null/missing value 
    {
      ExpTupleDesc::setNullValue(tgtNull,
                                 getOperand(0)->getNullBitIndex(),
                                 getOperand(0)->getTupleFormat() );

      nullResult = TRUE;
    }

  if ((NOT isAppend()) || (nullResult))
    {
      // unique update. Delete existing row.
      rc = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                                   lobHandle, handleLen,
                                   ExpLobOperV2::LOB_DML_DELETE,
                                   getOptions(), 
                                   lobStorageLocation(),
                                   h,
                                   NULL, NULL,
                                   NULL, NULL);
    }
  
  if (nullResult)
    return ex_expr::EXPR_NULL;

  if ((fromFile()) || (fromExternal()))
    {
      rc = insertSelectFileV2(lobHandle, handleLen,
                              op_data,
                              &lobUID, lobNum,
                              lobLen, // source file size
                              result,
                              TRUE,
                              h, diagsArea);
    }
  else
    {
      rc = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                                   lobHandle, handleLen,
                                   (isAppend() 
                                    ? ExpLobOperV2::LOB_DML_UPDATE_APPEND 
                                    : ExpLobOperV2::LOB_DML_UPDATE),
                                   getOptions(),
                                   lobStorageLocation(),
                                   h,
                                   (NOT isAppend() ? result : NULL),
                                   (NOT isAppend() ? &outHandleLen : NULL),
                                   (Int64*)lobData,
                                   &lobLen);
    }

  if (rc < 0)
    {
      Lng32 intParam1 = -rc;
      Lng32 cliError = 0;
      ExRaiseSqlError(h, diagsArea, 
                      (intParam1 == LOB_DATA_WRITE_ERROR_RETRY ?
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE_RETRY) :
                       (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE)),
                      NULL, &intParam1, 
                      &cliError, NULL, (char*)"lobOperV2_.dmlInterface",
                      NULL, (isAppend() ? "LOB_DML_INSERT_APPEND" : "LOB_DML_INSERT"));
      return ex_expr::EXPR_ERROR;
    }

  if (isAppend())
    {
      str_cpy_all(result, lobHandle, handleLen);
      getOperand(0)->setVarLength(handleLen, op_data[-MAX_OPERANDS]);
    }
  else
    getOperand(0)->setVarLength(outHandleLen, op_data[-MAX_OPERANDS]);

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBconvert::evalV2(char *op_data[],
                                               CollHeap*h,
                                               ComDiagsArea** diagsArea)
{
  Lng32 rc = 0;

  char * result = op_data[0];
  Int32 resultLen = getOperand(0)->getLength();
  char * lobHandle = op_data[1];
  Int32 handleLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS+1]);
  if (handleLen == 0)
   {
     ExRaiseSqlError(h, diagsArea, 
                     (ExeErrorCode)(8443), NULL, NULL);
     
     return ex_expr::EXPR_ERROR;
   }

  Int64 lobUID;
  extractFromLOBhandleV2(NULL, NULL, NULL, NULL,
                         NULL, NULL, &lobUID, NULL,
                         lobHandle);
  
  if (lobUID == -1) //This is an empty_blob/clob
    {
      Int32 intParam1 = LOB_DATA_EMPTY_ERROR;
      ExRaiseSqlError(h, diagsArea, 
                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), 
                      NULL, &intParam1, 
                      NULL, NULL, (char*)"extractFromLOBhandleV2",
                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
      return ex_expr::EXPR_ERROR;
    }

  if (toFile())
    {
      ExRaiseSqlError(h, diagsArea, 
                      (ExeErrorCode)(8145), NULL, NULL, NULL, NULL,
                      "convert::toFile() not yet supported");
      
      return ex_expr::EXPR_ERROR;
    }

  if (toString())
    {
      Int64 lobLen = resultLen; // max length that can be returned
      char *lobData = result;

      Int64 returnedLen = 0;

      rc = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                                   lobHandle, handleLen,
                                   ExpLobOperV2::LOB_DML_SELECT_ALL,
                                   getOptions(),
                                   lobStorageLocation(),
                                   h,
                                   NULL, NULL,
                                   (Int64*)&lobData, (Int64*)&lobLen);
      if (rc < 0)
	{
	  Lng32 intParam1 = -rc;
	  ExRaiseSqlError(h, diagsArea, 
			  (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE),
                          NULL, &intParam1, 
			  NULL, NULL, (char*)"lobOperV2_.dmlInterface",
		          NULL, "LOB_DML_SELECT_ALL");
	  return ex_expr::EXPR_ERROR;
	}
      
      returnedLen += lobLen;

      // store the length of substring in the varlen indicator.
      if (getOperand(0)->getVCIndicatorLength() > 0)
	{
	  getOperand(0)->setVarLength((UInt32)returnedLen, 
                                      op_data[-MAX_OPERANDS] );
	}
      else
	{
          str_pad(&result[returnedLen], resultLen - returnedLen);
	}
    }
  else
    return ex_expr::EXPR_ERROR;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBlength::evalV2(char *op_data[],
                                              CollHeap *heap,
                                              ComDiagsArea** diagsArea)
{
  Lng32 rc = 0;

  char * result = op_data[0];
  char * lobHandle = op_data[1];
  Int32 handleLen = getOperand(1)->getLength(op_data[-MAX_OPERANDS+1]);
  if (handleLen == 0)
   {
     ExRaiseSqlError(heap, diagsArea, 
                     (ExeErrorCode)(8443), NULL, NULL);
     
     return ex_expr::EXPR_ERROR;
   }

  Int64 lobUID;
  extractFromLOBhandleV2(NULL, NULL, NULL, NULL,
                         NULL, NULL, &lobUID, NULL,
                         lobHandle);
  
  if (lobUID == -1) //This is an  invalid lob
    {
      Int32 intParam1 = LOB_DATA_EMPTY_ERROR;
      ExRaiseSqlError(heap, diagsArea, 
                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE), 
                      NULL, &intParam1, 
                      NULL, NULL, (char*)"extractFromLOBhandleV2",
                      getLobErrStr(intParam1), (char*)getSqlJniErrorStr());
      return ex_expr::EXPR_ERROR;
    }

  Int64 lobLen = 0;
  rc = lobOperV2_.dmlInterface(getExeGlobals()->getExLobGlobal(),
                               lobHandle, handleLen,
                               ExpLobOperV2::LOB_DML_SELECT_LOBLENGTH,
                               getOptions(),
                               lobStorageLocation(),
                               heap,
                               NULL, NULL,
                               NULL, (Int64*)&lobLen);
  if (rc < 0)
    {
      Lng32 intParam1 = -rc;
      ExRaiseSqlError(heap, diagsArea, 
                      (ExeErrorCode)(EXE_ERROR_FROM_LOB_INTERFACE),
                      NULL, &intParam1, 
                      NULL, NULL, (char*)"lobOperV2_.dmlInterface",
                      NULL, "LOB_DML_SELECT_LOBLENGTH");
      return ex_expr::EXPR_ERROR;
    }
  
  *(Int64*)result = lobLen;

  return ex_expr::EXPR_OK;
}

ex_expr::exp_return_type ExpLOBiud::getLobInputLengthV2(char *op_data[], 
                                                        Int64 &lobLen,
                                                        CollHeap *h,
                                                        ComDiagsArea **diagsArea)
{
  Int32 retcode = 0;

  ExLobV2 lobPtr(getExeGlobals()->getExLobGlobal()->getHeap(), NULL);
  lobPtr.lobGlobalHeap_ = getExeGlobals()->getExLobGlobal()->getHeap();

  lobLen = 0;
  
  if (fromFile() || fromExternal())
    {
      lobLen = getOperand(1)->getLength(op_data[1] - 
                                        getOperand(1)->getVCIndicatorLength());
      NAString sourceFileName(op_data[1], lobLen);

      lobPtr.statSourceFile((char*)sourceFileName.data(), lobLen);
    }
  else if (fromBuffer())   
    memcpy(&lobLen, op_data[2],sizeof(Int64)); // user specified buffer length
  else if (!fromEmpty())
    {
      lobLen = getOperand(1)->getLength();
      //If source is a varchar, find the actual length
      if (fromString() && ((getOperand(1)->getVCIndicatorLength() >0)))
        lobLen = getOperand(1)->getLength(op_data[1]- 
                                          getOperand(1)->getVCIndicatorLength());
    }
  
  return ex_expr::EXPR_OK;
}
 
