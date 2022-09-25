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
****************************************************************************
*
* File:         Helper functions for use by bin/clitest.cpp
* Description:  Test driver useing exe util cli interface
*
*
*
*
****************************************************************************
*/
#include "blobtest.h"

Int32 setSessionBegin(CliGlobals *cliglob)
{
  Int32 retcode = 0;
 ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
 retcode = cliInterface.executeImmediate("SET SESSION DEFAULT SQL_SESSION 'BEGIN';");
 return retcode;
}

Int32 setSessionEnd(CliGlobals *cliglob)
{
 Int32 retcode = 0;
 ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
 retcode = cliInterface.executeImmediate("SET SESSION DEFAULT SQL_SESSION 'END';");
 return retcode;
}

Int32 extractLengthOfLobColumn(CliGlobals *cliglob, char *lobHandle, 
			       Int64 &lengthOfLob, 
			       char *lobColumnName, char *tableName)
{
  Int32 retcode = 0;
  char * query = new char[4096];
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  //Use lob handle to retrieve the lob length.
  char lobLengthResult[200];
  str_cpy_all(lobLengthResult," ",200);
  Int32 lobLengthResultLen = 0;

  Int32 lobV2 = 0;
  Int64 objectUID = 0;
  SQL_EXEC_ExtractFieldsFromLobHandleStr(lobHandle, strlen(lobHandle),
                                         &lobV2, &objectUID, NULL, NULL);

  if (lobV2)
    sprintf(query,"extract loblength (table uid '%020ld', lob '%s') LOCATION %lu ",
            objectUID, lobHandle, (unsigned long)&lengthOfLob);
  else
    sprintf(query,"extract loblength (lob '%s') LOCATION %lu ",
            lobHandle, (unsigned long)&lengthOfLob);
  retcode = cliInterface.executeImmediate(query,lobLengthResult,&lobLengthResultLen,FALSE);

  delete query;
  return retcode;
 
}

Int32 extractLengthOfLobColumnPrepExec(CliGlobals *cliglob, char *lobHandle, 
                                       Int64 &lengthOfLob, 
                                       char *lobColumnName, char *tableName)
{
  Int32 retcode = 0;
  char * query = new char[4096];
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);

  //Use lob handle to retrieve the lob length.
  char lobLengthResult[200];
  str_cpy_all(lobLengthResult," ",200);
  Int32 lobLengthResultLen = 0;

  Int32 lobV2 = 0;
  Int64 objectUID = 0;
  SQL_EXEC_ExtractFieldsFromLobHandleStr(lobHandle, strlen(lobHandle),
                                         &lobV2, &objectUID, NULL, NULL);

  if (! lobV2)
    return extractLengthOfLobColumn(cliglob, lobHandle, lengthOfLob,
                                    lobColumnName, tableName);

  sprintf(query,"extract lobtobuffer (table uid '%020ld', LOB ?, LOCATION ?, SIZE ?)", 
          objectUID);

  retcode = cliInterface.executeImmediatePrepare(query);
  if (retcode <0)
    {
      delete query;
      return retcode;
    }

  Int64 dataLocationAddr = (Int64)lobLengthResult;

  Int64 dataSizeLen = -1;
  Int64 dataSizeAddr = (Int64)&dataSizeLen;
  
  Lng32 dummy;
  Int32 lobHandleOffset = 0;
  Int32 lobHandleIndOffset = 0;
  Int32 locationAddrOffset = 0;
  Int32 locationAddrIndOffset = 0;
  Int32 sizeAddrOffset = 0;
  Int32 sizeAddrIndOffset = 0;
  Int32 positionAddrOffset = 0;
  Int32 positionAddrIndOffset = 0;
  cliInterface.getAttributes(1, TRUE,
                                dummy, dummy, dummy, &lobHandleIndOffset, &lobHandleOffset);
  cliInterface.getAttributes(2, TRUE,
                                dummy, dummy, dummy, &locationAddrIndOffset, &locationAddrOffset);
  cliInterface.getAttributes(3, TRUE,
                                dummy, dummy, dummy, &sizeAddrIndOffset, &sizeAddrOffset);

  Int16 nullV = 0;
  if (lobHandleIndOffset >= 0)
    memcpy(&cliInterface.inputBuf()[lobHandleIndOffset], (char*)&nullV, sizeof(nullV));
  memcpy(&cliInterface.inputBuf()[lobHandleOffset], lobHandle, strlen(lobHandle));

  if (locationAddrIndOffset >= 0)
    memcpy(&cliInterface.inputBuf()[locationAddrIndOffset], (char*)&nullV, sizeof(nullV));  
  memcpy(&cliInterface.inputBuf()[locationAddrOffset], (char*)&dataLocationAddr, sizeof(Int64));

  if (sizeAddrIndOffset >= 0)
    memcpy(&cliInterface.inputBuf()[sizeAddrIndOffset], (char*)&nullV, sizeof(nullV));  
  memcpy(&cliInterface.inputBuf()[sizeAddrOffset], (char*)&dataSizeAddr, sizeof(Int64));

  //    sprintf(query,"extract loblength (lob '%s') LOCATION %lu ",
  //            lobHandle, (unsigned long)&lengthOfLob);

  retcode = cliInterface.clearExecFetchClose(NULL, NULL);
  if (retcode <0)
    {
      delete query;
      return retcode;
    }
  
  lengthOfLob = *(Int64*)lobLengthResult;

  delete query;
  return retcode;
 
}

Int32 extractLobHandle(CliGlobals *cliglob, char *& lobHandle, 
		       char *lobColumnName, char *tableName)
{
  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  char * query = new char[4096];
  Int32 lobHandleLen = 0;
  sprintf(query,"select %s from %s",lobColumnName,tableName);
  
  retcode = cliInterface.executeImmediate(query,lobHandle,&lobHandleLen,FALSE);

  lobHandle[lobHandleLen]='\0';
  delete query;
  
  return retcode;
 
}

Int32 extractLobToBufferV2(CliGlobals *cliglob, char * lobHandle, Int64 &lengthOfLob, 
                           char *lobColumnName, char *tableName)
{
  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  // Extract lob data into a buffer.
  char * query = new char [500];
  
  char *lobFinalBuf = new char[lengthOfLob];
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  UInt64 lobExtractLen = 10000;
  UInt64 *inputOutputAddr = &lobExtractLen;
  char *lobDataBuf = new char[lobExtractLen];

  Int32 lobV2 = 0;
  Int64 objectUID = 0;
  SQL_EXEC_ExtractFieldsFromLobHandleStr(lobHandle, strlen(lobHandle),
                                         &lobV2, &objectUID, NULL, NULL);

  sprintf(query,"extract lobtobuffer(table uid '%020ld', lob '%s', LOCATION %lu, SIZE %lu) ", 
          objectUID, lobHandle, (unsigned long)lobDataBuf, (unsigned long) inputOutputAddr);

  retcode = cliInterface.executeImmediatePrepare(query);
  short i = 0;

  retcode = cliInterface.exec(NULL, NULL);
  while ((retcode != 100) && !(retcode<0))
    {    
      retcode = cliInterface.fetch();
      if ((retcode == 100) || (retcode < 0))
        continue;

      if (retcode >= 0)
	{
          memcpy((char*)&(lobFinalBuf[i]),(char *)lobDataBuf,*inputOutputAddr);
          i += lobExtractLen;
	}      
    } // while

  lobExtractLen = 0;
  retcode = cliInterface.fetch();

  retcode = cliInterface.close();

  if (retcode ==100 || retcode ==0)
    {
      FILE * lobFileId = fopen("lob_output_file","w");
  
      int byteCount=fwrite(lobFinalBuf,sizeof(char),lengthOfLob, lobFileId);
      cout << "Writing " << byteCount << " bytes from user buffer to file lob_output_file in current directory" << endl;

      fclose(lobFileId);
    }

  delete  lobFinalBuf;
  delete query;
  delete lobDataBuf;

  return retcode;
}

//#ifdef __ignore
Int32 extractLobToBufferV2chunks(CliGlobals *cliglob, char * lobHandle, Int64 &lengthOfLob, 
                                 char *lobColumnName, char *tableName)
{
  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  // Extract lob data into a buffer.
  char * query = new char [500];
  
  char *lobFinalBuf = new char[lengthOfLob+1];
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  UInt64 lobExtractLen = 5;
  //UInt64 lobExtractLen = 10;
  UInt64 *inputOutputAddr = &lobExtractLen;
  char *lobDataBuf = new char[lobExtractLen];

  Int64 position = 1;
  Int64 positionAddr = (Int64)&position;
 
  Int32 lobV2 = 0;
  Int64 objectUID = 0;
  SQL_EXEC_ExtractFieldsFromLobHandleStr(lobHandle, strlen(lobHandle),
                                         &lobV2, &objectUID, NULL, NULL);

  sprintf(query,"extract lobtobuffer(table uid '%020ld', lob '%s', LOCATION %lu, SIZE %lu, POSITION %lu) ", 
          objectUID, lobHandle, (unsigned long)lobDataBuf, 
          (unsigned long) inputOutputAddr, (unsigned long) positionAddr);

  retcode = cliInterface.executeImmediatePrepare(query);
  short i = 0;

  while ((retcode != 100) && !(retcode<0))
    {    
      retcode = cliInterface.clearExecFetchClose(NULL, NULL);
      if (retcode < 0)
        continue;

      if (retcode >= 0)
	{
          memcpy((char*)&(lobFinalBuf[i]),(char *)lobDataBuf,*inputOutputAddr);
          i += lobExtractLen;
	}      
    } // while

  if (retcode ==100 || retcode ==0)
    {
      FILE * lobFileId = fopen("lob_output_file","w");
  
      int byteCount=fwrite(lobFinalBuf,sizeof(char),lengthOfLob, lobFileId);
      cout << "Writing " << byteCount << " bytes from user buffer to file lob_output_file in current directory" << endl;

      lobFinalBuf[lengthOfLob] = 0;
      cout << "Data: " << lobFinalBuf << endl;
      fclose(lobFileId);
    }

  sprintf(query,"extract lobtobuffer(table uid '%020ld', lob '%s', LOCATION %lu, SIZE %lu) ", 
          objectUID, lobHandle, (unsigned long)lobDataBuf, (unsigned long) inputOutputAddr);
  
  lobExtractLen = 0;
  retcode = cliInterface.executeImmediatePrepare(query);
  cliInterface.clearExecFetchClose(NULL,NULL,statusBuf, &statusBufLen);

  delete  lobFinalBuf;
  delete query;
  delete lobDataBuf;

  return retcode;
}
//#endif

#ifdef __ignore
Int32 extractLobToBufferV2chunks(CliGlobals *cliglob, char * lobHandle, Int64 &lengthOfLob, 
                                 char *lobColumnName, char *tableName)
{
  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  // Extract lob data into a buffer.
  char * query = new char [500];
  
  char *lobFinalBuf = new char[lengthOfLob+1];
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  UInt64 lobExtractLen = 5;
  //UInt64 lobExtractLen = 10;
  UInt64 *inputOutputAddr = &lobExtractLen;
  char *lobDataBuf = new char[lobExtractLen];

  Int64 position = 1;
  Int64 positionAddr = (Int64)&position;
 
  Int32 lobV2 = 0;
  Int64 objectUID = 0;
  SQL_EXEC_ExtractFieldsFromLobHandleStr(lobHandle, strlen(lobHandle),
                                         &lobV2, &objectUID, NULL, NULL);

  sprintf(query,"extract lobtobuffer(table uid '%020ld', lob '%s', LOCATION %lu, SIZE %lu, POSITION %lu) ", 
          objectUID, lobHandle, (unsigned long)lobDataBuf, 
          (unsigned long) inputOutputAddr, (unsigned long) positionAddr);

  retcode = cliInterface.executeImmediatePrepare(query);
  short i = 0;

  retcode = cliInterface.exec(NULL, NULL);
  while ((retcode != 100) && !(retcode<0))
    {    
      retcode = cliInterface.fetch();
      if (retcode < 0)
        continue;

      if (retcode >= 0)
	{
          memcpy((char*)&(lobFinalBuf[i]),(char *)lobDataBuf,*inputOutputAddr);
          i += lobExtractLen;
	}      
    } // while

  cliInterface.close();
  if (retcode ==100 || retcode ==0)
    {
      retcode = cliInterface.close();

      FILE * lobFileId = fopen("lob_output_file","w");
  
      int byteCount=fwrite(lobFinalBuf,sizeof(char),lengthOfLob, lobFileId);
      cout << "Writing " << byteCount << " bytes from user buffer to file lob_output_file in current directory" << endl;

      lobFinalBuf[lengthOfLob] = 0;
      cout << "Data: " << lobFinalBuf << endl;
      fclose(lobFileId);
    }

  sprintf(query,"extract lobtobuffer(table uid '%020ld', lob '%s', LOCATION %lu, SIZE %lu) ", 
          objectUID, lobHandle, (unsigned long)lobDataBuf, (unsigned long) inputOutputAddr);
  
  lobExtractLen = 0;
  retcode = cliInterface.executeImmediatePrepare(query);
  cliInterface.clearExecFetchClose(NULL,NULL,statusBuf, &statusBufLen);

  delete  lobFinalBuf;
  delete query;
  delete lobDataBuf;

  return retcode;
}
#endif

Int32 extractLobToBufferV2chunksPrepExec(CliGlobals *cliglob, char * lobHandle, Int64 &lengthOfLob, 
                                         char *lobColumnName, char *tableName)
{
  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  // Extract lob data into a buffer.
  char * query = new char [500];
  
  char *lobFinalBuf = new char[lengthOfLob+1];
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  UInt64 lobExtractLen = 10;
  UInt64 *inputOutputAddr = &lobExtractLen;
  char *lobDataBuf = new char[lobExtractLen];

  Int64 position = 1;
  Int64 positionAddr = (Int64)&position;
 
  Int32 lobV2 = 0;
  Int64 objectUID = 0;
  SQL_EXEC_ExtractFieldsFromLobHandleStr(lobHandle, strlen(lobHandle),
                                         &lobV2, &objectUID, NULL, NULL);

  sprintf(query,"extract lobtobuffer(table uid '%020ld', lob '%s', LOCATION %lu, SIZE %lu, POSITION %lu) ", 
          objectUID, lobHandle, (unsigned long)lobDataBuf, 
          (unsigned long) inputOutputAddr, (unsigned long) positionAddr);

  retcode = cliInterface.executeImmediatePrepare(query);

  // execute statement first time
  retcode = cliInterface.exec(NULL, NULL);
  if (retcode < 0)
    {
      return retcode;
    }

  short i = 0;

  while ((retcode != 100) && !(retcode<0))
    {    
      retcode = cliInterface.fetch();
      if (retcode < 0)
        continue;

      if (retcode >= 0)
	{
          memcpy((char*)&(lobFinalBuf[i]),(char *)lobDataBuf,*inputOutputAddr);
          i += lobExtractLen;
	}      
    } // while

  if (retcode ==100 || retcode ==0)
    {
      FILE * lobFileId = fopen("lob_output_file","w");
  
      int byteCount=fwrite(lobFinalBuf,sizeof(char),lengthOfLob, lobFileId);
      cout << "Writing " << byteCount << " bytes from user buffer to file lob_output_file in current directory" << endl;

      lobFinalBuf[lengthOfLob] = 0;
      cout << "Data: " << lobFinalBuf << endl;
      fclose(lobFileId);
    }

  retcode = cliInterface.close();

  // execute statement second time
  retcode = cliInterface.exec(NULL, NULL);
  if (retcode < 0)
    return retcode;

  i = 0;

  while ((retcode != 100) && !(retcode<0))
    {    
      retcode = cliInterface.fetch();
      if (retcode < 0)
        continue;

      if (retcode >= 0)
	{
          memcpy((char*)&(lobFinalBuf[i]),(char *)lobDataBuf,*inputOutputAddr);
          i += lobExtractLen;
	}      
    } // while

  if (retcode ==100 || retcode ==0)
    {
      FILE * lobFileId = fopen("lob_output_file","w");
  
      int byteCount=fwrite(lobFinalBuf,sizeof(char),lengthOfLob, lobFileId);
      cout << "Writing " << byteCount << " bytes from user buffer to file lob_output_file in current directory" << endl;

      lobFinalBuf[lengthOfLob] = 0;
      cout << "Data: " << lobFinalBuf << endl;
      fclose(lobFileId);
    }

  lobExtractLen = 0;
  retcode = cliInterface.fetch();

  retcode = cliInterface.close();

  delete  lobFinalBuf;
  delete query;
  delete lobDataBuf;

  return retcode;
}

Int32 extractLobToBuffer(CliGlobals *cliglob, char * lobHandle, Int64 &lengthOfLob, 
                         char *lobColumnName, char *tableName)
{
  Int32 lobV2 = 0;
  Int64 objectUID = 0;
  SQL_EXEC_ExtractFieldsFromLobHandleStr(lobHandle, strlen(lobHandle),
                                         &lobV2, &objectUID, NULL, NULL);

  if (lobV2)
    {
      return extractLobToBufferV2chunksPrepExec(cliglob, lobHandle, lengthOfLob,
                                                  lobColumnName, tableName);
    }

  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  // Extract lob data into a buffer.
  char * query = new char [500];
  
  char *lobFinalBuf = new char[lengthOfLob+1];
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  UInt64 lobExtractLen = 5;
  UInt64 *inputOutputAddr = &lobExtractLen;
  char *lobDataBuf = new char[lobExtractLen];

  sprintf(query,"extract lobtobuffer(lob '%s', LOCATION %lu, SIZE %lu) ", 
          lobHandle, (unsigned long)lobDataBuf, (unsigned long) inputOutputAddr);
  
  retcode = cliInterface.executeImmediatePrepare(query);
  short i = 0;

  while ((retcode != 100) && !(retcode<0))
    {    
      retcode = cliInterface.clearExecFetchClose(NULL,NULL,statusBuf, &statusBufLen);
      if (retcode>= 0)
	{
          memcpy((char*)&(lobFinalBuf[i]),(char *)lobDataBuf,*inputOutputAddr);
          i += lobExtractLen;
	}
    }

  if (retcode ==100 || retcode ==0)
    {
      FILE * lobFileId = fopen("lob_output_file","w");
  
      int byteCount=fwrite(lobFinalBuf,sizeof(char),lengthOfLob, lobFileId);
      cout << "Writing " << byteCount << " bytes from user buffer to file lob_output_file in current directory" << endl;

      lobFinalBuf[lengthOfLob] = 0;
      cout << "Data: " << lobFinalBuf << endl;
      fclose(lobFileId);
    }

  sprintf(query,"extract lobtobuffer(lob '%s', LOCATION %lu, SIZE %lu) ", lobHandle, (unsigned long)lobDataBuf, (unsigned long) inputOutputAddr);

  lobExtractLen = 0;
  retcode = cliInterface.executeImmediatePrepare(query);
  cliInterface.clearExecFetchClose(NULL,NULL,statusBuf, &statusBufLen);

  delete  lobFinalBuf;
  delete query;
  delete lobDataBuf;
    
  return retcode;
}

Int32 extractLobToFileInChunksV2(CliGlobals *cliglob,  char * lobHandle, char *filename,Int64 &lengthOfLob, 
                                 char *lobColumnName, char *tableName)
{
  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);

  // Extract lob data into a buffer.
  char * query = new char [500];
  
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  Int64 inLobExtractLen = 10000;
  Int64 lobExtractLen = 10000;
  char *lobDataBuf = new char[lobExtractLen];
  Int64 *inputOutputAddr = &lobExtractLen;

  sprintf(query,"extract lobtobuffer(lob '%s', LOCATION %lu, SIZE %lu) ", lobHandle, (unsigned long)lobDataBuf, (unsigned long)inputOutputAddr);
 
  retcode = cliInterface.executeImmediatePrepare(query);
  retcode = cliInterface.exec(NULL, NULL);
  short i = 0;
  FILE * lobFileId = fopen(filename,"a+");
  int byteCount = 0;
  while ((retcode != 100) && !(retcode<0))
    {    
      lobExtractLen = inLobExtractLen;
      retcode = cliInterface.fetch();

      if (retcode < 0)
        continue;

      byteCount=fwrite(lobDataBuf,sizeof(char),*inputOutputAddr, lobFileId);
      cout << "Wrote " << byteCount << " bytes to file : " << filename << endl;
    } // while

  lobExtractLen = 0;
  retcode = cliInterface.fetch();

  retcode = cliInterface.close();

  fclose(lobFileId);
 
  delete query;
  delete lobDataBuf;

  return retcode;
}

Int32 extractLobToFileInChunks(CliGlobals *cliglob,  char * lobHandle, char *filename,Int64 &lengthOfLob, 
				char *lobColumnName, char *tableName)
{
  Int32 lobV2 = 0;
  Int64 objectUID = 0;
  SQL_EXEC_ExtractFieldsFromLobHandleStr(lobHandle, strlen(lobHandle),
                                         &lobV2, &objectUID, NULL, NULL);

  if (lobV2)
    {
      return extractLobToFileInChunksV2(cliglob, lobHandle, filename,
                                        lengthOfLob, lobColumnName, tableName);
    }

  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);

  // Extract lob data into a buffer.
  char * query = new char [500];
  
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  Int64 inLobExtractLen = 10000;
  Int64 lobExtractLen = 10000;
  char *lobDataBuf = new char[lobExtractLen];
  Int64 *inputOutputAddr = &lobExtractLen;

  sprintf(query,"extract lobtobuffer(lob '%s', LOCATION %lu, SIZE %lu) ", lobHandle, (unsigned long)lobDataBuf, (unsigned long)inputOutputAddr);
 
  retcode = cliInterface.executeImmediatePrepare(query);
  short i = 0;
  FILE * lobFileId = fopen(filename,"a+");
  int byteCount = 0;
  while ((retcode != 100) && !(retcode<0))
    {    
      lobExtractLen = inLobExtractLen;

      retcode = cliInterface.clearExecFetchClose(NULL,NULL,statusBuf, &statusBufLen);
      if (retcode>= 0)
      {
        byteCount=fwrite(lobDataBuf,sizeof(char),*inputOutputAddr, lobFileId);
        cout << "Wrote " << byteCount << " bytes to file : " << filename << endl;
      }
    } // while

  lobExtractLen = 0;
  sprintf(query,"extract lobtobuffer(lob '%s', LOCATION %lu, SIZE %lu) ", lobHandle, (unsigned long)lobDataBuf, (unsigned long)inputOutputAddr);
  retcode = cliInterface.executeImmediatePrepare(query);
  cliInterface.clearExecFetchClose(NULL,NULL,statusBuf, &statusBufLen);

  fclose(lobFileId);

 
  delete query;
  delete lobDataBuf;
    

  return retcode;

}


Int32 insertBufferToLob(CliGlobals *cliglob, char *tableName)
{
  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  // Extract lob data into a buffer.
  char * query = new char [500];
  
 
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  Int64 lobInsertLen = 10;
  char *lobDataBuf = new char[lobInsertLen];
  memcpy(lobDataBuf, "xxxxxyyyyy",10);
  sprintf(query,"insert into %s values (1, buffertolob (LOCATION %lu, SIZE %ld))", tableName, (unsigned long)lobDataBuf, lobInsertLen);
 
 
  retcode = cliInterface.executeImmediate(query);
  if (retcode <0)
    {
      cliInterface.executeImmediate("rollback work");
      delete query;
      delete lobDataBuf;
    
      return retcode;
    }

  retcode = cliInterface.executeImmediate("commit work");
  delete query;
  delete lobDataBuf;
    

  return retcode;

}


Int32 updateBufferToLob(CliGlobals *cliglob, char *tableName, char *columnName)
{
  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  // Extract lob data into a buffer.
  char * query = new char [500];
  
 
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  Int64 lobUpdateLen = 20;
  char *lobDataBuf = new char[lobUpdateLen];
  memcpy(lobDataBuf, "zzzzzzzzzzzzzzzzzzzz",20);
  sprintf(query,"update %s set %s= buffertolob(LOCATION %lu, SIZE %ld)", tableName,columnName, (unsigned long)lobDataBuf, lobUpdateLen);
 
 
  retcode = cliInterface.executeImmediate(query);
  if (retcode <0)
    {
      cliInterface.executeImmediate("rollback work");
      delete query;
      delete lobDataBuf;
      return retcode;
    }

  retcode = cliInterface.executeImmediate("commit work");
  delete query;
  delete lobDataBuf;
    

  return retcode;

}

Int32 updateAppendBufferToLob(CliGlobals *cliglob, char *tableName, char *columnName)
{
  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  // Extract lob data into a buffer.
  char * query = new char [500];
  
 
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  Int64 lobUpdateLen = 15;
  char *lobDataBuf = new char[lobUpdateLen];
  memcpy(lobDataBuf, "aaaaabbbbbccccc",15);
  sprintf(query,"update %s set %s=buffertolob (LOCATION %lu, SIZE %ld,append)", tableName, columnName, (unsigned long)lobDataBuf, lobUpdateLen);
 
 
  retcode = cliInterface.executeImmediate(query);
  if (retcode <0)
    {
      cliInterface.executeImmediate("rollback work");
      delete query;
      delete lobDataBuf;
      return retcode;
    }

  retcode = cliInterface.executeImmediate("commit work");
  delete query;
  delete lobDataBuf;
    
  return retcode;
}

Int32 updateBufferToLobHandleV2(CliGlobals *cliglob,char *lobHandle)
{
  Int32 lobV2 = 0;
  Int64 objectUID = 0;
  SQL_EXEC_ExtractFieldsFromLobHandleStr(lobHandle, strlen(lobHandle),
                                         &lobV2, &objectUID, NULL, NULL);

  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);

  // update lob data into via a buffer.
  char * query = new char [500];
  
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  Int64 lobUpdateLen = 20;

  char *lobDataBuf = new char[lobUpdateLen];
  Int64 dataLocationAddr = (Int64)lobDataBuf;

  Int64 dataSizeAddr = (Int64)&lobUpdateLen;

  Int64 position = -1;
  Int64 positionAddr = (Int64)&position;
 
  memcpy(lobDataBuf, "zzzzzzzzzzzzzzzzzzzz",20);

  sprintf(query,"update lob (table uid '%020ld', LOB '%s' , LOCATION %ld, SIZE %ld)", objectUID, lobHandle, dataLocationAddr, dataSizeAddr);
  retcode = cliInterface.executeImmediate(query);
  if (retcode <0)
    {
      cliInterface.executeImmediate("rollback work");
      delete query;
      delete lobDataBuf;
      return retcode;
    }

  //  sprintf(query,"update lob (table uid '%020ld', LOB ?, LOCATION ?, SIZE ?, append)", objectUID);
  sprintf(query,"update lob (table uid '%020ld', LOB ?, LOCATION ?, SIZE ?, POSITION ?)", objectUID);
  retcode = cliInterface.executeImmediatePrepare(query);
  if (retcode <0)
    {
      delete query;
      delete lobDataBuf;
      return retcode;
    }
  
  Lng32 dummy;
  Int32 lobHandleOffset = 0;
  Int32 lobHandleIndOffset = 0;
  Int32 locationAddrOffset = 0;
  Int32 locationAddrIndOffset = 0;
  Int32 sizeAddrOffset = 0;
  Int32 sizeAddrIndOffset = 0;
  Int32 positionAddrOffset = 0;
  Int32 positionAddrIndOffset = 0;
  cliInterface.getAttributes(1, TRUE,
                                dummy, dummy, dummy, &lobHandleIndOffset, &lobHandleOffset);
  cliInterface.getAttributes(2, TRUE,
                                dummy, dummy, dummy, &locationAddrIndOffset, &locationAddrOffset);
  cliInterface.getAttributes(3, TRUE,
                                dummy, dummy, dummy, &sizeAddrIndOffset, &sizeAddrOffset);
  cliInterface.getAttributes(4, TRUE,
                                dummy, dummy, dummy, &positionAddrIndOffset, &positionAddrOffset);


  Int16 nullV = 0;
  if (lobHandleIndOffset >= 0)
    memcpy(&cliInterface.inputBuf()[lobHandleIndOffset], (char*)&nullV, sizeof(nullV));
  memcpy(&cliInterface.inputBuf()[lobHandleOffset], lobHandle, strlen(lobHandle));

  if (locationAddrIndOffset >= 0)
    memcpy(&cliInterface.inputBuf()[locationAddrIndOffset], (char*)&nullV, sizeof(nullV));  
  memcpy(&cliInterface.inputBuf()[locationAddrOffset], (char*)&dataLocationAddr, sizeof(Int64));

  if (sizeAddrIndOffset >= 0)
    memcpy(&cliInterface.inputBuf()[sizeAddrIndOffset], (char*)&nullV, sizeof(nullV));  
  memcpy(&cliInterface.inputBuf()[sizeAddrOffset], (char*)&dataSizeAddr, sizeof(Int64));

  if (positionAddrIndOffset >= 0)
    memcpy(&cliInterface.inputBuf()[positionAddrIndOffset], (char*)&nullV, sizeof(nullV));  
  memcpy(&cliInterface.inputBuf()[positionAddrOffset], (char*)&positionAddr, sizeof(Int64));

  lobUpdateLen = 3;
  memcpy(lobDataBuf, "xyz",3);
  retcode = cliInterface.clearExecFetchClose(NULL, NULL);
  if (retcode <0)
    {
      cliInterface.executeImmediate("rollback work");
      delete query;
      delete lobDataBuf;
      return retcode;
    }

  position = 1;
  lobUpdateLen = 4;
  memcpy(lobDataBuf, "abcd",4);
  retcode = cliInterface.clearExecFetchClose(NULL, NULL);
  if (retcode <0)
    {
      cliInterface.executeImmediate("rollback work");
      delete query;
      delete lobDataBuf;
      return retcode;
    }
  
  retcode = cliInterface.executeImmediate("commit work");
  delete query;
  delete lobDataBuf;
    
  return retcode;
}

Int32 updateBufferToLobHandleV2multiple(CliGlobals *cliglob,char *lobHandle,
                                        char *lobHandle2)
{
  Int32 lobV2 = 0;
  Int64 objectUID = 0;
  SQL_EXEC_ExtractFieldsFromLobHandleStr(lobHandle, strlen(lobHandle),
                                         &lobV2, &objectUID, NULL, NULL);

  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);

  // update lob data into via a buffer.
  char * query = new char [500];
  
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  Int64 lobUpdateLen = 20;

  char *lobDataBuf = new char[lobUpdateLen];
  Int64 dataLocationAddr = (Int64)lobDataBuf;

  Int64 dataSizeAddr = (Int64)&lobUpdateLen;

  Int64 position = -1;
  Int64 positionAddr = (Int64)&position;
 
  memcpy(lobDataBuf, "zzzzzzzzzzzzzzzzzzzz",20);

  sprintf(query,"update lob (table uid '%020ld', LOB ?, LOCATION ?, SIZE ?, POSITION ?)", objectUID);
  retcode = cliInterface.executeImmediatePrepare(query);
  if (retcode <0)
    {
      delete query;
      delete lobDataBuf;
      return retcode;
    }
  
  Lng32 dummy;
  Int32 lobHandleOffset = 0;
  Int32 lobHandleIndOffset = 0;
  Int32 locationAddrOffset = 0;
  Int32 locationAddrIndOffset = 0;
  Int32 sizeAddrOffset = 0;
  Int32 sizeAddrIndOffset = 0;
  Int32 positionAddrOffset = 0;
  Int32 positionAddrIndOffset = 0;
  cliInterface.getAttributes(1, TRUE,
                                dummy, dummy, dummy, &lobHandleIndOffset, &lobHandleOffset);
  cliInterface.getAttributes(2, TRUE,
                                dummy, dummy, dummy, &locationAddrIndOffset, &locationAddrOffset);
  cliInterface.getAttributes(3, TRUE,
                                dummy, dummy, dummy, &sizeAddrIndOffset, &sizeAddrOffset);
  cliInterface.getAttributes(4, TRUE,
                                dummy, dummy, dummy, &positionAddrIndOffset, &positionAddrOffset);


  Int16 nullV = 0;
  if (lobHandleIndOffset >= 0)
    memcpy(&cliInterface.inputBuf()[lobHandleIndOffset], (char*)&nullV, sizeof(nullV));

  if (locationAddrIndOffset >= 0)
    memcpy(&cliInterface.inputBuf()[locationAddrIndOffset], (char*)&nullV, sizeof(nullV));  
  memcpy(&cliInterface.inputBuf()[locationAddrOffset], (char*)&dataLocationAddr, sizeof(Int64));

  if (sizeAddrIndOffset >= 0)
    memcpy(&cliInterface.inputBuf()[sizeAddrIndOffset], (char*)&nullV, sizeof(nullV));  
  memcpy(&cliInterface.inputBuf()[sizeAddrOffset], (char*)&dataSizeAddr, sizeof(Int64));

  if (positionAddrIndOffset >= 0)
    memcpy(&cliInterface.inputBuf()[positionAddrIndOffset], (char*)&nullV, sizeof(nullV));  
  memcpy(&cliInterface.inputBuf()[positionAddrOffset], (char*)&positionAddr, sizeof(Int64));

  lobUpdateLen = 3;
  memcpy(lobDataBuf, "xyz",3);

  // update lobhandle 1
  memcpy(&cliInterface.inputBuf()[lobHandleOffset], lobHandle, strlen(lobHandle));
  retcode = cliInterface.clearExecFetchClose(NULL, NULL);
  if (retcode <0)
    {
      cliInterface.executeImmediate("rollback work");
      delete query;
      delete lobDataBuf;
      return retcode;
    }

  // update lobhandle 2
  memcpy(&cliInterface.inputBuf()[lobHandleOffset], lobHandle2, strlen(lobHandle2));
  retcode = cliInterface.clearExecFetchClose(NULL, NULL);
  if (retcode <0)
    {
      cliInterface.executeImmediate("rollback work");
      delete query;
      delete lobDataBuf;
      return retcode;
    }
  
  retcode = cliInterface.executeImmediate("commit work");
  delete query;
  delete lobDataBuf;
    
  return retcode;
}

Int32 updateBufferToLobHandle(CliGlobals *cliglob, char *lobHandle, char *lobHandle2)
{
  Int32 lobV2 = 0;
  Int64 objectUID = 0;
  SQL_EXEC_ExtractFieldsFromLobHandleStr(lobHandle, strlen(lobHandle),
                                         &lobV2, &objectUID, NULL, NULL);

  if (lobV2)
    {
      if (lobHandle2)
        return updateBufferToLobHandleV2multiple(cliglob, lobHandle, lobHandle2);
      else
        return updateBufferToLobHandleV2(cliglob, lobHandle);
    }

  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  // update lob data into via a buffer.
  char * query = new char [500];
  
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  Int64 lobUpdateLen = 20;
  Int64 *inputOutputAddr = &lobUpdateLen;
  char *lobDataBuf = new char[lobUpdateLen];
  memcpy(lobDataBuf, "zzzzzzzzzzzzzzzzzzzz",20);

  sprintf(query,"update lob (LOB '%s' , LOCATION %ld, SIZE %ld)", lobHandle, (Int64)lobDataBuf, lobUpdateLen);
 
  retcode = cliInterface.executeImmediate(query);
  if (retcode <0)
    {
      cliInterface.executeImmediate("rollback work");
      delete query;
      delete lobDataBuf;
      return retcode;
    }

  if (lobHandle2)
    {
      sprintf(query,"update lob (LOB '%s' , LOCATION %ld, SIZE %ld)", lobHandle2, (Int64)lobDataBuf, lobUpdateLen);
      
      retcode = cliInterface.executeImmediate(query);
      if (retcode <0)
        {
          cliInterface.executeImmediate("rollback work");
          delete query;
          delete lobDataBuf;
          return retcode;
        }
    }

  retcode = cliInterface.executeImmediate("commit work");
  delete query;
  delete lobDataBuf;

  return retcode;
}

Int32 updateAppendBufferToLobHandle(CliGlobals *cliglob,char *handle, Int64 lobUpdateLen, Int64 sourceAddress)
{
  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  // update lob data into via a buffer.
  char * query = new char [500];
  
 
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  char *lobDataBuf = new char[lobUpdateLen];
  memcpy(lobDataBuf, (char *)sourceAddress,lobUpdateLen);
  sprintf(query,"update lob (LOB '%s' , LOCATION %lu, SIZE %ld,append )", handle, (unsigned long)lobDataBuf, lobUpdateLen);
 
 
  retcode = cliInterface.executeImmediate(query);
  if (retcode <0)
    {
      cliInterface.executeImmediate("rollback work");
      delete query;
      delete lobDataBuf;
      return retcode;
    }

  retcode = cliInterface.executeImmediate("commit work");
  delete query;
  delete lobDataBuf;
    

  return retcode;

}


Int32 updateTruncateLobHandle(CliGlobals *cliglob,char *handle)
{
  Int32 retcode = 0;
  ExeCliInterface cliInterface((cliglob->currContext())->exHeap(), (Int32)SQLCHARSETCODE_UTF8, cliglob->currContext(),NULL);
  // update lob data into via a buffer.
  char * query = new char [500];
  
 
  char statusBuf[200] = {'\0'};
  Int32 statusBufLen = 0;
  Int64 lobUpdateLen = 20;
  char *lobDataBuf = new char[lobUpdateLen];
  memcpy(lobDataBuf, "zzzzzzzzzzzzzzzzzzzz",20);
  sprintf(query,"update lob (LOB '%s' , empty_blob())", handle);
 
 
  retcode = cliInterface.executeImmediate(query);
  if (retcode <0)
    {
      cliInterface.executeImmediate("rollback work");
      delete query;
      delete lobDataBuf;
      return retcode;
    }

  retcode = cliInterface.executeImmediate("commit work");
  delete query;
  delete lobDataBuf;
    

  return retcode;

}
