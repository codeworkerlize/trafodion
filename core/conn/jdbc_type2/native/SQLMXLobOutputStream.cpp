/**************************************************************************
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
 **************************************************************************/
//
// MODULE: SQLMXLobInputStream.cpp
//
#include <platform_ndcs.h>
#include <sql.h>
#include <sqlext.h>
#include "JdbcDriverGlobal.h"
#include "org_apache_trafodion_jdbc_t2_SQLMXLobOutputStream.h"
#include "SQLMXCommonFunctions.h"
#include "CoreCommon.h"
#include "SrvrCommon.h"
#include "SrvrOthers.h"
#include "CSrvrConnect.h"
#include "Debug.h"
#include "GlobalInformation.h"
#include "sqlcli.h"
#include "CSrvrStmt.h"

#ifdef __cplusplus
extern "C" {
#endif
JNIEXPORT void JNICALL Java_org_apache_trafodion_jdbc_t2_SQLMXLobOutputStream_writeChunk
  (JNIEnv *jenv, jobject jobj, jstring jServer, jlong jDialogueId, jlong jStmtId, jlong jTxId, 
			jshort jLobVersion, jlong jLobTableUid, jstring jLobLocator, jbyteArray jChunk, 
                         jint jOffset, jint jLength, jlong jPos)
{
   ExceptionStruct exception = {0,0,0};
   const char *lobLocator = jenv->GetStringUTFChars(jLobLocator, NULL);
   SRVR_STMT_HDL *lobUpdateStmt;
   IDL_long_long updateLobPos;
   BYTE *chunkBuf;
   int length = jLength;
   if (jLobVersion == 2) {
      if ((lobUpdateStmt = getLobUpdateStmt(jLobVersion, jDialogueId, jStmtId, jLobTableUid, &exception)) == NULL)
         goto func_exit;  
      chunkBuf = lobUpdateStmt->getBlobChunkBuffer();
      jenv->GetByteArrayRegion(jChunk, jOffset, jLength, (jbyte *)chunkBuf); 
      updateLobPos = jPos;
      lobUpdateStmt->updateLob((IDL_char *)lobLocator, chunkBuf, length, updateLobPos, &exception);
   } 
   else { 
   	chunkBuf = new BYTE[jLength];
   	jenv->GetByteArrayRegion(jChunk, jOffset, jLength, (jbyte *)chunkBuf); 

   	odbc_SQLSrvr_UpdateLob_sme_(NULL, NULL, &exception, jDialogueId, jStmtId, jLobVersion, jLobTableUid, 
                (IDL_char *)lobLocator, jPos, jOffset, jLength, chunkBuf);
   	delete chunkBuf;
   }
func_exit:
   jenv->ReleaseStringUTFChars(jLobLocator, lobLocator);
   switch (exception.exception_nr) {
      case CEE_SUCCESS:
         break;
      case odbc_SQLSvc_UpdateLob_SQLError_exn_:
         throwSQLException(jenv, &exception.u.SQLError);
         break;
      case odbc_SQLSvc_UpdateLob_SQLInvalidhandle_exn_:
         throwSQLException(jenv, INVALID_HANDLE_ERROR, NULL, "HY000", exception.u.SQLInvalidHandle.sqlcode);
         break;
      case odbc_SQLSvc_UpdateLob_ParamError_exn_:
         throwSQLException(jenv, MODULE_ERROR, exception.u.ParamError.ParamDesc, "HY000");
         break;
      case odbc_SQLSvc_UpdateLob_InvalidConnect_exn_:
      default:
         throwSQLException(jenv, PROGRAMMING_ERROR, NULL, "HY000", exception.exception_nr);
         break;
   }
}
#ifdef __cplusplus
}
#endif
