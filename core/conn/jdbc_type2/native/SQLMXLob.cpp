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
// MODULE: SQLMXLob.cpp
//
#include <platform_ndcs.h>
#include <sql.h>
#include <sqlext.h>
#include "JdbcDriverGlobal.h"
#include "org_apache_trafodion_jdbc_t2_SQLMXLob.h"
#include "SQLMXCommonFunctions.h"
#include "CoreCommon.h"
#include "SrvrCommon.h"
#include "SrvrOthers.h"
#include "CSrvrConnect.h"
#include "Debug.h"
#include "GlobalInformation.h"
#include "sqlcli.h"

#ifdef __cplusplus
extern "C" {
#endif
JNIEXPORT jlong JNICALL Java_org_apache_trafodion_jdbc_t2_SQLMXLob_getLobLength
  (JNIEnv *jenv, jobject jobj, jstring jServer, jlong jDialogueId, jlong jStmtId,  
		jshort jLobVersion, jlong jLobTableUid, jstring jLobLocator)
{
   ExceptionStruct exception = {0,0,0};

   const char *lobLocator = jenv->GetStringUTFChars(jLobLocator, NULL);
   IDL_long_long lobLength;
   IDL_long_long extractLen = -1;
   BYTE *chunkBuf = NULL;

   odbc_SQLSrvr_ExtractLob_sme_(NULL, NULL, &exception, jDialogueId, jStmtId, 0, jLobVersion, jLobTableUid, 
                (IDL_char *)lobLocator, lobLength, extractLen, chunkBuf);
   jenv->ReleaseStringUTFChars(jLobLocator, lobLocator);
   switch (exception.exception_nr) {
      case CEE_SUCCESS:
	 return *(Int64 *)chunkBuf;
      case odbc_SQLSvc_ExtractLob_SQLError_exn_:
         throwSQLException(jenv, &exception.u.SQLError);
         break;
      case odbc_SQLSvc_ExtractLob_SQLInvalidhandle_exn_:
         throwSQLException(jenv, INVALID_HANDLE_ERROR, NULL, "HY000", exception.u.SQLInvalidHandle.sqlcode);
         break;
      case odbc_SQLSvc_ExtractLob_InvalidConnect_exn_:
      default:
         throwSQLException(jenv, PROGRAMMING_ERROR, NULL, "HY000", exception.exception_nr);
         break;
   }
   return 0;
}

JNIEXPORT void JNICALL Java_org_apache_trafodion_jdbc_t2_SQLMXLob_truncate
  (JNIEnv *jenv, jobject jobj, jstring jServer, jlong jDialogueId, jlong jStmtId,  
		jshort jLobVersion, jlong jLobTableUid, jstring jLobLocator, jlong jPos)
{
   ExceptionStruct exception = {0,0,0};
   const char *lobLocator = jenv->GetStringUTFChars(jLobLocator, NULL);
   SRVR_STMT_HDL *lobUpdateStmt;
   IDL_long_long updateLobPos;
   BYTE *chunkBuf;
   int length = 0;
   if (jLobVersion == 2) {
      if ((lobUpdateStmt = getLobUpdateStmt(jLobVersion, jDialogueId, jStmtId, jLobTableUid, &exception)) == NULL)
         goto func_exit;  
      chunkBuf = lobUpdateStmt->getBlobChunkBuffer();
      updateLobPos = (-1 *jPos) - 2;
      lobUpdateStmt->updateLob((IDL_char *)lobLocator, chunkBuf, length, updateLobPos, &exception);
   } else
     abort(); 
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

