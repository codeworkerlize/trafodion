// **********************************************************************
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

// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
// **********************************************************************

#include "Context.h"
#include "Globals.h"
#include <signal.h>
#include "HBaseClient_JNI.h"
#include "QRLogger.h"
#include "pthread.h"
#include "ComEncryption.h"
#include "LateBindInfo.h"
#include "ExStats.h"

//
// ===========================================================================
// ===== Class HBaseClient_JNI
// ===========================================================================

JavaMethodInit* HBaseClient_JNI::JavaMethods_ = NULL;
jclass HBaseClient_JNI::javaClass_ = 0;
bool HBaseClient_JNI::javaMethodsInitialized_ = false;
pthread_mutex_t HBaseClient_JNI::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

// Keep in sync with HBC_RetCode enum.
static const char* const hbcErrorEnumStr[] = 
{
  "Preparing parameters for initConnection()."
 ,"Java exception in initConnection()."
 ,"Java exception in getHTableClient()."
 ,"Java exception in releaseHTableClient()."
 ,"Preparing parameters for create()."
 ,"Java exception in create()."
 ,"Preparing parameters for alter()."
 ,"Java exception in alter()."
 ,"Preparing parameters for drop()."
 ,"Java exception in drop()."
 ,"Preparing parameters for exists()."
 ,"Java exception in exists()."
 ,"Preparing parameters for grant()."
 ,"Java exception in grant()."
 ,"Preparing parameters for revoke()."
 ,"Java exception in revoke()."
 ,"Error in Thread Create"
 ,"Error in Thread Req Alloc"
 ,"Error in Thread SIGMAS"
 ,"Error in Attach JVM"
 ,"Java exception in getHBulkLoadClient()."
 ,"Preparing parameters for estimateRowCount()."
 ,"Java exception in estimateRowCount()."
 ,"estimateRowCount() returned false."
 ,"Java exception in releaseHBulkLoadClient()."
 ,"Java exception in getBlockCacheFraction()."
 ,"Preparing parameters for getLatestSnapshot()."
 ,"Java exception in getLatestSnapshot()."
 ,"Preparing parameters for cleanSnpTmpLocation()."
 ,"Java exception in cleanSnpTmpLocation()."
 ,"Preparing parameters for setArcPerms()."
 ,"Java exception in setArcPerms()."
 ,"Preparing parameters for startGet()."
 ,"Java exception in startGet()."
 ,"Preparing parameters for startGets()."
 ,"Java exception in startGets()."
 ,"Preparing parameters for getHbaseTableInfo()."
 ,"Java exception in getHbaseTableInfo()."
 ,"preparing parameters for createCounterTable()."
 ,"java exception in createCounterTable()."
 ,"preparing parameters for incrCounter()."
 ,"java exception in incrCounter()."
 ,"Preparing parameters for getRegionsNodeName()."
 ,"Java exception in getRegionsNodeName()."
 ,"Preparing parameters for insertRow()."
 ,"Java exception in insertRow()."
 ,"Dup RowId in insertRow()."
 ,"DDL for plan has become invalid in insertRow()."
 ,"Preparing parameters for insertRows()."
 ,"Java exception in insertRows()."
 ,"DDL for plan has become invalid in insertRows()."
 ,"Java exception in checkAndUpdateRow()."
 ,"Preparing parameters for checkAndUpdateRow()."
 ,"Row not found in checkAndUpdateRow()."
 ,"Preparing parameters for deleteRow()."
 ,"Java exception in deleteRow()."
 ,"DDL for plan has become invalid in deleteRow()."
 ,"Preparing parameters for deleteRows()."
 ,"Java exception in deleteRows()."
 ,"DDL for plan has become invalid in deleteRows()."
 ,"Preparing parameters for checkAndDeleteRow()."
 ,"Java exception in checkAndDeleteRow()."
 ,"Row not found in checkAndDeleteRow()."
 ,"DDL for plan has become invalid in checkAndDeleteRow()."
 ,"Java exception in addTablesToHdfsCache()."
 ,"Java exception in removeTablesFromHdfsCache()."
 ,"Java exception in showTablesHdfsCache()."
 ,"Pool does not exist."
 ,"Preparing parameters for listAll()."
 ,"Preparing parameters for getKeys()."
 ,"Preparing parameters for listAll()."
 ,"Preparing parameters for getRegionStats()."
 ,"JNI NewStringUTF() in backupObjects()."
 ,"Java exception in backupObjects()."
 ,"JNI NewStringUTF() in deleteSnapshot()."
 ,"Java exception in deleteSnapshot()."
 ,"JNI NewStringUTF() in verifySnapshot()."
 ,"Java exception in verifySnapshot()."
 ,"Preparing parameters for namespaceOperation()"
 ,"Java exception or Error in namespaceOperation()"
 ,"Namespace doesn't exist"
 ,"Preparing parameters for truncate()."
 ,"Java exception in truncate()."
 ,"Lock timeout Exception"
 ,"java excpetion in lockRequired()"
 ,"Preparing parameters in reconnect()."
 ,"Java exception in reconnect()."
 ,"Parparing parameters in getNextValue()."
 ,"Java exception in getNextValue()."
 ,"Parparing parameters in deleteSeqRow()."
 ,"Java exception in deleteSeqRow()."
 ,"Java exception in cancelOperation()."
 ,"Parparing parameters in updateTableDefForBinlog()."
 ,"Java exception in updateTableDefForBinlog()."
 ,"Lock not enough resourcs exception"
};

#define CHECK_EXCEPTION_AND_RETURN_LOCK_ERROR(tabName, reterr)          \
do{                                                                     \
    if (jenv_->ExceptionCheck())                                        \
    {                                                                   \
        getExceptionDetails(__FILE__, __LINE__, __FUNCTION__, tableName_); \
        retValue = (reterr);                                            \
                                                                        \
        if (gEnableRowLevelLock) {                                      \
            HTC_RetCode ret_lock = getLockError();                      \
            if (ret_lock != HTC_OK)                                     \
                retValue = ret_lock;                                    \
        }                                                               \
        goto errorLabel;                                                \
    }                                                                   \
}while(false)

#define COMMON_CHECK_RESULT_AND_RETURN_ERROR(result, tabName, reterr)   \
do{                                                                     \
    if (result)                                                         \
    {                                                                   \
        getExceptionDetails(__FILE__, __LINE__, __FUNCTION__, tabName); \
        retValue = (reterr);                                            \
        goto errorLabel;                                                \
    }                                                                   \
} while(false)

#define CHECK_RESULT_AND_RETURN_ERROR(result, tabName, reterr)          \
    COMMON_CHECK_RESULT_AND_RETURN_ERROR(((result) == NULL), (tabName), (reterr))

#define CHECK_EXCEPTION_AND_RETURN_ERROR(tabName, reterr)               \
    COMMON_CHECK_RESULT_AND_RETURN_ERROR(jenv_->ExceptionCheck(), (tabName), (reterr))

#define CHECK_RESULT_AND_SET_RETURN_ERROR(retobj, srcobj, errid, reterr) \
do {                                                                    \
    if ((retobj) == NULL)                                               \
    {                                                                   \
        char buffer[1024];                                              \
        snprintf(buffer, sizeof(buffer), "%s %s %s:%d",                 \
                 getErrorText((errid)), (srcobj), __FILE__, __LINE__);  \
        GetCliGlobals()->setJniErrorStr(buffer);                        \
        retValue = reterr;                                              \
        goto errorLabel;                                                \
    }                                                                   \
} while(false)

#define CHECK_AND_DELETE_NEWOBJ(objname)                                \
do {                                                                    \
    if ((objname) != NULL)                                              \
    {                                                                   \
        jenv_->DeleteLocalRef(objname);                                 \
        objname = NULL;                                                 \
    }                                                                   \
} while (false)

#define CHECK_AND_DELETE_GLOBAL_NEWOBJ(objname)                         \
do {                                                                    \
    if ((objname) != NULL)                                              \
    {                                                                   \
        jenv_->DeleteGlobalRef(objname);                                \
        objname = NULL;                                                 \
    }                                                                   \
} while (false)

#define CHECK_AND_REPORT_ERROR(result, reterr)                          \
do {                                                                    \
    if ((result))                                                       \
    {                                                                   \
        logError(CAT_SQL_HBASE, __FUNCTION__, getLastError());          \
        retValue = (reterr);                                            \
        goto errorLabel;                                                \
    }                                                                   \
} while (false)

#define CHECK_AND_NEW_GLOBAL_REF(excepted, jvalue, jtype, value)        \
do {                                                                    \
    if (!(excepted)) {                                                  \
        jvalue = (jtype)jenv_->NewGlobalRef(value);                     \
        if (jenv_->ExceptionCheck())                                    \
            excepted = TRUE;                                            \
    }                                                                   \
} while(false)

#ifdef _DEBUG
#define FUNCTION_DEBUG(fmt, ...)                                        \
do{                                                                     \
   QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, fmt, ##__VA_ARGS__);          \
} while(false)

#define ENTRY_FUNCTION()                                                \
    FUNCTION_DEBUG("%s() called.", __FUNCTION__)
#define EXIT_FUNCTION(retValue)                                         \
    FUNCTION_DEBUG("exit %s(%d) %s:%d.",                                \
                       __FUNCTION__, (retValue), __FILE__, __LINE__)
#else
#define FUNCTION_DEBUG(fmt, ...)
#define ENTRY_FUNCTION()
#define EXIT_FUNCTION(retValue)
#endif

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
// private default constructor
HBaseClient_JNI::HBaseClient_JNI(NAHeap *heap, NABoolean bigTable)
                 :  JavaObjectInterface(heap)
                   ,isConnected_(FALSE)
{
  for (int i=0; i<NUM_HBASE_WORKER_THREADS; i++) {
    threadID_[i] = 0;
  }
  bigTable_ = bigTable;
  noConflictCheckForIndex_ = FALSE;
  trigger_operation_ = 0;
  skvBuffer_ = NULL;
  sBufSize_ = NULL;
  srowIDs_ = NULL;
  srowIDsLen_ = NULL;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
char* HBaseClient_JNI::getErrorText(HBC_RetCode errEnum)
{
  if (errEnum < (HBC_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)errEnum);
  else    
    return (char*)hbcErrorEnumStr[errEnum-HBC_FIRST-1];
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBaseClient_JNI* HBaseClient_JNI::getInstance(NABoolean bigtable)
{
   CliGlobals *cliGlobals = GetCliGlobals();
   NAHeap *heap = cliGlobals->getExecutorMemory();
   HBaseClient_JNI *hbaseClient_JNI;
   HBC_RetCode hbcRetCode;
   if (bigtable)    
      hbaseClient_JNI = cliGlobals->getBigtableClient();
   else
      hbaseClient_JNI = cliGlobals->getHBaseClient();
   if (hbaseClient_JNI == NULL) {
      hbaseClient_JNI  = new (heap) HBaseClient_JNI(heap,
                    bigtable);
      if (bigtable)    
         cliGlobals->setBigtableClient(hbaseClient_JNI);
      else
         cliGlobals->setHbaseClient(hbaseClient_JNI);
   }
   return hbaseClient_JNI;
}

void HBaseClient_JNI::deleteInstance()
{
   CliGlobals *cliGlobals = GetCliGlobals();
   HBaseClient_JNI *hbaseClient_JNI = cliGlobals->getHBaseClient();
   if (hbaseClient_JNI != NULL)
   {
      NADELETE(hbaseClient_JNI, HBaseClient_JNI, cliGlobals->getExecutorMemory());
      cliGlobals->setHbaseClient(NULL);
   }
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBaseClient_JNI::~HBaseClient_JNI()
{
  //QRLogger::log(CAT_JNI_TOP, LL_DEBUG, "HBaseClient_JNI destructor called.");
  
  // worker threads need to go away and be joined.
  cleanupOldValueInfo();
  if (threadID_[0])
  {
    // tell the worker threads to go away
    for (int i=0; i<NUM_HBASE_WORKER_THREADS; i++) {
      enqueueShutdownRequest(); 
    }

    // wait for worker threads to exit and join
    for (int i=0; i<NUM_HBASE_WORKER_THREADS; i++) {
      pthread_join(threadID_[i], NULL);
    }

    pthread_mutex_destroy(&mutex_);
    pthread_cond_destroy(&workBell_);
  }
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::init()
{
  static char className[]="org/trafodion/sql/HBaseClient";
  HBC_RetCode rc;
  
  if (isInitialized())
    return HBC_OK;
  
  if (javaMethodsInitialized_)
    return (HBC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
  else
  {
    pthread_mutex_lock(&javaMethodsInitMutex_);
    if (javaMethodsInitialized_)
    {
      pthread_mutex_unlock(&javaMethodsInitMutex_);
      return (HBC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    }
    JavaMethods_ = new JavaMethodInit[JM_LAST];
    
    JavaMethods_[JM_CTOR       ].jm_name      = "<init>";
    JavaMethods_[JM_CTOR       ].jm_signature = "()V";
    JavaMethods_[JM_INIT       ].jm_name      = "init";
    JavaMethods_[JM_INIT       ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;Z)Z";
    JavaMethods_[JM_GET_HTC    ].jm_name      = "getHTableClient";
    JavaMethods_[JM_GET_HTC    ].jm_signature = "(JLjava/lang/String;ZZZ)Lorg/trafodion/sql/HTableClient;";
    JavaMethods_[JM_REL_HTC    ].jm_name      = "releaseHTableClient";
    JavaMethods_[JM_REL_HTC    ].jm_signature = "(Lorg/trafodion/sql/HTableClient;)V";
    JavaMethods_[JM_CREATE     ].jm_name      = "create";
    JavaMethods_[JM_CREATE     ].jm_signature = "(Ljava/lang/String;[Ljava/lang/Object;Z)Z";
    JavaMethods_[JM_CREATEK    ].jm_name      = "createk";
    JavaMethods_[JM_CREATEK    ].jm_signature = "(Ljava/lang/String;[Ljava/lang/Object;[Ljava/lang/Object;JIIZZ)Z";
    JavaMethods_[JM_TRUNCABORT ].jm_name      = "registerTruncateOnAbort";
    JavaMethods_[JM_TRUNCABORT ].jm_signature = "(Ljava/lang/String;J)Z";
    JavaMethods_[JM_ALTER      ].jm_name      = "alter";
    JavaMethods_[JM_ALTER      ].jm_signature = "(Ljava/lang/String;[Ljava/lang/Object;J)Z";
    JavaMethods_[JM_DROP       ].jm_name      = "drop";
    JavaMethods_[JM_DROP       ].jm_signature = "(Ljava/lang/String;J)Z";
    JavaMethods_[JM_DROP_ALL       ].jm_name      = "dropAll";
    JavaMethods_[JM_DROP_ALL       ].jm_signature = "(Ljava/lang/String;J)Z";
    JavaMethods_[JM_LIST_ALL       ].jm_name      = "listAll";
    JavaMethods_[JM_LIST_ALL       ].jm_signature = "(Ljava/lang/String;)[Ljava/lang/String;";
    JavaMethods_[JM_GET_REGION_STATS       ].jm_name      = "getRegionStats";
    JavaMethods_[JM_GET_REGION_STATS       ].jm_signature = "(Ljava/lang/String;)[[B";
    JavaMethods_[JM_COPY       ].jm_name      = "copy";
    JavaMethods_[JM_COPY       ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;Z)Z";
    JavaMethods_[JM_EXISTS     ].jm_name      = "exists";
    JavaMethods_[JM_EXISTS     ].jm_signature = "(Ljava/lang/String;J)Z";
    JavaMethods_[JM_GRANT      ].jm_name      = "grant";
    JavaMethods_[JM_GRANT      ].jm_signature = "([B[B[Ljava/lang/Object;)Z";
    JavaMethods_[JM_REVOKE     ].jm_name      = "revoke";
    JavaMethods_[JM_REVOKE     ].jm_signature = "([B[B[Ljava/lang/Object;)Z";
    JavaMethods_[JM_GET_HBLC   ].jm_name      = "getHBulkLoadClient";
    JavaMethods_[JM_GET_HBLC   ].jm_signature = "()Lorg/trafodion/sql/HBulkLoadClient;";
    JavaMethods_[JM_GET_BRC    ].jm_name      = "getBackupRestoreClient";
    JavaMethods_[JM_GET_BRC    ].jm_signature = "()Lorg/apache/hadoop/hbase/pit/BackupRestoreClient;";
    JavaMethods_[JM_EST_RC     ].jm_name      = "estimateRowCount";
    JavaMethods_[JM_EST_RC     ].jm_signature = "(Ljava/lang/String;III[J)Z";
    JavaMethods_[JM_EST_RC_COPROC     ].jm_name      = "estimateRowCountViaCoprocessor";
    JavaMethods_[JM_EST_RC_COPROC     ].jm_signature = "(Ljava/lang/String;III[J)Z";
    JavaMethods_[JM_REL_HBLC   ].jm_name      = "releaseHBulkLoadClient";
    JavaMethods_[JM_REL_HBLC   ].jm_signature = "(Lorg/trafodion/sql/HBulkLoadClient;)V";
    JavaMethods_[JM_REL_BRC    ].jm_name      = "releaseBackupRestoreClient";
    JavaMethods_[JM_REL_BRC    ].jm_signature = "(Lorg/apache/hadoop/hbase/pit/BackupRestoreClient;)V";
    JavaMethods_[JM_GET_CAC_FRC].jm_name      = "getBlockCacheFraction";
    JavaMethods_[JM_GET_CAC_FRC].jm_signature = "()F";
    JavaMethods_[JM_GET_LATEST_SNP].jm_name      = "getLatestSnapshot";
    JavaMethods_[JM_GET_LATEST_SNP].jm_signature = "(Ljava/lang/String;)Ljava/lang/String;";
    JavaMethods_[JM_CLEAN_SNP_TMP_LOC].jm_name      = "cleanSnpScanTmpLocation";
    JavaMethods_[JM_CLEAN_SNP_TMP_LOC].jm_signature = "(Ljava/lang/String;)Z";
    JavaMethods_[JM_SET_ARC_PERMS].jm_name      = "setArchivePermissions";
    JavaMethods_[JM_SET_ARC_PERMS].jm_signature = "(Ljava/lang/String;)Z";
    JavaMethods_[JM_START_GET].jm_name      = "startGet";
    JavaMethods_[JM_START_GET].jm_signature = "(JLjava/lang/String;ZZIZJJJI[B[Ljava/lang/Object;JLjava/lang/String;IZZLjava/lang/String;)I";
    JavaMethods_[JM_START_GETS].jm_name      = "startGet";
    JavaMethods_[JM_START_GETS].jm_signature = "(JLjava/lang/String;ZZIZZJJJI[Ljava/lang/Object;[Ljava/lang/Object;JLjava/lang/String;ILjava/lang/String;)I";
    JavaMethods_[JM_START_DIRECT_GETS].jm_name      = "startGet";
    JavaMethods_[JM_START_DIRECT_GETS].jm_signature = "(JLjava/lang/String;ZZIZZJJJISLjava/lang/Object;[Ljava/lang/Object;Ljava/lang/String;ILjava/lang/String;)I";
    JavaMethods_[JM_GET_HBTI].jm_name      = "getHbaseTableInfo";
    JavaMethods_[JM_GET_HBTI].jm_signature = "(Ljava/lang/String;[I)Z";
    JavaMethods_[JM_CREATE_COUNTER_TABLE ].jm_name      = "createCounterTable";
    JavaMethods_[JM_CREATE_COUNTER_TABLE ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)Z";
    JavaMethods_[JM_INCR_COUNTER         ].jm_name      = "incrCounter";
    JavaMethods_[JM_INCR_COUNTER         ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;J)J";
    JavaMethods_[JM_GET_REGN_NODES].jm_name      = "getRegionsNodeName";
    JavaMethods_[JM_GET_REGN_NODES].jm_signature = "(Ljava/lang/String;[Ljava/lang/String;)Z";
    JavaMethods_[JM_HBC_DIRECT_INSERT_ROW].jm_name      = "insertRow";
    JavaMethods_[JM_HBC_DIRECT_INSERT_ROW].jm_signature = "(JLjava/lang/String;JJJ[BLjava/lang/Object;JZSIILjava/lang/String;)Z";
    JavaMethods_[JM_HBC_DIRECT_INSERT_ROWS].jm_name      = "insertRows";
    JavaMethods_[JM_HBC_DIRECT_INSERT_ROWS].jm_signature = "(JLjava/lang/String;JJJSLjava/lang/Object;Ljava/lang/Object;JILjava/lang/String;)Z";
    JavaMethods_[JM_HBC_DIRECT_UPDATE_TAGS].jm_name      = "updateVisibility";
    JavaMethods_[JM_HBC_DIRECT_UPDATE_TAGS].jm_signature = "(JLjava/lang/String;ZJ[BLjava/lang/Object;Ljava/lang/String;)Z";
    JavaMethods_[JM_HBC_DIRECT_CHECKANDUPDATE_ROW].jm_name      = "checkAndUpdateRow";
    JavaMethods_[JM_HBC_DIRECT_CHECKANDUPDATE_ROW].jm_signature = "(JLjava/lang/String;JJJ[BLjava/lang/Object;[B[BJIILjava/lang/String;)Z";
    JavaMethods_[JM_HBC_DELETE_ROW ].jm_name      = "deleteRow";
    JavaMethods_[JM_HBC_DELETE_ROW ].jm_signature = "(JLjava/lang/String;JJJ[B[Ljava/lang/Object;JLjava/lang/String;ILjava/lang/String;)Z";
    JavaMethods_[JM_HBC_DIRECT_DELETE_ROWS ].jm_name      = "deleteRows";
    JavaMethods_[JM_HBC_DIRECT_DELETE_ROWS ].jm_signature = "(JLjava/lang/String;JJJSLjava/lang/Object;[Ljava/lang/Object;JLjava/lang/String;ILjava/lang/String;)Z";
    JavaMethods_[JM_HBC_CHECKANDDELETE_ROW ].jm_name      = "checkAndDeleteRow";
    JavaMethods_[JM_HBC_CHECKANDDELETE_ROW ].jm_signature = "(JLjava/lang/String;JJJ[B[Ljava/lang/Object;[B[BJLjava/lang/String;ILjava/lang/String;)Z";
    JavaMethods_[JM_SHOW_TABLES_HDFS_CACHE].jm_name = "showTablesHDFSCache";
    JavaMethods_[JM_SHOW_TABLES_HDFS_CACHE].jm_signature = "([Ljava/lang/Object;)[[B";
    JavaMethods_[JM_ADD_TABLES_TO_HDFS_CACHE].jm_name = "addTablesToHDFSCache";
    JavaMethods_[JM_ADD_TABLES_TO_HDFS_CACHE].jm_signature = "([Ljava/lang/Object;Ljava/lang/String;)I";
    JavaMethods_[JM_REMOVE_TABLES_FROM_HDFS_CACHE].jm_name = "removeTablesFromHDFSCache";
    JavaMethods_[JM_REMOVE_TABLES_FROM_HDFS_CACHE].jm_signature = "([Ljava/lang/Object;Ljava/lang/String;)I";
    JavaMethods_[JM_HBC_GETSTARTKEYS ].jm_name      = "getStartKeys";
    JavaMethods_[JM_HBC_GETSTARTKEYS ].jm_signature = "(Ljava/lang/String;Z)[[B";
    JavaMethods_[JM_HBC_GETENDKEYS ].jm_name      = "getEndKeys";
    JavaMethods_[JM_HBC_GETENDKEYS ].jm_signature = "(Ljava/lang/String;Z)[[B";
    JavaMethods_[JM_HBC_CREATE_SNAPSHOT].jm_name      = "createSnapshot";
    JavaMethods_[JM_HBC_CREATE_SNAPSHOT].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)Z";
    JavaMethods_[JM_HBC_RESTORE_SNAPSHOT].jm_name      = "restoreSnapshot";
    JavaMethods_[JM_HBC_RESTORE_SNAPSHOT].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)Z";
    JavaMethods_[JM_HBC_DELETE_SNAPSHOT].jm_name      = "deleteSnapshot";
    JavaMethods_[JM_HBC_DELETE_SNAPSHOT].jm_signature = "(Ljava/lang/String;)Z";
    JavaMethods_[JM_HBC_VERIFY_SNAPSHOT].jm_name      = "verifySnapshot";
    JavaMethods_[JM_HBC_VERIFY_SNAPSHOT].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)Z";
    JavaMethods_[JM_SAVEPOINT_COMMIT_OR_ROLLBACK].jm_name      = "savepointCommitOrRollback";
    JavaMethods_[JM_SAVEPOINT_COMMIT_OR_ROLLBACK].jm_signature = "(JJJZ)Z";
    JavaMethods_[JM_NAMESPACE_OPERATION].jm_name      = "namespaceOperation";
    JavaMethods_[JM_NAMESPACE_OPERATION].jm_signature = "(ILjava/lang/String;[Ljava/lang/Object;[Ljava/lang/Object;)[Ljava/lang/String;";
    JavaMethods_[JM_TRUNCATE   ].jm_name      = "truncate";
    JavaMethods_[JM_TRUNCATE   ].jm_signature = "(Ljava/lang/String;ZJ)Z";
    JavaMethods_[JM_LOCKREQUIRED].jm_name      = "lockRequired";
    JavaMethods_[JM_LOCKREQUIRED].jm_signature = "(Ljava/lang/String;JJJIZLjava/lang/String;)Z";
    JavaMethods_[JM_RECONNECT].jm_name      = "reconnect";
    JavaMethods_[JM_RECONNECT].jm_signature = "()Z";
    JavaMethods_[JM_GET_NEXT_VALUE].jm_name = "getNextValue";
    JavaMethods_[JM_GET_NEXT_VALUE].jm_signature = "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;JZ)J";
    JavaMethods_[JM_DELETE_SEQ_ROW].jm_name = "deleteSeqRow";
    JavaMethods_[JM_DELETE_SEQ_ROW].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)J";
    JavaMethods_[JM_CANCLE_OPERATION].jm_name = "cancelOperation";
    JavaMethods_[JM_CANCLE_OPERATION].jm_signature = "(J)Z";
    JavaMethods_[JM_UPDATE_TBL_DEF_BINLOG].jm_name = "updateTableDefForBinlog";
    JavaMethods_[JM_UPDATE_TBL_DEF_BINLOG].jm_signature = "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;J)J";
    JavaMethods_[JM_GET_TBL_DEF_BINLOG].jm_name = "getTableDefForBinlog";
    JavaMethods_[JM_GET_TBL_DEF_BINLOG].jm_signature = "(Ljava/lang/String;)[Ljava/lang/String;";
    JavaMethods_[JM_PUT_SQL_TO_HBASE].jm_name = "putData";
    JavaMethods_[JM_PUT_SQL_TO_HBASE].jm_signature = "(JLjava/lang/String;ILjava/lang/String;[B)Z";

    rc = (HBC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    if (rc == HBC_OK)
       javaMethodsInitialized_ = TRUE;
    pthread_mutex_unlock(&javaMethodsInitMutex_);
  }
  return rc;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::initConnection(const char* connectParam1, const char* connectParam2, NABoolean bigtable)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::initConnection(%s, %s) called.", connectParam1, connectParam2);


  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_connectParam1 = jenv_->NewStringUTF(connectParam1);
  if (js_connectParam1 == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INIT_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INIT_PARAM;
  }
  jstring js_connectParam2 = jenv_->NewStringUTF(connectParam2);
  if (js_connectParam2 == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INIT_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INIT_PARAM;
  }
  tsRecentJMFromJNI = JavaMethods_[JM_INIT].jm_full_name;
  jboolean j_bigtable = bigtable;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_INIT].methodID, js_connectParam1, js_connectParam2, j_bigtable);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::initConnection()");
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INIT_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::initConnection()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INIT_EXCEPTION;
  }

  isConnected_ = TRUE;
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::reconnectHBase()
{
  QRLogger::log(CAT_SQL_HBASE, LL_INFO, "HBaseClient_JNI::reconnectHbase() called.");

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_RECONNECT_PARAM;

  tsRecentJMFromJNI = JavaMethods_[JM_RECONNECT].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_RECONNECT].methodID);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::reconnectHbase()");
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_RECONNECT_EXCEPTION;
  }

  isConnected_ = jresult;
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::getLockError()
{
  char *errMsg = (char*)getSqlJniErrorStr();
  if (errMsg == "" || errMsg == NULL)
  {
      return HBC_OK;
  }

  NAString error_msg(errMsg);
  if (error_msg.contains("LockTimeOutException failed to get lock for transaction rollbacked"))
    {
      return HBC_ERROR_LOCK_ROLLBACK_EXCEPTION;
    }
  else if (error_msg.contains("LockTimeOutException"))
    {
      return HBC_ERROR_LOCK_TIME_OUT_EXCEPTION;
    }
  else if (error_msg.contains("DeadLockException"))
    {
      return HBC_ERROR_DEAD_LOCK_EXCEPTION;
    }
  else if (error_msg.contains("RPCTimeOutException"))
    {
      return HBC_ERROR_RPC_TIME_OUT_EXCEPTION;
    }
  else if (error_msg.contains("LockCancelOperationException"))
    {
      return HBC_ERROR_CANCEL_OPERATION;
    }
  else if (error_msg.contains("FailedToLockException") && error_msg.contains("region move"))
    {
      return HBC_ERROR_LOCK_REGION_MOVE;
    }
  else if (error_msg.contains("FailedToLockException") && error_msg.contains("region split"))
    {
      return HBC_ERROR_LOCK_REGION_SPLIT;
    }
  else if (error_msg.contains("LockNotEnoughResourcsException"))
    {
      return HBC_ERROR_LOCK_NOT_ENOUGH_RESOURCE_EXCEPTION;
    }
  else
    {
      return HBC_OK;
    }
  
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HTableClient_JNI* HBaseClient_JNI::getHTableClient(NAHeap *heap, 
						   const char* tableName,
						   bool useTRex,
						   NABoolean replSync,
                                                   NABoolean incrBackupEnabled,
						   ExHbaseAccessStats *hbs)
{
  FUNCTION_DEBUG("%s(%s) called.", __FUNCTION__, tableName);
  if (javaObj_ == NULL || (!isInitialized()))
  {
    EXIT_FUNCTION(HBC_ERROR_GET_HTC_EXCEPTION);
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GET_HTC_EXCEPTION));
    return NULL;
  }
  if (initJNIEnv() != JOI_OK) 
  {
     EXIT_FUNCTION(HBC_ERROR_GET_HTC_EXCEPTION);
     return NULL;
  }

  jobject j_htc = NULL;
  HTableClient_JNI *htc = NULL;
  HTableClient_JNI *retValue = NULL;
  jstring js_tblName = jenv_->NewStringUTF(tableName);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_tblName, tableName, (HBC_RetCode)JOI_ERROR_NEWOBJ, NULL);

  htc = new (heap) HTableClient_JNI(heap, (jobject)-1);
  if (htc->init() != HTC_OK)
  {
      goto errorLabel;
  }
  // assign value after successful initialization
  retValue = htc;
  htc->setTableName(tableName);
  htc->setHbaseStats(hbs);

  tsRecentJMFromJNI = JavaMethods_[JM_GET_HTC].jm_full_name;
  j_htc = jenv_->CallObjectMethod(javaObj_, 
                                  JavaMethods_[JM_GET_HTC].methodID, 
                                  (jlong)htc, 
                                  js_tblName, 
                                  (jboolean)useTRex, 
                                  (jboolean) replSync,
                                  (jboolean) incrBackupEnabled);

  CHECK_EXCEPTION_AND_RETURN_ERROR(tableName, NULL);
  CHECK_RESULT_AND_RETURN_ERROR(j_htc, tableName, NULL);

  htc->setJavaObject(j_htc);

  if (htc->init() != HTC_OK) 
  {
      retValue = NULL;
  }

errorLabel:
  if (retValue == NULL) 
  {
      if (htc != NULL)
      {
          releaseHTableClient(htc);
      } 
      NADELETE(htc, HTableClient_JNI, heap);
  }
  CHECK_AND_DELETE_NEWOBJ(js_tblName);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue == NULL ? HBC_OK : HBC_ERROR_GET_HTC_EXCEPTION);
  return retValue;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::releaseHTableClient(HTableClient_JNI* htc)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::releaseHTableClient() called.");

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jobject j_htc = htc->getJavaObject();
  if (j_htc != (jobject)-1) {
      tsRecentJMFromJNI = JavaMethods_[JM_REL_HTC].jm_full_name;
      jenv_->CallVoidMethod(javaObj_, JavaMethods_[JM_REL_HTC].methodID, j_htc);
      if (jenv_->ExceptionCheck()) {
         getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::releaseHTableClient()", htc->getTableName());
         jenv_->PopLocalFrame(NULL);
         return HBC_ERROR_REL_HTC_EXCEPTION;
      }
  }
  NADELETE(htc, HTableClient_JNI, htc->getHeap()); 
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
HBulkLoadClient_JNI* HBaseClient_JNI::getHBulkLoadClient(NAHeap *heap)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::getHBulkLoadClient() called.");
  if (javaObj_ == NULL || (!isInitialized()))
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GET_HBLC_EXCEPTION));
    return NULL;
  }
  if (initJNIEnv() != JOI_OK)
     return NULL;

  tsRecentJMFromJNI = JavaMethods_[JM_GET_HBLC].jm_full_name;
  jobject j_hblc = jenv_->CallObjectMethod(javaObj_, JavaMethods_[JM_GET_HBLC].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::getHBulkLoadClient()");
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
  HBulkLoadClient_JNI *hblc = new (heap) HBulkLoadClient_JNI(heap, j_hblc);
  if ( hblc->init()!= HBLC_OK)
  {
     NADELETE(hblc, HBulkLoadClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return NULL; 
  }
  jenv_->PopLocalFrame(NULL);
  return hblc;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::releaseHBulkLoadClient(HBulkLoadClient_JNI* hblc)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::releaseHBulkLoadClient() called.");

  jobject j_hblc = hblc->getJavaObject();

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  tsRecentJMFromJNI = JavaMethods_[JM_REL_HBLC].jm_full_name;
  jenv_->CallVoidMethod(javaObj_, JavaMethods_[JM_REL_HBLC].methodID, j_hblc);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::releaseHBulkLoadClient()");
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_REL_HBLC_EXCEPTION;
  }
  NADELETE(hblc, HBulkLoadClient_JNI, hblc->getHeap());
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
BackupRestoreClient_JNI* HBaseClient_JNI::getBackupRestoreClient(NAHeap *heap)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::getBackupRestoreClient() called.");
  if (javaObj_ == NULL || (!isInitialized()))
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GET_BRC_EXCEPTION));
    return NULL;
  }

  if (initJNIEnv() != JOI_OK)
     return NULL;

  tsRecentJMFromJNI = JavaMethods_[JM_GET_BRC].jm_full_name;
  jobject j_brc = jenv_->CallObjectMethod(javaObj_, JavaMethods_[JM_GET_BRC].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::getBackupRestoreClient()");
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
  if (j_brc == NULL)
  {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::getBackupRestoreClient()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
  BackupRestoreClient_JNI *brc = new (heap) BackupRestoreClient_JNI(heap, j_brc);
  jenv_->DeleteLocalRef(j_brc);
  if ( brc->init()!= BRC_OK)
  {
     NADELETE(brc, BackupRestoreClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return NULL; 
  }
  jenv_->PopLocalFrame(NULL);
  return brc;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::releaseBackupRestoreClient(BackupRestoreClient_JNI* brc)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::releaseBackupRestoreClient() called.");

  jobject j_brc = brc->getJavaObject();

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;
  tsRecentJMFromJNI = JavaMethods_[JM_REL_BRC].jm_full_name;
  jenv_->CallVoidMethod(javaObj_, JavaMethods_[JM_REL_BRC].methodID, j_brc);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::releaseBackupRestoreClient()");
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_REL_BRC_EXCEPTION;
  }
  NADELETE(brc, BackupRestoreClient_JNI, brc->getHeap());
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::create(const char* fileName, HBASE_NAMELIST& colFamilies, NABoolean isMVCC)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::create(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CREATE_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CREATE_PARAM;
  }
  jobjectArray j_fams = convertToStringObjectArray(colFamilies);
  if (j_fams == NULL)
  {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CREATE_PARAM));
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_CREATE_PARAM;
  }
    
  jboolean j_isMVCC = isMVCC;
  tsRecentJMFromJNI = JavaMethods_[JM_CREATE].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
        JavaMethods_[JM_CREATE].methodID, js_fileName, j_fams, j_isMVCC);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::create()");
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CREATE_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::create()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CREATE_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}


//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::create(const char* fileName, 
                                    NAText* createOptionsArray,
                                    int numSplits, int keyLength,
                                    const char ** splitValues,
                                    Int64 transID,
                                    NABoolean isMVCC,
                                    NABoolean incrBackupEnabled)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::create(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CREATE_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CREATE_PARAM;
  }
  jobjectArray j_opts = convertToStringObjectArray(createOptionsArray, 
                   HBASE_MAX_OPTIONS);
  if (j_opts == NULL)
  {
     getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::create()", fileName);
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_CREATE_PARAM;
  }

  jobjectArray j_keys = NULL;    
  if (numSplits > 0)
  {
     j_keys = convertToByteArrayObjectArray(splitValues, numSplits, keyLength);
     if (j_keys == NULL)
     {
        getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::create()", fileName);
        jenv_->PopLocalFrame(NULL);
        return HBC_ERROR_CREATE_PARAM;
     }
  }
  jlong j_tid = transID;
  jint j_numSplits = numSplits;
  jint j_keyLength = keyLength;

  jboolean j_isMVCC = isMVCC;
  tsRecentJMFromJNI = JavaMethods_[JM_CREATEK].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod
    (javaObj_, 
     JavaMethods_[JM_CREATEK].methodID, js_fileName, j_opts, j_keys, 
     j_tid, j_numSplits, j_keyLength, j_isMVCC, 
     (jboolean)incrBackupEnabled);
  
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::create()", fileName);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CREATE_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::create()", getLastError());
    GetCliGlobals()->setJniErrorStr(getLastError());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CREATE_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::alter(const char* fileName,
                                   NAText* createOptionsArray,
                                   Int64 transID)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::alter(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CREATE_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_ALTER_PARAM;
  }
  jobjectArray j_opts = convertToStringObjectArray(createOptionsArray, 
                   HBASE_MAX_OPTIONS);
  if (jenv_->ExceptionCheck())
  {
     getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::alter()", fileName);
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_ALTER_PARAM;
  }

  jlong j_tid = transID;

  tsRecentJMFromJNI = JavaMethods_[JM_ALTER].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
          JavaMethods_[JM_ALTER].methodID, js_fileName, j_opts, j_tid);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::alter()", fileName);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_ALTER_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, __FILE__, __LINE__, "HBaseClient_JNI::alter()", fileName, getLastError());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_ALTER_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);

  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
HBaseClientRequest::HBaseClientRequest(NAHeap *heap, HBaseClientReqType reqType)
                    :  heap_(heap)
                      ,reqType_(reqType)
                      ,fileName_(NULL)
{
}

HBaseClientRequest::~HBaseClientRequest()
{
  if (fileName_) {
    NADELETEBASIC(fileName_, heap_);
  }
}

void HBaseClientRequest::setFileName(const char *fileName)
{
  int len = strlen(fileName);
  fileName_ = new (heap_) char[len + 1];
  strcpy(fileName_, fileName);
}

HBC_RetCode HBaseClient_JNI::enqueueRequest(HBaseClientRequest *request)
{
  pthread_mutex_lock( &mutex_ );
  reqQueue_.push_back(request);
  pthread_cond_signal(&workBell_);
  pthread_mutex_unlock( &mutex_ );

  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::enqueueDropRequest(const char* fileName)
{
  HBaseClientRequest *request = new (heap_) HBaseClientRequest(heap_, HBC_Req_Drop);

  if (!request) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_THREAD_REQ_ALLOC));
    return HBC_ERROR_THREAD_REQ_ALLOC; 
  }

  request->setFileName(fileName);

  enqueueRequest(request);

  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::enqueueShutdownRequest()
{
  HBaseClientRequest *request = new (heap_) HBaseClientRequest(heap_, HBC_Req_Shutdown);

  if (!request) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_THREAD_REQ_ALLOC));
    return HBC_ERROR_THREAD_REQ_ALLOC;
  }

  enqueueRequest(request);

  return HBC_OK;
}

HBaseClientRequest* HBaseClient_JNI::getHBaseRequest() 
{
  HBaseClientRequest *request;
  reqList_t::iterator it;

  pthread_mutex_lock( &mutex_ );
  it = reqQueue_.begin();

  request = NULL;

  while (request == NULL)
  {
    if (it != reqQueue_.end())
    {
      request = *it;
      it = reqQueue_.erase(it);
    } else {
      pthread_cond_wait(&workBell_, &mutex_);
      it = reqQueue_.begin();
    }
  }

  pthread_mutex_unlock( &mutex_ );

  return request;
}

HBC_RetCode HBaseClient_JNI::performRequest(HBaseClientRequest *request, JNIEnv* jenv)
{
  switch (request->reqType_)
  {
    case HBC_Req_Drop :
	  drop(request->fileName_, jenv, 0);
	  break;
	default :
	  break;
  }

  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::doWorkInThread() 
{
  int rc;

  HBaseClientRequest *request;

  // mask all signals
  sigset_t mask;
  sigfillset(&mask);
  rc = pthread_sigmask(SIG_BLOCK, &mask, NULL);
  if (rc)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_THREAD_SIGMASK));
    return HBC_ERROR_THREAD_SIGMASK;
  }

  JNIEnv* jenv; // thread local
  jint result = jvm_->GetEnv((void**) &jenv, JNI_VERSION_1_6);
  switch (result)
  {
    case JNI_OK:
	  break;

	case JNI_EDETACHED:
	  result = jvm_->AttachCurrentThread((void**) &jenv, NULL);
          if (result != JNI_OK)
          {
             GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_ATTACH_JVM));
	     return HBC_ERROR_ATTACH_JVM;
          }
      break;

	default: 
	  break;
  }

  // enter processing zone
  for (;;)
  {
    request = getHBaseRequest();

	if (request->isShutDown()) {
	  // wake up another thread as this wakeup could have consumed
	  // multiple workBell_ rings.
	  pthread_cond_signal(&workBell_);
	  break;
	} else {
      performRequest(request, jenv);
	  NADELETE(request, HBaseClientRequest, request->getHeap());
	}
  }

  jvm_->DetachCurrentThread();

  pthread_exit(0);

  return HBC_OK;
}

static void *workerThreadMain_JNI(void *arg)
{
  // parameter passed to the thread is an instance of the HBaseClient_JNI object
  HBaseClient_JNI *client = (HBaseClient_JNI *)arg;

  client->doWorkInThread();

  return NULL;
}

HBC_RetCode HBaseClient_JNI::startWorkerThreads()
{
  pthread_mutexattr_t mutexAttr;
  pthread_mutexattr_init( &mutexAttr );
  pthread_mutex_init( &mutex_, &mutexAttr );
  pthread_cond_init( &workBell_, NULL );

  int rc;
  for (int i=0; i<NUM_HBASE_WORKER_THREADS; i++) {
    rc = pthread_create(&threadID_[i], NULL, workerThreadMain_JNI, this);
    if (rc != 0)
    {
       GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_THREAD_CREATE));
       return HBC_ERROR_THREAD_CREATE;
    }
  }

  return HBC_OK;
}


//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::drop(const char* fileName, bool async, long transID)
{
  if (async) {
    if (!threadID_[0]) {
	  startWorkerThreads();
	}
    enqueueDropRequest(fileName);
  } else {
    return drop(fileName, jenv_, transID); // not in worker thread
  }

  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::registerTruncateOnAbort(const char* fileName, Int64 transID)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::drop(%s) called.", fileName);

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DROP_PARAM;
  }

  jlong j_tid = transID;

  tsRecentJMFromJNI = JavaMethods_[JM_TRUNCABORT].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_TRUNCABORT].methodID, js_fileName, j_tid);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::drop()", fileName);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DROP_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::truncate(const char* fileName, NABoolean preserveSplits, Int64 transID)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::truncate(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_TRUNCATE_PARAM;
  }
  jboolean j_preserveSplits = preserveSplits;
  jlong j_tid = transID;  
  // boolean drop(java.lang.String);
  tsRecentJMFromJNI = JavaMethods_[JM_TRUNCATE].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_TRUNCATE].methodID, js_fileName, j_preserveSplits, j_tid);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::truncate()", fileName);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_TRUNCATE_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::drop(const char* fileName, JNIEnv* jenv, Int64 transID)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::drop(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;
  jstring js_fileName = jenv->NewStringUTF(fileName);
  if (js_fileName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DROP_PARAM;
  }

  jlong j_tid = transID;  
  // boolean drop(java.lang.String);
  tsRecentJMFromJNI = JavaMethods_[JM_DROP].jm_full_name;
  jboolean jresult = jenv->CallBooleanMethod(javaObj_, JavaMethods_[JM_DROP].methodID, js_fileName, j_tid);

  if (jenv->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::drop()", fileName);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DROP_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::dropAll(const char* pattern, bool async, Int64 transID)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::dropAll(%s) called.", pattern);

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  if (async) {
    // not supported yet.
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DROP_PARAM;
  }

  jstring js_pattern = jenv_->NewStringUTF(pattern);
  if (js_pattern == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DROP_PARAM;
  }

  jlong j_tid = transID;  

  // boolean drop(java.lang.String);
  tsRecentJMFromJNI = JavaMethods_[JM_DROP_ALL].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_DROP_ALL].methodID, js_pattern, j_tid);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::dropAll()");
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DROP_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr>* HBaseClient_JNI::listAll(NAHeap *heap, const char* pattern)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::listAll(%s) called.", pattern);

  if (initJNIEnv() != JOI_OK)
     return NULL;

  jstring js_pattern = jenv_->NewStringUTF(pattern);
  if (js_pattern == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_LISTALL));
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_LIST_ALL].jm_full_name;
  jarray j_hbaseTables = 
    (jarray)jenv_->CallObjectMethod(javaObj_, JavaMethods_[JM_LIST_ALL].methodID, js_pattern);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::listAll()");
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  if (j_hbaseTables  == NULL) {
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  NAArray<HbaseStr> *hbaseTables;
  jint retcode = convertStringObjectArrayToNAArray(heap, j_hbaseTables, &hbaseTables);
  jenv_->PopLocalFrame(NULL);
  if (retcode == 0)
     return NULL;
  else
     return hbaseTables;
}

HBC_RetCode HBaseClient_JNI::namespaceOperation
(short oper, const char *nameSpace,
 Lng32 numKeyValEntries,
 NAText * keyArray, NAText * valArray,
 NAHeap *heap,
 NAArray<HbaseStr>* *retNames)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::namespaceOperation() called.");

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  tsRecentJMFromJNI = JavaMethods_[JM_NAMESPACE_OPERATION].jm_full_name;

  jint j_oper = oper;
  jstring js_nameSpace = NULL;
  if (nameSpace != NULL)
    {
      js_nameSpace = jenv_->NewStringUTF(nameSpace);
      if (js_nameSpace == NULL) 
        {
          GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_NAMESPACE_PARAM));
          jenv_->PopLocalFrame(NULL);
          return HBC_ERROR_NAMESPACE_PARAM;
        }
    }

  jobjectArray j_keys = NULL;
  if (keyArray != NULL)
    {
      j_keys = convertToStringObjectArray(keyArray, 
                                          numKeyValEntries);

      if (j_keys == NULL)
        {
          getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::namespaceOperation()");
          jenv_->PopLocalFrame(NULL);
          return HBC_ERROR_NAMESPACE_PARAM;
        }
    }

  jobjectArray j_vals = NULL;
  if (valArray != NULL)
    {
      j_vals = convertToStringObjectArray(valArray, 
                                          numKeyValEntries);

      if (j_vals == NULL)
        {
          getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::namespaceOperation()");
          jenv_->PopLocalFrame(NULL);
          return HBC_ERROR_NAMESPACE_PARAM;
        }
    }

  jobjectArray j_namespaceObjs = (jobjectArray)
    jenv_->CallObjectMethod(javaObj_, 
                         JavaMethods_[JM_NAMESPACE_OPERATION].methodID,
                         j_oper,
                         js_nameSpace,
                         j_keys, j_vals);
  
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::namespaceOperation()");
    jenv_->PopLocalFrame(NULL);

    if ((oper == COM_CHECK_NAMESPACE_EXISTS) ||
        (oper == COM_GET_NAMESPACE_OBJECTS))
      return HBC_ERROR_NAMESPACE_NOT_EXIST;
    else
      return HBC_ERROR_NAMESPACE_OPER_ERROR;
  }

  if ((j_namespaceObjs != NULL) && (retNames != NULL)) {
    jint retcode = 
      convertStringObjectArrayToNAArray(heap, j_namespaceObjs, retNames);
    if(retcode == 0) {
      jenv_->PopLocalFrame(NULL);
      return HBC_ERROR_NAMESPACE_OPER_ERROR;
    }
  }

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::savepointCommitOrRollback
(Int64 transId, Int64 savepointId, Int64 tgtSavepointId, bool isCommit)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::savepointCommitOrRollback() called.");

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jlong j_tid = transId;
  jlong j_sid = savepointId;  
  jlong j_tgt_sid = tgtSavepointId;
  jboolean j_isCommit = isCommit;

  tsRecentJMFromJNI = JavaMethods_[JM_SAVEPOINT_COMMIT_OR_ROLLBACK].jm_full_name;
  jboolean jresult = 
    jenv_->CallBooleanMethod(
         javaObj_, 
         JavaMethods_[JM_SAVEPOINT_COMMIT_OR_ROLLBACK].methodID,
         j_tid,
         j_sid,
         j_tgt_sid,
         j_isCommit);
  
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::savepointCommitOrRollback()");
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_SAVEPOINT_COMMIT_OR_ROLLBACK;
  }

  if (jresult == false) {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::savepointCommitOrRollback()", 
             getLastError());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_SAVEPOINT_COMMIT_OR_ROLLBACK;
  }

  jenv_->PopLocalFrame(NULL);

  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr>* HBaseClient_JNI::getRegionStats(NAHeap *heap, const char* tblName)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::getRegionStats(%s) called.", tblName);

  if (initJNIEnv() != JOI_OK)
     return NULL;

  jstring js_tblName = NULL;
  if (tblName) {
    js_tblName = jenv_->NewStringUTF(tblName);
    if (js_tblName == NULL) 
      {
        GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_REGION_STATS));
        jenv_->PopLocalFrame(NULL);
        return NULL;
      }
  }

  tsRecentJMFromJNI = JavaMethods_[JM_GET_REGION_STATS].jm_full_name;
  jarray j_regionInfo = 
    (jarray)jenv_->CallObjectMethod(javaObj_, JavaMethods_[JM_GET_REGION_STATS].methodID, js_tblName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::getRegionStats()", tblName);
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  if (j_regionInfo == NULL) {
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  NAArray<HbaseStr> *regionInfo;
  jint retcode = convertByteArrayObjectArrayToNAArray(heap, j_regionInfo, &regionInfo);

  jenv_->PopLocalFrame(NULL);
  if (retcode == 0)
     return NULL;
  else
     return regionInfo;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::copy(const char* srcTblName, 
                                  const char* tgtTblName,
                                  NABoolean force)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::copy(%s,%s) called.", srcTblName, tgtTblName);

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_srcTblName = jenv_->NewStringUTF(srcTblName);
  if (js_srcTblName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DROP_PARAM;
  }

  jstring js_tgtTblName = jenv_->NewStringUTF(tgtTblName);
  if (js_tgtTblName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DROP_PARAM;
  }

  jboolean j_force = force;
  tsRecentJMFromJNI = JavaMethods_[JM_COPY].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(
       javaObj_, JavaMethods_[JM_COPY].methodID, 
       js_srcTblName, js_tgtTblName, j_force);
  
  jenv_->DeleteLocalRef(js_srcTblName);  

  jenv_->DeleteLocalRef(js_tgtTblName);  

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::copy()", srcTblName);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DROP_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::copy()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DROP_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}


//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::exists(const char* fileName, Int64 transID)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::exists(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_EXISTS_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_EXISTS_PARAM;
  }

  jlong j_tid = transID;  

  // boolean exists(java.lang.String);
  tsRecentJMFromJNI = JavaMethods_[JM_EXISTS].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_EXISTS].methodID, js_fileName, j_tid);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::exists()", fileName);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_EXISTS_EXCEPTION;
  }

  if (jresult == false) {
     jenv_->PopLocalFrame(NULL);
     return HBC_DONE;  // Table does not exist
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;  // Table exists.
}


//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::grant(const Text& user, const Text& tblName, const TextVec& actions)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::grant(%s, %s, %s) called.", user.data(), tblName.data(), actions.data());
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  int len = user.size();
  jbyteArray jba_user = jenv_->NewByteArray(len);
  if (jba_user == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GRANT_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GRANT_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_user, 0, len, (const jbyte*)user.data());

  len = tblName.size();
  jbyteArray jba_tblName = jenv_->NewByteArray(len);
  if (jba_tblName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GRANT_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GRANT_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_tblName, 0, len, (const jbyte*)tblName.data());

  jobjectArray j_actionCodes = NULL;
  if (!actions.empty())
  {
    QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  Adding %d actions.", actions.size());
    j_actionCodes = convertToStringObjectArray(actions);
    if (j_actionCodes == NULL)
    {
       getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::grant()", tblName.data());
       jenv_->PopLocalFrame(NULL);
       return HBC_ERROR_GRANT_PARAM;
    }
  }
  tsRecentJMFromJNI = JavaMethods_[JM_GRANT].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
       JavaMethods_[JM_GRANT].methodID, jba_user, jba_tblName, j_actionCodes);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::grant()", tblName.data());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GRANT_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::grant()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GRANT_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// Estimate row count for tblName by adding the entry counts from the trailer
// block of each HFile for the table, and dividing by the number of columns.
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::estimateRowCount(const char* tblName,
                                              Int32 partialRowSize,
                                              Int32 numCols,
                                              Int32 retryLimitMilliSeconds,
                                              NABoolean useCoprocessor,
                                              Int64& rowCount,
                                              Int32& breadCrumb)
{
  // Note: Please use HBC_ERROR_ROWCOUNT_EST_EXCEPTION only for
  // those error returns that call getExceptionDetails(). This
  // tells the caller that Java exception information is available.

  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::estimateRowCount(%s) called.", tblName);
  breadCrumb = 1;
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  breadCrumb = 3;
  jstring js_tblName = jenv_->NewStringUTF(tblName);
  if (js_tblName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_ROWCOUNT_EST_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_ROWCOUNT_EST_PARAM;
  }

  jint jPartialRowSize = partialRowSize;
  jint jNumCols = numCols;
  jint jRetryLimitMilliSeconds = retryLimitMilliSeconds;
  jlongArray jRowCount = jenv_->NewLongArray(1);
  enum JAVA_METHODS method = (useCoprocessor ? JM_EST_RC_COPROC : JM_EST_RC);
  tsRecentJMFromJNI = JavaMethods_[method].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[method].methodID,
                                              js_tblName, jPartialRowSize,
                                              jNumCols, jRetryLimitMilliSeconds, jRowCount);
  jboolean isCopy;
  jlong* arrayElems = jenv_->GetLongArrayElements(jRowCount, &isCopy);
  rowCount = *arrayElems;
  if (isCopy == JNI_TRUE)
    jenv_->ReleaseLongArrayElements(jRowCount, arrayElems, JNI_ABORT);

  breadCrumb = 4;
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::estimateRowCount()", tblName);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_ROWCOUNT_EST_EXCEPTION;
  }

  breadCrumb = 5;
  if (jresult == false)
  {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::estimateRowCount() returned false", getLastError());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_ROWCOUNT_EST_FALSE;
  }

  breadCrumb = 6;
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;  // Table exists.
}

HBC_RetCode HBaseClient_JNI::getLatestSnapshot(const char * tblName, char *& snapshotName, NAHeap * heap)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::getLatestSnapshot(%s) called.", tblName);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tblName);
  if (js_tblName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GET_LATEST_SNP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_LATEST_SNP_PARAM;
  }
  tsRecentJMFromJNI = JavaMethods_[JM_GET_LATEST_SNP].jm_full_name;
  jstring jresult = (jstring)jenv_->CallStaticObjectMethod(javaClass_, JavaMethods_[JM_GET_LATEST_SNP].methodID,js_tblName);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::getLatestSnapshot()", tblName);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_LATEST_SNP_EXCEPTION;
  }

  if (jresult == NULL)
    snapshotName = NULL;
  else
  {
    char * tmp = (char*)jenv_->GetStringUTFChars(jresult, NULL);
    snapshotName = new (heap) char[strlen(tmp)+1];
    strcpy(snapshotName, tmp);
    jenv_->ReleaseStringUTFChars(jresult, tmp);
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;  
}
HBC_RetCode HBaseClient_JNI::cleanSnpTmpLocation(const char * path)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::cleanSnpTmpLocation(%s) called.", path);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_path = jenv_->NewStringUTF(path);
  if (js_path == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CLEAN_SNP_TMP_LOC_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CLEAN_SNP_TMP_LOC_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_CLEAN_SNP_TMP_LOC].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_CLEAN_SNP_TMP_LOC].methodID,js_path);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::cleanupSnpTmpLocation()", path);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CLEAN_SNP_TMP_LOC_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::setArchivePermissions(const char * tbl)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::setArchivePermissions(%s) called.", tbl);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_tbl = jenv_->NewStringUTF(tbl);
  if (js_tbl == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_SET_ARC_PERMS_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_SET_ARC_PERMS_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_SET_ARC_PERMS].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_SET_ARC_PERMS].methodID,js_tbl);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::setArchivePermissions()", tbl);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_SET_ARC_PERMS_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::getBlockCacheFraction(float& frac)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, 
                 "HBaseClient_JNI::getBlockCacheFraction() called.");
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  tsRecentJMFromJNI = JavaMethods_[JM_GET_CAC_FRC].jm_full_name;
  jfloat jresult = jenv_->CallFloatMethod(javaObj_, 
                                          JavaMethods_[JM_GET_CAC_FRC].methodID);
  
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::getBlockCacheFraction()");
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_CACHE_FRAC_EXCEPTION;
  }
  frac = jresult;
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
JavaMethodInit* HBulkLoadClient_JNI::JavaMethods_ = NULL;
jclass HBulkLoadClient_JNI::javaClass_ = 0;
bool HBulkLoadClient_JNI::javaMethodsInitialized_ = false;
pthread_mutex_t HBulkLoadClient_JNI::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;


static const char* const hblcErrorEnumStr[] = ///need to update content
{
    "preparing parameters for init."
   ,"java exception in init."
   ,"java exception in cleanup."
   ,"java exception in close"
   ,"java exception in create_hfile()."
   ,"java exception in create_hfile()."
   ,"preparing parameters for add_to_hfile()."
   ,"java exception in add_to_hfile()."
   ,"preparing parameters for hblc_error_close_hfile()."
   ,"java exception in close_hfile()."
   ,"java exception in do_bulkload()."
   ,"java exception in do_bulkload()."
   ,"preparing parameters for bulkload_cleanup()."
   ,"java exception in bulkload_cleanup()."
   ,"preparing parameters for init_hblc()."
   ,"java exception in init_hblc()."
};
HBLC_RetCode HBulkLoadClient_JNI::init()
{
  static char className[]="org/trafodion/sql/HBulkLoadClient";
  HBLC_RetCode rc;

  if (isInitialized())
    return HBLC_OK;

  if (javaMethodsInitialized_)
    return (HBLC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
  else
  {
    pthread_mutex_lock(&javaMethodsInitMutex_);
    if (javaMethodsInitialized_)
    {
      pthread_mutex_unlock(&javaMethodsInitMutex_);
      return (HBLC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    }
    JavaMethods_ = new JavaMethodInit[JM_LAST];

    JavaMethods_[JM_CTOR       ].jm_name      = "<init>";
    JavaMethods_[JM_CTOR       ].jm_signature = "()V";
    JavaMethods_[JM_INIT_HFILE_PARAMS     ].jm_name      = "initHFileParams";
    JavaMethods_[JM_INIT_HFILE_PARAMS     ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;JLjava/lang/String;Ljava/lang/String;Ljava/lang/String;F)Z";
    JavaMethods_[JM_CLOSE_HFILE      ].jm_name      = "closeHFile";
    JavaMethods_[JM_CLOSE_HFILE      ].jm_signature = "()Z";
    JavaMethods_[JM_DO_BULK_LOAD     ].jm_name      = "doBulkLoad";
    JavaMethods_[JM_DO_BULK_LOAD     ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;ZZ)Z";
    JavaMethods_[JM_BULK_LOAD_CLEANUP].jm_name      = "bulkLoadCleanup";
    JavaMethods_[JM_BULK_LOAD_CLEANUP].jm_signature = "(Ljava/lang/String;)Z";
    JavaMethods_[JM_ADD_TO_HFILE_DB  ].jm_name      = "addToHFile";
    JavaMethods_[JM_ADD_TO_HFILE_DB  ].jm_signature = "(SLjava/lang/Object;Ljava/lang/Object;)Z";

    rc = (HBLC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    if (rc == HBLC_OK)
       javaMethodsInitialized_ = TRUE;
    pthread_mutex_unlock(&javaMethodsInitMutex_);
  }
  return rc;
}

#define ENC_STATIC_BUFFER_LEN 4096
static short encryptRowId(const char * encryptionInfo,
                          NAHeap * heap, Int32 numRows,
                          const char * rowId, Int32 rowIdLen,
                          Text &encRowId)
{
  short rc = 0;

  if (! encryptionInfo)
    return 0;

  Int32 header, rowIdKeyLen, dataKeyLen, rowidInitVecLen, dataInitVecLen,
    encDataLen;
  unsigned char * rowIdKey;
  unsigned char * dataKey;
  unsigned char * rowidInitVec;
  unsigned char * dataInitVec;
  Int16 rowidCipherType, dataCipherType;
  Text encryptedRow;
  char * encRowsPtr = NULL;
  
  rc = ComEncryption::extractEncryptionInfo
    (*(ComEncryption::EncryptionInfo*)encryptionInfo,
     header, rowidCipherType, dataCipherType,
     rowIdKey, rowIdKeyLen, dataKey, dataKeyLen, 
     rowidInitVec, rowidInitVecLen, dataInitVec, dataInitVecLen);
  if (rc)
    return -1;

  Int32 encRowIdMaxLen = rowIdLen + numRows * 16;
  char encRowIdBuffer[encRowIdMaxLen];

  Int32 encRowIdLen = 0;
  rc = ComEncryption::encryptRowId((unsigned char *)rowId, rowIdLen, rowIdKey,
                                   rowidInitVec,
                                   (unsigned char *)encRowIdBuffer, encRowIdLen);
  if (rc)
    {
      return -1;
    }

  encRowId.assign(encRowIdBuffer, encRowIdLen);

  return 0;
}

static short encryptRowsData(const char * encryptionInfo,
                             NAHeap * heap, Int32 numRows,
                             char * rowsData, Int32 rowsLen,
                             unsigned char* &encRows, Int32 &encRowsLen)
{
  short rc = 0;
  encRowsLen = 0;

  if (! encryptionInfo)
    return 0;

  unsigned char * rowIdKey;
  unsigned char * dataKey;
  unsigned char * rowidInitVec;
  unsigned char * dataInitVec;
  Int32 header, rowIdKeyLen, dataKeyLen, rowidInitVecLen, dataInitVecLen,
    encDataLen;
  Int16 rowidCipherType, dataCipherType;
  Text encryptedRow;
  unsigned char * encRowsPtr = NULL;
  
  rc = ComEncryption::extractEncryptionInfo
    (*(ComEncryption::EncryptionInfo*)encryptionInfo,
     header, rowidCipherType, dataCipherType,
     rowIdKey, rowIdKeyLen, dataKey, dataKeyLen, 
     rowidInitVec, rowidInitVecLen, dataInitVec, dataInitVecLen);
  if (rc)
    return -1;

  Int32 encRowsMaxLen = rowsLen + numRows * (dataInitVecLen + 16);
  if (encRowsMaxLen > ENC_STATIC_BUFFER_LEN)
    {
      encRows = new(heap) unsigned char[encRowsMaxLen];
    }

  encRowsPtr = encRows;
  
  char * rowsPtr = rowsData;

  for (int rowNum = 0; rowNum < numRows; rowNum++) {
    short numCols = bswap_16(*(short*)rowsPtr);
    *(short*)encRowsPtr = bswap_16(numCols);
    
    rowsPtr += 2;
    encRowsPtr += 2;
    
    for (short colIndex = 0; colIndex < numCols; colIndex++) {
      short colNameLen = bswap_16(*(short*)rowsPtr);
      *(short*)encRowsPtr = bswap_16(colNameLen);
      rowsPtr += 2;
      encRowsPtr += 2;
      
      str_cpy_all((char *)encRowsPtr, (const char *)rowsPtr, colNameLen);
      rowsPtr += colNameLen;
      encRowsPtr += colNameLen;
      
      int dataLen = bswap_32(*(int*)rowsPtr);
      rowsPtr += sizeof(int);
      
      unsigned char * encDataLenPtr = encRowsPtr;
      encRowsPtr += sizeof(int);
      
      // add init vector to beginning of encrypted row in plain text
      str_cpy_all((char *)encRowsPtr, (const char *)dataInitVec, dataInitVecLen);
      encRowsPtr += dataInitVecLen;
      rc = ComEncryption::encryptData(dataCipherType,
                                      (unsigned char *)rowsPtr, dataLen, dataKey,
                                      dataInitVec,
                                      (unsigned char *)encRowsPtr, encDataLen);
      if (rc)
        {
          return -1;
        }
      
      *(int*)encDataLenPtr = bswap_32((int)(dataInitVecLen+encDataLen));
      rowsPtr += dataLen;
      encRowsPtr += encDataLen;
    } // for cols in a row
  } // for rows

  encRowsLen = encRowsPtr - encRows;

  return 0;
}

char* HBulkLoadClient_JNI::getErrorText(HBLC_RetCode errEnum)
{
  if (errEnum < (HBLC_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)errEnum);
  else
    return (char*)hblcErrorEnumStr[errEnum-HBLC_FIRST-1];
}

HBLC_RetCode HBulkLoadClient_JNI::initHFileParams(
                        const HbaseStr &tblName,
                        const Text& hFileLoc,
                        const Text& hfileName,
                        Int64 maxHFileSize,
                        const Text& hFileSampleLoc,
                        const Text& hfileSampleName,
                        float fSampleRate)
{
  FUNCTION_DEBUG("%s(%s, %s, %s, %ld, %s, %s) called.",
                 __FUNCTION__, hFileLoc.data(), hfileName.data(), tblName.val, 
                 maxHFileSize, hFileSampleLoc.data(), hfileSampleName.data());
  if (initJNIEnv() != JOI_OK)
  {
     EXIT_FUNCTION(HBLC_ERROR_INIT_PARAM);
     return HBLC_ERROR_INIT_PARAM;
  }

  HBLC_RetCode retValue = HBLC_OK;
  jboolean jresult = false;
  jlong j_maxSize = maxHFileSize;
  jfloat j_sampleRate = fSampleRate;
  jstring js_tabName = NULL;
  jstring js_hfileName = NULL;
  jstring js_sampleFileLoc = NULL;
  jstring js_hfileSampleName = NULL;
  jstring js_hFileLoc = jenv_->NewStringUTF(hFileLoc.c_str());
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_hFileLoc, hFileLoc.c_str(), (HBLC_RetCode)JOI_ERROR_NEWOBJ, HBLC_ERROR_CREATE_HFILE_PARAM);

  js_hfileName = jenv_->NewStringUTF(hfileName.c_str());
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_hfileName, hfileName.c_str(), (HBLC_RetCode)JOI_ERROR_NEWOBJ, HBLC_ERROR_CREATE_HFILE_PARAM);

  js_tabName = jenv_->NewStringUTF(tblName.val);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_tabName, tblName.val, (HBLC_RetCode)JOI_ERROR_NEWOBJ, HBLC_ERROR_CREATE_HFILE_PARAM);

  js_sampleFileLoc = jenv_->NewStringUTF(hFileSampleLoc.c_str());
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_sampleFileLoc, hFileSampleLoc.c_str(), (HBLC_RetCode)JOI_ERROR_NEWOBJ, HBLC_ERROR_CREATE_HFILE_PARAM);

  js_hfileSampleName = jenv_->NewStringUTF(hfileSampleName.c_str());
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_hfileSampleName, hfileSampleName.c_str(), (HBLC_RetCode)JOI_ERROR_NEWOBJ, HBLC_ERROR_CREATE_HFILE_PARAM);

  CHECK_EXCEPTION_AND_RETURN_ERROR(tblName.val, HBLC_ERROR_CREATE_HFILE_EXCEPTION);

  tsRecentJMFromJNI = JavaMethods_[JM_INIT_HFILE_PARAMS].jm_full_name;
  jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_INIT_HFILE_PARAMS].methodID, js_hFileLoc,
                                     js_hfileName, j_maxSize, js_tabName, js_sampleFileLoc, js_hfileSampleName, j_sampleRate);

  CHECK_EXCEPTION_AND_RETURN_ERROR(tblName.val, HBLC_ERROR_CREATE_HFILE_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, HBLC_ERROR_CREATE_HFILE_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_hFileLoc);
  CHECK_AND_DELETE_NEWOBJ(js_hfileName);
  CHECK_AND_DELETE_NEWOBJ(js_tabName);
  CHECK_AND_DELETE_NEWOBJ(js_sampleFileLoc);
  CHECK_AND_DELETE_NEWOBJ(js_hfileSampleName);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

HBLC_RetCode HBulkLoadClient_JNI::addToHFile( short rowIDLen, HbaseStr &rowIDs,
                                              HbaseStr &rows, 
                                              ExHbaseAccessStats *hbs,
                                              const char * encryptionInfo,
                                              NAHeap * heap)
{
  ENTRY_FUNCTION();
  if (initJNIEnv() != JOI_OK)
  {
     EXIT_FUNCTION(HBLC_ERROR_INIT_PARAM);
     return HBLC_ERROR_INIT_PARAM;
  }

  HBLC_RetCode retValue = HBLC_OK;
  jboolean jresult = 0;
  jobject jRows = NULL;
  Int32 encRowsLen = 0;
  jshort j_rowIDLen = rowIDLen;
  unsigned char * encRowsData = NULL;
  unsigned char encDataBuffer[ENC_STATIC_BUFFER_LEN];
  jobject jRowIDs = jenv_->NewDirectByteBuffer(rowIDs.val, rowIDs.len);
  CHECK_RESULT_AND_SET_RETURN_ERROR(jRowIDs, rowIDs.val, (HBLC_RetCode)JOI_ERROR_NEWOBJ, HBLC_ERROR_ADD_TO_HFILE_PARAM);

  if (encryptionInfo) {
    encRowsData = encDataBuffer;
    short numRows = bswap_16(*(short*)rowIDs.val);
    short rc = encryptRowsData(encryptionInfo, heap, numRows, rows.val, 
                               rows.len, encRowsData, encRowsLen);
    if (rc) {
      GetCliGlobals()->setJniErrorStr(getErrorText(HBLC_ERROR_ADD_TO_HFILE_EXCEPTION));
      retValue = HBLC_ERROR_ADD_TO_HFILE_EXCEPTION;
      goto errorLabel;
    }
    
    if (encRowsData) {
      jRows = jenv_->NewDirectByteBuffer(encRowsData, encRowsLen);
    }
  }
  else
    jRows = jenv_->NewDirectByteBuffer(rows.val, rows.len);

  CHECK_RESULT_AND_SET_RETURN_ERROR(jRows, encRowsData, (HBLC_RetCode)JOI_ERROR_NEWOBJ, HBLC_ERROR_ADD_TO_HFILE_PARAM);

  if (hbs)
    hbs->getHbaseTimer().start();

  tsRecentJMFromJNI = JavaMethods_[JM_ADD_TO_HFILE_DB].jm_full_name;
  jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_ADD_TO_HFILE_DB].methodID, j_rowIDLen, jRowIDs, jRows);

  if (hbs)
  {
    hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
    hbs->incHbaseCalls();
  }

  CHECK_EXCEPTION_AND_RETURN_ERROR(rows.val, HBLC_ERROR_ADD_TO_HFILE_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, HBLC_ERROR_ADD_TO_HFILE_EXCEPTION);

errorLabel:
  if (encRowsData && (encRowsData != encDataBuffer))
      NADELETEBASIC(encRowsData, heap);
  if (encRowsData && (encRowsData != encDataBuffer))
      NADELETEBASIC(encRowsData, heap);

  CHECK_AND_DELETE_NEWOBJ(jRowIDs);
  CHECK_AND_DELETE_NEWOBJ(jRows);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

HBLC_RetCode HBulkLoadClient_JNI::closeHFile(
                        const HbaseStr &tblName)
{
  FUNCTION_DEBUG("%s(%s) called.", __FUNCTION__, tblName.val);
  if (initJNIEnv() != JOI_OK)
  {
     EXIT_FUNCTION(HBLC_ERROR_INIT_PARAM);
     return HBLC_ERROR_INIT_PARAM;
  }

  jboolean jresult = false;
  HBLC_RetCode retValue = HBLC_OK;
  tsRecentJMFromJNI = JavaMethods_[JM_CLOSE_HFILE].jm_full_name;
  jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_CLOSE_HFILE].methodID);

  CHECK_EXCEPTION_AND_RETURN_ERROR(tblName.val, HBLC_ERROR_CLOSE_HFILE_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, HBLC_ERROR_CLOSE_HFILE_EXCEPTION);

errorLabel:
  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}


HBLC_RetCode HBulkLoadClient_JNI::doBulkLoad(
                             const HbaseStr &tblName,
                             const Text& prepLocation,
                             const Text& tableName,
                             NABoolean quasiSecure,
                             NABoolean snapshot)
{
  FUNCTION_DEBUG("%s(%s, %s, %s) called.", __FUNCTION__, 
                 tblName.val, prepLocation.data(), tableName.data());

  if (initJNIEnv() != JOI_OK)
  {
     EXIT_FUNCTION(HBLC_ERROR_INIT_PARAM);
     return HBLC_ERROR_INIT_PARAM;
  }

  HBLC_RetCode retValue = HBLC_OK;
  jboolean jresult = 0;
  jboolean j_snapshot = snapshot;
  jboolean j_quasiSecure = quasiSecure;
  jstring js_TableName = NULL;
  jstring js_PrepLocation = jenv_->NewStringUTF(prepLocation.c_str());
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_PrepLocation, prepLocation.c_str(), (HBLC_RetCode)JOI_ERROR_NEWOBJ, HBLC_ERROR_DO_BULKLOAD_PARAM);
  js_TableName = jenv_->NewStringUTF(tableName.c_str());
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_TableName, tableName.c_str(), (HBLC_RetCode)JOI_ERROR_NEWOBJ, HBLC_ERROR_DO_BULKLOAD_PARAM);  
  tsRecentJMFromJNI = JavaMethods_[JM_DO_BULK_LOAD].jm_full_name;
  jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_DO_BULK_LOAD].methodID, js_PrepLocation, js_TableName, j_quasiSecure, j_snapshot);

  CHECK_EXCEPTION_AND_RETURN_ERROR(tableName.data(), HBLC_ERROR_DO_BULKLOAD_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, HBLC_ERROR_DO_BULKLOAD_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_PrepLocation);
  CHECK_AND_DELETE_NEWOBJ(js_TableName);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

HBLC_RetCode HBulkLoadClient_JNI::bulkLoadCleanup(
                             const HbaseStr &tblName,
                             const Text& location)
{
  FUNCTION_DEBUG("%s(%s, %s) called.", 
                 __FUNCTION__, tblName.val, location.data());
  if (initJNIEnv() != JOI_OK)
  {
     EXIT_FUNCTION(HBLC_ERROR_INIT_PARAM);
     return HBLC_ERROR_INIT_PARAM;
  }

  jboolean jresult = 0;
  HBLC_RetCode retValue = HBLC_OK;
  jstring js_location = jenv_->NewStringUTF(location.c_str());
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_location, location.c_str(), (HBLC_RetCode)JOI_ERROR_NEWOBJ, HBLC_ERROR_BULKLOAD_CLEANUP_PARAM);

  tsRecentJMFromJNI = JavaMethods_[JM_BULK_LOAD_CLEANUP].jm_full_name;
  jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_BULK_LOAD_CLEANUP].methodID, js_location);
  CHECK_EXCEPTION_AND_RETURN_ERROR(tblName.val, HBLC_ERROR_BULKLOAD_CLEANUP_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, HBLC_ERROR_BULKLOAD_CLEANUP_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_location);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
JavaMethodInit* BackupRestoreClient_JNI::JavaMethods_ = NULL;
jclass BackupRestoreClient_JNI::javaClass_ = 0;
bool BackupRestoreClient_JNI::javaMethodsInitialized_ = false;
pthread_mutex_t BackupRestoreClient_JNI::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;


static const char* const brcErrorEnumStr[] = ///need to update content
{
    "Preparing parameters for init"       // BRC_ERROR_INIT_PARAM
    , "Create snapshot exception"         // BRC_ERROR_CREATE_SNAPSHOT_EXCEPTION
    , "Restore snapshot exception"        // BRC_ERROR_RESTORE_SNAPSHOT_EXCEPTION
    , "Delete backup exception"           // BRC_ERROR_DELETE_BACKUP_EXCEPTION
    , "Get backup type exception"         // no used BRC_ERROR_GET_BACKUP_TYPE_EXCEPTION
    , "Get extended attributes exception" // no used BRC_ERROR_GET_EXTENDED_ATTRIBUTES_EXCEPTION
    , "Get backup status exception"       // no used BRC_ERROR_GET_BACKUP_STATUS_EXCEPTION
    , "List all backups exception"        // BRC_ERROR_LIST_ALL_BACKUPS_EXCEPTION
    , "Export import backup exception"    // BRC_ERROR_EXPORT_IMPORT_BACKUP_EXCEPTION
    , "Exception in initBRC()"            // no used BRC_ERROR_INIT_BRC_EXCEPTION
    , "Lock holder exception"             // No used error num, BRC_ERROR_LOCK_HOLDER_EXCEPTION
    , "Operation lock exception"          // BRC_ERROR_OPERATION_LOCK_EXCEPTION
    , "Operation unlock exception"        // BRC_ERROR_OPERATION_UNLOCK_EXCEPTION
    , "Backup nonfatal"                   // BRC_ERROR_BACKUP_NONFATAL
    , "Set hiatus exception"              // No used BRC_ERROR_SET_HIATUS_EXCEPTION
    , "Clear hiatus exception"            // No used BRC_ERROR_CLEAR_HIATUS_EXCEPTION
    , "Get linked backup exception"       // No used BRC_ERROR_GET_LINKED_BACKUP_EXCEPTION
    , "Unknown Error"
};
BRC_RetCode BackupRestoreClient_JNI::init()
{
  static char className[]="org/apache/hadoop/hbase/pit/BackupRestoreClient";
  BRC_RetCode rc;

  if (isInitialized())
    return BRC_OK;

  if (javaMethodsInitialized_)
    return (BRC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
  else
  {
    pthread_mutex_lock(&javaMethodsInitMutex_);
    if (javaMethodsInitialized_)
    {
      pthread_mutex_unlock(&javaMethodsInitMutex_);
      return (BRC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    }
    JavaMethods_ = new JavaMethodInit[JM_LAST];
    
    JavaMethods_[JM_CTOR       ].jm_name      = "<init>";
    JavaMethods_[JM_CTOR       ].jm_signature = "()V";
    JavaMethods_[JM_VALIDATE_BACKUP_OBJECTS].jm_name = "validateBackupObjects";
    JavaMethods_[JM_VALIDATE_BACKUP_OBJECTS].jm_signature = "([Ljava/lang/Object;[Ljava/lang/Object;Ljava/lang/String;Ljava/lang/String;)Z";
    JavaMethods_[JM_BACKUP_OBJECTS].jm_name = "backupObjects";
    JavaMethods_[JM_BACKUP_OBJECTS].jm_signature = "([Ljava/lang/Object;[Ljava/lang/Object;[Ljava/lang/Object;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;II)Z";
    JavaMethods_[JM_SNAPSHOT_INCR_BACKUP].jm_name = "createSnapshotForIncrBackup";
    JavaMethods_[JM_SNAPSHOT_INCR_BACKUP].jm_signature = "([Ljava/lang/Object;)Z";
    JavaMethods_[JM_RESTORE_OBJECTS].jm_name = "restoreObjects";
    JavaMethods_[JM_RESTORE_OBJECTS].jm_signature = "(Ljava/lang/String;[Ljava/lang/Object;[Ljava/lang/Object;Ljava/lang/String;ZZZII)[[B";
    JavaMethods_[JM_FINALIZE_BACKUP].jm_name = "finalizeBackup";
    JavaMethods_[JM_FINALIZE_BACKUP].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)Z";
    JavaMethods_[JM_FINALIZE_RESTORE].jm_name = "finalizeRestore";
    JavaMethods_[JM_FINALIZE_RESTORE].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)Z";
    JavaMethods_[JM_DELETE_BACKUP].jm_name = "deleteBackup";
    JavaMethods_[JM_DELETE_BACKUP].jm_signature = "(Ljava/lang/String;ZZZZ)Z";
    JavaMethods_[JM_EXPORT_IMPORT_BACKUP].jm_name = "exportOrImportBackup";
    JavaMethods_[JM_EXPORT_IMPORT_BACKUP].jm_signature = "(Ljava/lang/String;ZZLjava/lang/String;II)Z";
    JavaMethods_[JM_LIST_ALL_BACKUPS].jm_name = "listAllBackups";
    JavaMethods_[JM_LIST_ALL_BACKUPS].jm_signature = "(ZZ)[[B";
    JavaMethods_[JM_GET_BACKUP_TYPE  ].jm_name      = "getBackupType";
    JavaMethods_[JM_GET_BACKUP_TYPE  ].jm_signature = "(Ljava/lang/String;)Ljava/lang/String;";
    JavaMethods_[JM_GET_EXTENDED_ATTRIBUTES].jm_name      = "getExtendedAttributes";
    JavaMethods_[JM_GET_EXTENDED_ATTRIBUTES].jm_signature = "(Ljava/lang/String;)Ljava/lang/String;";
    JavaMethods_[JM_GET_BACKUP_STATUS].jm_name      = "getBackupStatus";
    JavaMethods_[JM_GET_BACKUP_STATUS].jm_signature = "(Ljava/lang/String;)Ljava/lang/String;";
    JavaMethods_[JM_GET_PRIOR_BACKUP_TAG].jm_name      = "getPriorBackupTag";
    JavaMethods_[JM_GET_PRIOR_BACKUP_TAG].jm_signature = "(Ljava/lang/String;)Ljava/lang/String;";
    JavaMethods_[JM_GET_RESTORE_TO_TS_BACKUP_TAG].jm_name      = "getRestoreToTsBackupTag";
    JavaMethods_[JM_GET_RESTORE_TO_TS_BACKUP_TAG].jm_signature = "(Ljava/lang/String;)Ljava/lang/String;";
    JavaMethods_[JM_LOCK_HOLDER].jm_name      = "lockHolder";
    JavaMethods_[JM_LOCK_HOLDER].jm_signature = "()Ljava/lang/String;";
    JavaMethods_[JM_OPERATION_LOCK].jm_name      = "operationLock";
    JavaMethods_[JM_OPERATION_LOCK].jm_signature = "(Ljava/lang/String;)V";
    JavaMethods_[JM_OPERATION_UNLOCK].jm_name      = "operationUnlock";
    JavaMethods_[JM_OPERATION_UNLOCK].jm_signature = "(Ljava/lang/String;ZZ)V";
    JavaMethods_[JM_SET_HIATUS].jm_name      = "setHiatus";
    JavaMethods_[JM_SET_HIATUS].jm_signature = "(Ljava/lang/String;ZZZI)Z";
    JavaMethods_[JM_CLEAR_HIATUS].jm_name      = "clearHiatus";
    JavaMethods_[JM_CLEAR_HIATUS].jm_signature = "(Ljava/lang/String;)Z";
    JavaMethods_[JM_GET_LINKED_BACKUPS].jm_name      = "getBackupNameLink";
    JavaMethods_[JM_GET_LINKED_BACKUPS].jm_signature = "(Ljava/lang/String;)[[B";
    
    rc = (BRC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    if (rc == BRC_OK)
       javaMethodsInitialized_ = TRUE;
    pthread_mutex_unlock(&javaMethodsInitMutex_);
  }
  return rc;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
BRC_RetCode BackupRestoreClient_JNI::backupObjects(const TextVec& tables,
                                                   const TextVec& tableFlags,
                                                   const TextVec& lobLocList,
                                                   const char* backuptag,
                                                   const char* extendedBackupAttributes,
                                                   const char* backupType,
                                                   const int backupThreads,
                                                   const int progressUpdateDelay)
{
  FUNCTION_DEBUG("%s(%s) called.", __FUNCTION__, (backuptag ? backuptag : " "));

  if (initJNIEnv() != JOI_OK)
  {
    EXIT_FUNCTION(BRC_ERROR_INIT_PARAM);
    return BRC_ERROR_INIT_PARAM;
  }

  BRC_RetCode retValue = BRC_OK;
  jboolean jresult = NULL;
  jobjectArray j_tables = NULL;
  jobjectArray j_tableFlags = NULL;
  jobjectArray j_lobLocList = NULL;
  jstring js_backuptag = NULL;
  jstring js_backupType = NULL;
  jstring js_extendedBackupAttributes = NULL;
  jint ji_backupThreads = backupThreads;
  jint ji_progressUpdateDelay = progressUpdateDelay;
  if(!tables.empty()) {
      QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "%s %d tables.", __FUNCTION__, tables.size());
      j_tables = convertToStringObjectArray(tables);
      CHECK_RESULT_AND_RETURN_ERROR(j_tables, backuptag, BRC_ERROR_BACKUP_NONFATAL);
  }

  if(!tableFlags.empty()) {
      j_tableFlags = convertToStringObjectArray(tableFlags);
      CHECK_RESULT_AND_RETURN_ERROR(j_tableFlags, backuptag, BRC_ERROR_BACKUP_NONFATAL);
  }

  if(!lobLocList.empty()) {
      j_lobLocList = convertToStringObjectArray(lobLocList);
      CHECK_RESULT_AND_RETURN_ERROR(j_lobLocList, backuptag, BRC_ERROR_BACKUP_NONFATAL);
  }

  js_backuptag = jenv_->NewStringUTF(backuptag);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_BACKUP_NONFATAL);

  js_extendedBackupAttributes = jenv_->NewStringUTF(extendedBackupAttributes);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_extendedBackupAttributes, extendedBackupAttributes, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_BACKUP_NONFATAL);

  js_backupType = jenv_->NewStringUTF(backupType);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backupType, backupType, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_BACKUP_NONFATAL);


  tsRecentJMFromJNI = JavaMethods_[JM_VALIDATE_BACKUP_OBJECTS].jm_full_name;
  jresult = jenv_->CallBooleanMethod(
       javaObj_, JavaMethods_[JM_VALIDATE_BACKUP_OBJECTS].methodID, j_tables,
       j_tableFlags,
       js_backuptag, js_backupType);

  CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, BRC_ERROR_BACKUP_NONFATAL);
  CHECK_AND_REPORT_ERROR(!jresult, BRC_ERROR_BACKUP_NONFATAL);

  tsRecentJMFromJNI = JavaMethods_[JM_BACKUP_OBJECTS].jm_full_name;
  jresult = jenv_->CallBooleanMethod(
       javaObj_, JavaMethods_[JM_BACKUP_OBJECTS].methodID, 
       j_tables,
       j_tableFlags, 
       j_lobLocList, 
       js_backuptag, 
       js_extendedBackupAttributes, 
       js_backupType,
       ji_backupThreads,
       ji_progressUpdateDelay);
  
  CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, BRC_ERROR_CREATE_SNAPSHOT_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, BRC_ERROR_CREATE_SNAPSHOT_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(j_tables);
  CHECK_AND_DELETE_NEWOBJ(j_tableFlags);
  CHECK_AND_DELETE_NEWOBJ(j_lobLocList);
  CHECK_AND_DELETE_NEWOBJ(js_backuptag);
  CHECK_AND_DELETE_NEWOBJ(js_extendedBackupAttributes);
  CHECK_AND_DELETE_NEWOBJ(js_backupType);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
BRC_RetCode BackupRestoreClient_JNI::setHiatus(const char* tableName,
                                               NABoolean lockOperation,
                                               NABoolean createSnapIfNotExist,
                                               NABoolean ignoreSnapIfNotExist,
                                               const int parallelThreads) 
{
  FUNCTION_DEBUG("%s(%s) called.", __FUNCTION__, tableName);
  if (initJNIEnv() != JOI_OK)
  {
    EXIT_FUNCTION(BRC_ERROR_INIT_PARAM);
    return BRC_ERROR_INIT_PARAM;
  }

  jboolean jresult = false;
  BRC_RetCode retValue = BRC_OK;
  jint ji_parallelThreads = parallelThreads;
  jstring js_tableName = jenv_->NewStringUTF(tableName);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_tableName, tableName, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_BACKUP_NONFATAL);
  
  tsRecentJMFromJNI = JavaMethods_[JM_SET_HIATUS].jm_full_name;
  jresult = jenv_->CallBooleanMethod(
       javaObj_, JavaMethods_[JM_SET_HIATUS].methodID, js_tableName,
       (jboolean)lockOperation,
       (jboolean)createSnapIfNotExist,
       (jboolean)ignoreSnapIfNotExist,
       ji_parallelThreads);
  
  CHECK_EXCEPTION_AND_RETURN_ERROR(tableName, BRC_ERROR_BACKUP_NONFATAL);
  CHECK_AND_REPORT_ERROR(!jresult, BRC_ERROR_BACKUP_NONFATAL); 

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_tableName);
  
  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
BRC_RetCode BackupRestoreClient_JNI::clearHiatus(const char* tableName) 
{
  FUNCTION_DEBUG("%s(%s) called.", __FUNCTION__, tableName);
  if (initJNIEnv() != JOI_OK)
  {
    EXIT_FUNCTION(BRC_ERROR_INIT_PARAM);
    return BRC_ERROR_INIT_PARAM;
  }

  jboolean jresult = false;
  BRC_RetCode retValue = BRC_OK;
  jstring js_tableName = jenv_->NewStringUTF(tableName);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_tableName, tableName, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_BACKUP_NONFATAL);
  
  tsRecentJMFromJNI = JavaMethods_[JM_CLEAR_HIATUS].jm_full_name;
  jresult = jenv_->CallBooleanMethod(
          javaObj_, JavaMethods_[JM_CLEAR_HIATUS].methodID, js_tableName);
       
  CHECK_EXCEPTION_AND_RETURN_ERROR(tableName, BRC_ERROR_BACKUP_NONFATAL);
  CHECK_AND_REPORT_ERROR(!jresult, BRC_ERROR_BACKUP_NONFATAL); 

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_tableName);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
BRC_RetCode BackupRestoreClient_JNI::createSnapshotForIncrBackup(
     const TextVec& tables)
{
  ENTRY_FUNCTION();
  if (initJNIEnv() != JOI_OK)
  {
    EXIT_FUNCTION(BRC_ERROR_INIT_PARAM);
    return BRC_ERROR_INIT_PARAM;
  }

  BRC_RetCode retValue = BRC_OK;
  jboolean jresult = false;
  jobjectArray j_tables = NULL;
  if(!tables.empty())
  {
      QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "%s adding %d tables.", __FUNCTION__, tables.size());
      j_tables = convertToStringObjectArray(tables);
      CHECK_RESULT_AND_RETURN_ERROR(j_tables, "tables is null", BRC_ERROR_CREATE_SNAPSHOT_EXCEPTION);
  }
		
  tsRecentJMFromJNI = JavaMethods_[JM_SNAPSHOT_INCR_BACKUP].jm_full_name;
  jresult = jenv_->CallBooleanMethod(
          javaObj_, JavaMethods_[JM_SNAPSHOT_INCR_BACKUP].methodID, j_tables);
  
  CHECK_EXCEPTION_AND_RETURN_ERROR("call snapshot incr backup failed", BRC_ERROR_CREATE_SNAPSHOT_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, BRC_ERROR_CREATE_SNAPSHOT_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(j_tables);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr>* BackupRestoreClient_JNI::restoreObjects
(const char* backuptag,
 const TextVec *schemas,
 const TextVec *tables,
 const char* restoreToTS,
 NABoolean showObjects,
 NABoolean saveObjects,
 NABoolean restoreSavedObjects,
 int parallelThreads,
 const int progressUpdateDelay,
 NAHeap *heap)
{
  FUNCTION_DEBUG("%s(%s) called.", __FUNCTION__, (backuptag ? backuptag : " "));
  if (initJNIEnv() != JOI_OK)
  {
      EXIT_FUNCTION(HBC_ERROR_INIT_PARAM);
      return NULL;
  }

  NAArray<HbaseStr>* retValue = NULL;
  jint retcode = 0;
  jarray j_backupList = NULL;
  jstring js_timestamp = NULL;
  jobjectArray j_tables = NULL;
  jobjectArray j_schemas = NULL;
  jint ji_parallelThreads = parallelThreads;
  jint ji_progressUpdateDelay = progressUpdateDelay;

  jstring js_backuptag = jenv_->NewStringUTF(backuptag);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, NULL);

  if(schemas && !schemas->empty())
    {
      QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "%s adding %d schemas.", __FUNCTION__, schemas->size());
      j_schemas = convertToStringObjectArray(*schemas);
      CHECK_RESULT_AND_RETURN_ERROR(j_schemas, backuptag, NULL);
    }

  if(tables && !tables->empty())
    {
      QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "%s adding %d tables.", __FUNCTION__, tables->size());
      j_tables = convertToStringObjectArray(*tables);
      CHECK_RESULT_AND_RETURN_ERROR(j_tables, backuptag, NULL);
    }

  js_timestamp = jenv_->NewStringUTF(restoreToTS);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_timestamp, restoreToTS, (BRC_RetCode)JOI_ERROR_NEWOBJ, NULL);
    
  tsRecentJMFromJNI = JavaMethods_[JM_RESTORE_OBJECTS].jm_full_name;
  j_backupList = (jarray)jenv_->CallObjectMethod(
       javaObj_, 
       JavaMethods_[JM_RESTORE_OBJECTS].methodID, 
       js_backuptag, 
       j_schemas,
       j_tables,
       js_timestamp,
       (jboolean)showObjects,
       (jboolean)saveObjects,
       (jboolean)restoreSavedObjects,
       ji_parallelThreads,
       ji_progressUpdateDelay);

  CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, NULL);
  if (j_backupList != NULL) {
    retcode = convertByteArrayObjectArrayToNAArray(heap, j_backupList, &retValue);
    if (retcode == 0)
        retValue = NULL;
  }

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_backuptag);
  CHECK_AND_DELETE_NEWOBJ(j_schemas);
  CHECK_AND_DELETE_NEWOBJ(j_tables);
  CHECK_AND_DELETE_NEWOBJ(js_timestamp);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

BRC_RetCode BackupRestoreClient_JNI::finalizeBackup(
     const char* backuptag,
     const char* backupType)
{
  FUNCTION_DEBUG("%s(%s) called.", __FUNCTION__, (backuptag ? backuptag : " "));
  if (initJNIEnv() != JOI_OK)
  {
    EXIT_FUNCTION(BRC_ERROR_INIT_PARAM);
    return BRC_ERROR_INIT_PARAM;
  }

  BRC_RetCode retValue = BRC_OK;
  jboolean jresult = false;
  jstring js_backupType = NULL;
  jstring js_backuptag = jenv_->NewStringUTF(backuptag);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_CREATE_SNAPSHOT_EXCEPTION);

  js_backupType = jenv_->NewStringUTF(backupType);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backupType, backupType, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_CREATE_SNAPSHOT_EXCEPTION);

  tsRecentJMFromJNI = JavaMethods_[JM_FINALIZE_BACKUP].jm_full_name;
  jresult = jenv_->CallBooleanMethod(
       javaObj_, JavaMethods_[JM_FINALIZE_BACKUP].methodID,
       js_backuptag, js_backupType);

  CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, BRC_ERROR_CREATE_SNAPSHOT_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, BRC_ERROR_CREATE_SNAPSHOT_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_backuptag);
  CHECK_AND_DELETE_NEWOBJ(js_backupType);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

BRC_RetCode BackupRestoreClient_JNI::finalizeRestore(
     const char* backuptag,
     const char* backupType)
{
  FUNCTION_DEBUG("%s(%s) called.", __FUNCTION__, (backuptag ? backuptag : " "));
  if (initJNIEnv() != JOI_OK)
  {
    EXIT_FUNCTION(BRC_ERROR_INIT_PARAM);
    return BRC_ERROR_INIT_PARAM;
  }

  BRC_RetCode retValue = BRC_OK;
  jboolean jresult = false;
  jstring js_backupType = NULL;
  jstring js_backuptag = jenv_->NewStringUTF(backuptag);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_RESTORE_SNAPSHOT_EXCEPTION);

  js_backupType = jenv_->NewStringUTF(backupType);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backupType, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_RESTORE_SNAPSHOT_EXCEPTION);

  tsRecentJMFromJNI = JavaMethods_[JM_FINALIZE_RESTORE].jm_full_name;
  jresult = jenv_->CallBooleanMethod(
       javaObj_, JavaMethods_[JM_FINALIZE_RESTORE].methodID,
       js_backuptag, js_backupType);

  CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, BRC_ERROR_RESTORE_SNAPSHOT_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, BRC_ERROR_RESTORE_SNAPSHOT_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_backuptag);
  CHECK_AND_DELETE_NEWOBJ(js_backupType);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
BRC_RetCode BackupRestoreClient_JNI::deleteBackup(const char* backuptag,
                                                  NABoolean timestamp,
                                                  NABoolean cascade,
                                                  NABoolean force,
                                                  NABoolean skipLock)
{
    FUNCTION_DEBUG("%s(%s) called.", __FUNCTION__, (backuptag ? backuptag : " "));
    if (initJNIEnv() != JOI_OK)
    {
       EXIT_FUNCTION(BRC_ERROR_INIT_PARAM);
       return BRC_ERROR_INIT_PARAM;
    }

    BRC_RetCode retValue = BRC_OK; 
    jboolean jresult = false;
    jboolean j_timestamp = timestamp; 
    jstring js_backuptag = jenv_->NewStringUTF(backuptag);
    CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_DELETE_BACKUP_EXCEPTION);

    tsRecentJMFromJNI = JavaMethods_[JM_DELETE_BACKUP].jm_full_name;
    jresult = jenv_->CallBooleanMethod(
            javaObj_, JavaMethods_[JM_DELETE_BACKUP].methodID, js_backuptag, 
            j_timestamp, (jboolean)cascade, (jboolean)force, (jboolean)skipLock);

    CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, BRC_ERROR_DELETE_BACKUP_EXCEPTION);
    CHECK_AND_REPORT_ERROR(!jresult, BRC_ERROR_DELETE_BACKUP_EXCEPTION);

errorLabel:
    CHECK_AND_DELETE_NEWOBJ(js_backuptag);

    jenv_->PopLocalFrame(NULL);
    EXIT_FUNCTION(retValue);
    return retValue;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
BRC_RetCode BackupRestoreClient_JNI::exportOrImportBackup(const char* backuptag,
                                                          NABoolean isExport,
                                                          NABoolean override,
                                                          const char* location,
                                                          int parallelThreads,
                                                          const int progressUpdateDelay)
{
  FUNCTION_DEBUG("%s (%s) called.", __FUNCTION__, (backuptag ? backuptag : " "));

  if (initJNIEnv() != JOI_OK)
  {
    EXIT_FUNCTION(BRC_ERROR_INIT_PARAM);
    return BRC_ERROR_INIT_PARAM;
  }

  BRC_RetCode retValue = BRC_OK; 
  jboolean jresult = false;
  jstring js_location = NULL;
  jint ji_parallelThreads = parallelThreads;
  jint ji_progressUpdateDelay = progressUpdateDelay;
  jstring js_backuptag = jenv_->NewStringUTF(backuptag);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_EXPORT_IMPORT_BACKUP_EXCEPTION); 

  js_location = jenv_->NewStringUTF(location);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_location, location, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_EXPORT_IMPORT_BACKUP_EXCEPTION); 
  
  tsRecentJMFromJNI = JavaMethods_[JM_EXPORT_IMPORT_BACKUP].jm_full_name;
  jresult = jenv_->CallBooleanMethod(
       javaObj_, JavaMethods_[JM_EXPORT_IMPORT_BACKUP].methodID, 
       js_backuptag,
       (jboolean)isExport,
       (jboolean)override,
       js_location,
       ji_parallelThreads,
       ji_progressUpdateDelay);
  
  CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, BRC_ERROR_EXPORT_IMPORT_BACKUP_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, BRC_ERROR_EXPORT_IMPORT_BACKUP_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_backuptag);
  CHECK_AND_DELETE_NEWOBJ(js_location);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

BRC_RetCode BackupRestoreClient_JNI::listAllBackups(NAArray<HbaseStr>* *backupList,
                                                    NABoolean shortFormat,
                                                    NABoolean reverseOrder,
                                                    NAHeap *heap)
{
  ENTRY_FUNCTION();
  if (initJNIEnv() != JOI_OK)
  {
     EXIT_FUNCTION(BRC_ERROR_INIT_PARAM);
     return BRC_ERROR_INIT_PARAM;
  }
  
  BRC_RetCode retValue = BRC_OK; 
  tsRecentJMFromJNI = JavaMethods_[JM_LIST_ALL_BACKUPS].jm_full_name;
  jarray j_backupList = (jarray)jenv_->CallObjectMethod(javaObj_, 
          JavaMethods_[JM_LIST_ALL_BACKUPS].methodID,
          (jboolean)shortFormat, (jboolean)reverseOrder);

  CHECK_EXCEPTION_AND_RETURN_ERROR("call list all backups failed", BRC_ERROR_LIST_ALL_BACKUPS_EXCEPTION);
  if (j_backupList != NULL) {
      jint retcode = convertByteArrayObjectArrayToNAArray(heap, j_backupList, backupList);
  }

errorLabel:
  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}


char* BackupRestoreClient_JNI::getErrorText(BRC_RetCode errEnum)
{
  if (errEnum < (BRC_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)errEnum);
  else
    return (char*)brcErrorEnumStr[errEnum-BRC_FIRST-1];
}

NAString BackupRestoreClient_JNI::getBackupType(const char* backuptag)
{
  ENTRY_FUNCTION();

  NAString retValue = "";
  jstring jresult = NULL;
  const char* char_result = NULL;
  jstring js_backuptag = jenv_->NewStringUTF(backuptag);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, NAString(""));

  tsRecentJMFromJNI = JavaMethods_[JM_GET_BACKUP_TYPE].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod( javaObj_, 
       JavaMethods_[JM_GET_BACKUP_TYPE].methodID, js_backuptag);
  
  CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, NAString(""));
  char_result = jenv_->GetStringUTFChars(jresult, 0);
  if (char_result != NULL)
      retValue = char_result;

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_backuptag);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(BRC_ERROR_GET_BACKUP_TYPE_EXCEPTION);
  return retValue;
}

NAString BackupRestoreClient_JNI::getExtendedAttributes(const char* backuptag)
{
  ENTRY_FUNCTION();

  NAString retValue = "";
  jstring jresult = NULL;
  const char* char_result = NULL;
  jstring js_backuptag = jenv_->NewStringUTF(backuptag);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, NAString(""));

  tsRecentJMFromJNI = JavaMethods_[JM_GET_EXTENDED_ATTRIBUTES].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
       JavaMethods_[JM_GET_EXTENDED_ATTRIBUTES].methodID, js_backuptag);

  CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, NAString(""));
  if (jresult != NULL)
  {
      char_result = jenv_->GetStringUTFChars(jresult, 0);
      if (char_result != NULL)
          retValue = char_result;
  }

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_backuptag);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(BRC_ERROR_GET_EXTENDED_ATTRIBUTES_EXCEPTION);
  return retValue;
}

NAString BackupRestoreClient_JNI::getBackupStatus(const char* backuptag)
{
  ENTRY_FUNCTION();

  NAString retValue = "";
  jstring jresult = NULL;
  const char* char_result = NULL;
  jstring js_backuptag = jenv_->NewStringUTF(backuptag);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, NAString(""));

  tsRecentJMFromJNI = JavaMethods_[JM_GET_BACKUP_STATUS].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
       JavaMethods_[JM_GET_BACKUP_STATUS].methodID, js_backuptag);

  CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, NAString(""));
  if (jresult != NULL)
  {
      char_result = jenv_->GetStringUTFChars(jresult, 0);
      if (char_result != NULL)
          retValue = char_result;
  }

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_backuptag);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(BRC_ERROR_GET_BACKUP_STATUS_EXCEPTION);
  return retValue;
}

NAString BackupRestoreClient_JNI::getPriorBackupTag(const char* restoreToTimestamp)
{
  ENTRY_FUNCTION();

  NAString retValue = "";
  jstring jresult = NULL;
  const char* char_result = NULL;
  jstring js_rtt = jenv_->NewStringUTF(restoreToTimestamp);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_rtt, restoreToTimestamp, (BRC_RetCode)JOI_ERROR_NEWOBJ, NAString(""));

  tsRecentJMFromJNI = JavaMethods_[JM_GET_PRIOR_BACKUP_TAG].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
       JavaMethods_[JM_GET_PRIOR_BACKUP_TAG].methodID, js_rtt);

  CHECK_EXCEPTION_AND_RETURN_ERROR(restoreToTimestamp, NAString(""));
  if (jresult != NULL) 
  {
      char_result = jenv_->GetStringUTFChars(jresult, 0);
      if (char_result != NULL)
          retValue = char_result;
  }

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_rtt);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(BRC_ERROR_GET_BACKUP_STATUS_EXCEPTION);
  return retValue;
}

NAString BackupRestoreClient_JNI::getRestoreToTsBackupTag(const char* restoreToTimestamp)
{
  ENTRY_FUNCTION();

  NAString retValue = "";
  jstring jresult = NULL;
  const char* char_result = NULL;
  jstring js_rtt = jenv_->NewStringUTF(restoreToTimestamp);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_rtt, restoreToTimestamp, (BRC_RetCode)JOI_ERROR_NEWOBJ, NAString(""));

  tsRecentJMFromJNI = JavaMethods_[JM_GET_RESTORE_TO_TS_BACKUP_TAG].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
       JavaMethods_[JM_GET_RESTORE_TO_TS_BACKUP_TAG].methodID, js_rtt);

  CHECK_EXCEPTION_AND_RETURN_ERROR(restoreToTimestamp, NAString(""));
  if(jresult != NULL)
  {
      char_result = jenv_->GetStringUTFChars(jresult, 0);
      if(char_result != NULL)
          retValue = char_result;     
  }

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_rtt);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(BRC_ERROR_RESTORE_SNAPSHOT_EXCEPTION);
  return retValue;
}

NAString BackupRestoreClient_JNI::lockHolder()
{
  ENTRY_FUNCTION();

  NAString retValue = "";
  jstring jresult = NULL;
  const char* char_result = NULL;
  tsRecentJMFromJNI = JavaMethods_[JM_LOCK_HOLDER].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
          JavaMethods_[JM_LOCK_HOLDER].methodID);

  CHECK_EXCEPTION_AND_RETURN_ERROR("call lock holder failed", NAString("?"));
  if (jresult != NULL)
  {
      char_result = jenv_->GetStringUTFChars(jresult, 0);
      if (char_result != NULL)
          retValue = char_result;
  }

errorLabel:
  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(BRC_ERROR_LOCK_HOLDER_EXCEPTION);
  return retValue;
}

BRC_RetCode BackupRestoreClient_JNI::operationLock(const char* backuptag)
{
  ENTRY_FUNCTION();

  BRC_RetCode retValue = BRC_OK;
  jstring jresult = NULL;
  const char* char_result = NULL;
  jstring js_backuptag = jenv_->NewStringUTF(backuptag);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_OPERATION_LOCK_EXCEPTION);
  
  tsRecentJMFromJNI = JavaMethods_[JM_OPERATION_LOCK].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
       JavaMethods_[JM_OPERATION_LOCK].methodID, js_backuptag);
  
  CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, BRC_ERROR_OPERATION_LOCK_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_backuptag);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

BRC_RetCode BackupRestoreClient_JNI::operationUnlock(const char* backuptag, NABoolean recoverMeta, 
                                                     NABoolean cleanupLock)
{
  ENTRY_FUNCTION();

  BRC_RetCode retValue = BRC_OK;
  jstring jresult = NULL;
  jstring js_backuptag = jenv_->NewStringUTF(backuptag);
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, BRC_ERROR_OPERATION_UNLOCK_EXCEPTION);

  tsRecentJMFromJNI = JavaMethods_[JM_OPERATION_UNLOCK].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
       JavaMethods_[JM_OPERATION_UNLOCK].methodID, js_backuptag, 
       (jboolean)recoverMeta, (jboolean)cleanupLock);

  CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, BRC_ERROR_OPERATION_UNLOCK_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_backuptag);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

//////////////////////////////////////////////////////////////////////////////
// get dependent incremental tags for current regular backup tag
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr>* BackupRestoreClient_JNI::getLinkedBackupTags(const char* backuptag,
                                                                NAHeap *heap)
{
    FUNCTION_DEBUG("%s(%s) called.", __FUNCTION__, (backuptag ? backuptag : " "));
    if (initJNIEnv() != JOI_OK)
        return NULL;

    NAArray<HbaseStr>* retValue = NULL;
    jint retcode = 0;
    jarray j_backupList = NULL;
    jstring js_backuptag = jenv_->NewStringUTF(backuptag);
    CHECK_RESULT_AND_SET_RETURN_ERROR(js_backuptag, backuptag, (BRC_RetCode)JOI_ERROR_NEWOBJ, NULL);

    tsRecentJMFromJNI = JavaMethods_[JM_GET_LINKED_BACKUPS].jm_full_name;
    j_backupList = (jarray)jenv_->CallObjectMethod( javaObj_,
            JavaMethods_[JM_GET_LINKED_BACKUPS].methodID, js_backuptag);

    CHECK_EXCEPTION_AND_RETURN_ERROR(backuptag, NULL);
    if (j_backupList != NULL)
    {
        retcode = convertByteArrayObjectArrayToNAArray(heap, j_backupList, &retValue);
        if(retcode == 0)
            retValue = NULL;
    }

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_backuptag);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

BackupRestoreClient_JNI::~BackupRestoreClient_JNI()
{
	
}

 //////////////////////////////////////////////////////////////////////////////
 //
 //////////////////////////////////////////////////////////////////////////////

 HBC_RetCode  HBaseClient_JNI::incrCounter( const char * tabName, const char * rowId, const char * famName, const char * qualName , Int64 incr, Int64 & count)
 {
   ENTRY_FUNCTION();

   if (initJNIEnv() != JOI_OK)
   {
      EXIT_FUNCTION(HBC_ERROR_INIT_PARAM);
      return HBC_ERROR_INIT_PARAM;
   }

   HBC_RetCode retValue = HBC_OK;
   jlong jcount = 0;
   jlong j_incr = incr;
   jstring js_rowId = NULL;
   jstring js_famName = NULL;
   jstring js_qualName = NULL;
   jstring js_tabName = jenv_->NewStringUTF(tabName);
   CHECK_RESULT_AND_SET_RETURN_ERROR(js_tabName, tabName, (HBC_RetCode)JOI_ERROR_NEWOBJ, HBC_ERROR_INCR_COUNTER_PARAM);

   js_rowId = jenv_->NewStringUTF(rowId);
   CHECK_RESULT_AND_SET_RETURN_ERROR(js_rowId, rowId, (HBC_RetCode)JOI_ERROR_NEWOBJ, HBC_ERROR_INCR_COUNTER_PARAM);

   js_famName = jenv_->NewStringUTF(famName);
   CHECK_RESULT_AND_SET_RETURN_ERROR(js_famName, famName, (HBC_RetCode)JOI_ERROR_NEWOBJ, HBC_ERROR_INCR_COUNTER_PARAM);

   js_qualName = jenv_->NewStringUTF(qualName);
   CHECK_RESULT_AND_SET_RETURN_ERROR(js_qualName, qualName, (HBC_RetCode)JOI_ERROR_NEWOBJ, HBC_ERROR_INCR_COUNTER_PARAM);

   tsRecentJMFromJNI = JavaMethods_[JM_INCR_COUNTER].jm_full_name;
   jcount = jenv_->CallLongMethod(javaObj_, JavaMethods_[JM_INCR_COUNTER].methodID, js_tabName, js_rowId, js_famName, js_qualName, j_incr);

   count = jcount;
   CHECK_EXCEPTION_AND_RETURN_ERROR(tabName, HBC_ERROR_INCR_COUNTER_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_tabName);
  CHECK_AND_DELETE_NEWOBJ(js_rowId);
  CHECK_AND_DELETE_NEWOBJ(js_famName);
  CHECK_AND_DELETE_NEWOBJ(js_qualName);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
 }

 //////////////////////////////////////////////////////////////////////////////
 //
 //////////////////////////////////////////////////////////////////////////////
 HBC_RetCode  HBaseClient_JNI::createCounterTable( const char * tabName,  const char * famName)
 {
   ENTRY_FUNCTION();

   if (initJNIEnv() != JOI_OK)
   {
      EXIT_FUNCTION(HBC_ERROR_INIT_PARAM);
      return HBC_ERROR_INIT_PARAM;
   }

   HBC_RetCode retValue = HBC_OK; 
   jboolean jresult = false;
   jstring js_famName = NULL;
   jstring js_tabName = jenv_->NewStringUTF(tabName);
   CHECK_RESULT_AND_SET_RETURN_ERROR(js_tabName, tabName, (HBC_RetCode)JOI_ERROR_NEWOBJ, HBC_ERROR_CREATE_COUNTER_PARAM);

   js_famName = jenv_->NewStringUTF(famName);
   CHECK_RESULT_AND_SET_RETURN_ERROR(js_famName, famName, (HBC_RetCode)JOI_ERROR_NEWOBJ, HBC_ERROR_INCR_COUNTER_PARAM);

   tsRecentJMFromJNI = JavaMethods_[JM_CREATE_COUNTER_TABLE].jm_full_name;
   jresult = jenv_->CallLongMethod(javaObj_, JavaMethods_[JM_CREATE_COUNTER_TABLE].methodID, js_tabName, js_famName);

   CHECK_EXCEPTION_AND_RETURN_ERROR(tabName, HBC_ERROR_CREATE_COUNTER_EXCEPTION);
   CHECK_AND_REPORT_ERROR(!jresult, HBC_ERROR_CREATE_COUNTER_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(js_tabName);
  CHECK_AND_DELETE_NEWOBJ(js_famName);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
 }


HBulkLoadClient_JNI::~HBulkLoadClient_JNI()
{
  //QRLogger::log(CAT_JNI_TOP, LL_DEBUG, "HBulkLoadClient_JNI destructor called.");
}


////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::revoke(const Text& user, const Text& tblName, const TextVec& actions)
{
  FUNCTION_DEBUG("%s(%s, %s, %s) called.", 
                 __FUNCTION__, user.data(), tblName.data(), actions.data());
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  HBC_RetCode retValue = HBC_OK;
  int len = user.size();
  jboolean jresult = false;
  jbyteArray jba_tblName = NULL;
  jobjectArray j_actionCodes = NULL;
  jbyteArray jba_user = jenv_->NewByteArray(len);
  CHECK_RESULT_AND_SET_RETURN_ERROR(jba_user, "revoke", (HBC_RetCode)JOI_ERROR_NEWOBJ, HBC_ERROR_REVOKE_PARAM);
  jenv_->SetByteArrayRegion(jba_user, 0, len, (const jbyte*)user.data());

  len = tblName.size();
  jba_tblName = jenv_->NewByteArray(len);
  CHECK_RESULT_AND_SET_RETURN_ERROR(jba_tblName, "revoke", (HBC_RetCode)JOI_ERROR_NEWOBJ, HBC_ERROR_REVOKE_PARAM);
  jenv_->SetByteArrayRegion(jba_tblName, 0, len, (const jbyte*)tblName.data());

  if (!actions.empty())
  {
    QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  Adding %d actions.", actions.size());
    j_actionCodes = convertToStringObjectArray(actions);
    CHECK_RESULT_AND_RETURN_ERROR(j_actionCodes, tblName.data(), HBC_ERROR_REVOKE_PARAM);
  }
  tsRecentJMFromJNI = JavaMethods_[JM_REVOKE].jm_full_name;
  jresult = jenv_->CallBooleanMethod(javaObj_, 
       JavaMethods_[JM_REVOKE].methodID, jba_user, jba_tblName, j_actionCodes);

  CHECK_EXCEPTION_AND_RETURN_ERROR(tblName.data(), HBC_ERROR_REVOKE_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, HBC_ERROR_REVOKE_EXCEPTION);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(jba_user);
  CHECK_AND_DELETE_NEWOBJ(jba_tblName);
  CHECK_AND_DELETE_NEWOBJ(j_actionCodes);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}


////////////////////////////////////////////////////////////////////
void HBaseClient_JNI::logIt(const char* str)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, str);
}

HBC_RetCode HBaseClient_JNI::startGet(NAHeap *heap,
                                      const char* tableName,
                                      bool useTRex,
                                      NABoolean replSync,
                                      int lockMode,
                                      NABoolean skipReadConflict,
                                      ExHbaseAccessStats *hbs, 
                                      Int64 transID,
                                      Int64 savepointID,
                                      Int64 pSavepointId,
                                      Int32 isolationLevel,
                                      const HbaseStr& rowID, 
                                      const LIST(HbaseStr) & cols, 
                                      Int64 timestamp,
                                      int numReplications,
                                      HTableClient_JNI *htc,
                                      const char * hbaseAuths,
                                      const char * encryptionInfo,
                                      NABoolean waitOnSelectForUpdate,
                                      NABoolean firstReadBypassTm 
                                      )
{
  if (javaObj_ == NULL || (!isInitialized())) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GET_HTC_EXCEPTION));
    NADELETE(htc, HTableClient_JNI, heap);
    return HBC_ERROR_GET_HTC_EXCEPTION;
  }

  if (htc->init() != HTC_OK) {
     NADELETE(htc, HTableClient_JNI, heap);
     return HBC_ERROR_GET_HTC_EXCEPTION;
  }
  htc->setTableName(tableName);
  htc->setHbaseStats(hbs);
  htc->setFetchMode(HTableClient_JNI::GET_ROW);

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_STARTGET_EXCEPTION));
    NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_HTC_EXCEPTION;
  }

  Text encRowIdText;
  char * rowIdVal = rowID.val;
  int rowIdLen = rowID.len;
  if (encryptionInfo && 
      ComEncryption::isFlagSet(*(ComEncryption::EncryptionInfo*)encryptionInfo,
                               ComEncryption::ROWID_ENCRYPT)) {
    encryptRowId(encryptionInfo, heap, 1, rowIdVal, rowIdLen, encRowIdText);
    rowIdVal = (char *)encRowIdText.c_str();
    rowIdLen = encRowIdText.length();
  }

  jbyteArray jba_rowID = jenv_->NewByteArray(rowIdLen);
  if (jba_rowID == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_STARTGET_EXCEPTION));
     NADELETE(htc, HTableClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_GET_HTC_EXCEPTION;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowIdLen, (const jbyte*)rowIdVal);
  jobjectArray j_cols = NULL;
  if (!cols.isEmpty()) {
     j_cols = convertToByteArrayObjectArray(cols);
     if (j_cols == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::startGet()", tableName);
        NADELETE(htc, HTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return HBC_ERROR_GET_HTC_EXCEPTION;
     }  
     htc->setNumColsInScan(cols.entries());
  }
  else
     htc->setNumColsInScan(0);
  htc->setNumReqRows(1);
  jlong j_tid = transID;
  jlong j_sid = savepointID;
  jlong j_psid = pSavepointId;
  jint j_isolationLevel = isolationLevel;
  jlong j_ts = timestamp;
  jint j_lockMode = lockMode;
  
  jstring js_hbaseAuths = NULL;
  if (hbaseAuths)
    {
      js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
      if (js_hbaseAuths == NULL)
        {
          GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_STARTGET_PARAM));
          NADELETE(htc, HTableClient_JNI, heap);
          jenv_->PopLocalFrame(NULL);
          return HBC_ERROR_GET_HTC_EXCEPTION;
        }
    }
  jint j_replicaId = HBaseClient_JNI::getReplicaId(numReplications);
  char* queryContext = getClientInfoFromContext();
  jstring js_QueryContext = NULL;
  if (queryContext != NULL) {
    js_QueryContext = jenv_->NewStringUTF(queryContext);
  } else {
    js_QueryContext = jenv_->NewStringUTF("");
  }
  if (js_QueryContext == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_STARTGET_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_HTC_EXCEPTION;
  }
  if (hbs)
    hbs->getHbaseTimer().start();


  tsRecentJMFromJNI = JavaMethods_[JM_START_GET].jm_full_name;
  jint jresult = jenv_->CallIntMethod(javaObj_, 
				      JavaMethods_[JM_START_GET].methodID, 
				      (jlong)htc,
				      js_tblName,
				      (jboolean)useTRex, 
				      (jboolean) replSync,
                                      j_lockMode,                                      
                                      (jboolean)skipReadConflict,
				      j_tid,
                                      j_sid,
                                      j_psid,
                                      j_isolationLevel,
				      jba_rowID,
				      j_cols, j_ts,
                                      js_hbaseAuths, j_replicaId,
                                      (jboolean)waitOnSelectForUpdate,
                                      (jboolean)firstReadBypassTm,
                                      js_QueryContext);
  if (hbs) {
      hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
      hbs->incHbaseCalls();
  }

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::startGet()", tableName);
    
    HBC_RetCode ret_Lock = HBC_OK;
    if (gEnableRowLevelLock) {
      ret_Lock = getLockError();
    }

    jenv_->PopLocalFrame(NULL);
    releaseHTableClient(htc);
    if (ret_Lock != HBC_OK) {
      return ret_Lock;
    }
    return HBC_ERROR_GET_HTC_EXCEPTION;
  }

  if (jresult == 0) 
     htc->setNumRowsReturned(-1);
  else
     htc->setNumRowsReturned(1);
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::startGets(NAHeap *heap,
                                       const char* tableName,
                                       bool useTRex,
                                       NABoolean replSync,
                                       Int32 lockMode,                                       
                                       NABoolean skipReadConflict,
                                       NABoolean skipTransactionForBatchGet,
                                       ExHbaseAccessStats *hbs,
                                       Int64 transID,
                                       Int64 savepointID,
                                       Int64 pSavepointId,
                                       Int32 isolationLevel,
                                       const LIST(HbaseStr) *rowIDs, 
                                       short rowIDLen,
                                       const HbaseStr *rowIDsInDB,
                                       const LIST(HbaseStr) & cols,
                                       Int64 timestamp,
                                       int numReplications,
                                       HTableClient_JNI *htc,
                                       const char * hbaseAuths,
                                       const char * encryptionInfo)
{
  if (javaObj_ == NULL || (!isInitialized())) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GET_HTC_EXCEPTION));
    NADELETE(htc, HTableClient_JNI, heap);
    return HBC_ERROR_GET_HTC_EXCEPTION;
  }

  if (htc->init() != HTC_OK) {
     NADELETE(htc, HTableClient_JNI, heap);
     return HBC_ERROR_GET_HTC_EXCEPTION;
  }
  htc->setTableName(tableName);
  htc->setHbaseStats(hbs);
  htc->setFetchMode(HTableClient_JNI::BATCH_GET);

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_STARTGET_EXCEPTION));
    NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_HTC_EXCEPTION;
  }
  jobjectArray j_cols = NULL;
  if (!cols.isEmpty()) {
     j_cols = convertToByteArrayObjectArray(cols);
     if (j_cols == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::startGets()", tableName);
        NADELETE(htc, HTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return HBC_ERROR_GET_HTC_EXCEPTION;
     }  
     htc->setNumColsInScan(cols.entries());
  }
  else
     htc->setNumColsInScan(0);
  jobjectArray	j_rows = NULL;
  jobject       jRowIDs = NULL;

  if (rowIDs != NULL) {
     j_rows = convertToByteArrayObjectArray(*rowIDs);
     if (j_rows == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::startGets()", tableName);
        NADELETE(htc, HTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return HBC_ERROR_GET_HTC_EXCEPTION;
     }  
     htc->setNumReqRows(rowIDs->entries());
  } else {
     jRowIDs = jenv_->NewDirectByteBuffer(rowIDsInDB->val, rowIDsInDB->len);
     if (jRowIDs == NULL) {
        GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_STARTGET_EXCEPTION));
        NADELETE(htc, HTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return HBC_ERROR_GET_HTC_EXCEPTION;
     }
     // Need to swap the bytes 
     short numReqRows = *(short *)rowIDsInDB->val;
     htc->setNumReqRows(bswap_16(numReqRows));
  }

  jlong j_tid = transID;
  jlong j_sid = savepointID;
  jlong j_psid = pSavepointId;
  jint j_isolationLevel = isolationLevel;
  jlong j_ts = timestamp;
  jshort jRowIDLen = rowIDLen;
  jint j_lockMode = lockMode;

  jstring js_hbaseAuths = NULL;
  if (hbaseAuths)
    {
      js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
      if (js_hbaseAuths == NULL)
        {
          GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_STARTGETS_PARAM));
          NADELETE(htc, HTableClient_JNI, heap);
          jenv_->PopLocalFrame(NULL);
          return HBC_ERROR_GET_HTC_EXCEPTION;
        }
    }
  jint j_replicaId = HBaseClient_JNI::getReplicaId(numReplications);
  char* queryContext = getClientInfoFromContext();
  jstring js_QueryContext = NULL;
  if (queryContext != NULL) {
    js_QueryContext = jenv_->NewStringUTF(queryContext);
  } else {
    js_QueryContext = jenv_->NewStringUTF("");
  }
  if (js_QueryContext == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_STARTGETS_PARAM));
    if (htc != NULL)
      NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_HTC_EXCEPTION;
  }
  if (hbs)
    hbs->getHbaseTimer().start();

  jint jresult;
  if (rowIDs != NULL) {
     tsRecentJMFromJNI = JavaMethods_[JM_START_GETS].jm_full_name;
     jresult = jenv_->CallIntMethod(javaObj_, 
                                    JavaMethods_[JM_START_GETS].methodID, 
                                    (jlong)htc, js_tblName, (jboolean)useTRex, 
                                    (jboolean) replSync, j_lockMode, (jboolean)skipReadConflict,
                                    (jboolean)skipTransactionForBatchGet,
                                    j_tid, j_sid, j_psid, j_isolationLevel, j_rows,
                                    j_cols, j_ts, js_hbaseAuths, j_replicaId, js_QueryContext);
  } else {
    tsRecentJMFromJNI = JavaMethods_[JM_START_DIRECT_GETS].jm_full_name;
    jresult = jenv_->CallIntMethod(javaObj_, 
                                   JavaMethods_[JM_START_DIRECT_GETS].methodID, 
                                   (jlong)htc, js_tblName, 
                                   (jboolean)useTRex, (jboolean) replSync, j_lockMode, 
                                   (jboolean)skipReadConflict,
                                    (jboolean)skipTransactionForBatchGet,
                                   j_tid, j_sid, j_psid, j_isolationLevel, jRowIDLen, jRowIDs,
                                   j_cols, js_hbaseAuths, j_replicaId, js_QueryContext);
  }
  if (hbs) {
      hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
      hbs->incHbaseCalls();
  }

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::startGets()", tableName);
      
    HBC_RetCode ret_Lock = HBC_OK;
    if (gEnableRowLevelLock) {
      ret_Lock = getLockError();
    }

    jenv_->PopLocalFrame(NULL);
    releaseHTableClient(htc);
    if (ret_Lock != HBC_OK){
      return ret_Lock;
    }
    return HBC_ERROR_GET_HTC_EXCEPTION;
  }

  if (jresult == 0) 
     htc->setNumRowsReturned(-1);
  else
     htc->setNumRowsReturned(jresult);
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::getRegionsNodeName(const char* tblName,
                                                Int32 partns,
                                                ARRAY(const char *)& nodeNames)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::getRegionsNodeName(%s) called.", tblName);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tblName);
  if (js_tblName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GET_HBTI_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_HBTI_PARAM;
  }

  jobjectArray jNodeNames = jenv_->NewObjectArray(partns,
                                                  jenv_->FindClass("java/lang/String"),
                                                  jenv_->NewStringUTF(""));
                              
  tsRecentJMFromJNI = JavaMethods_[JM_GET_REGN_NODES].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_GET_REGN_NODES].methodID,
                                              js_tblName, jNodeNames);
  NAHeap *heap = getHeap();
  if (jresult) {
    jstring strObj = NULL;
    char* node = NULL;
    for(int i=0; i < partns; i++) {
      strObj = (jstring) jenv_->GetObjectArrayElement(jNodeNames, i);
      node = (char*)jenv_->GetStringUTFChars(strObj, NULL);
      char* copyNode = new (heap) char[strlen(node)+1];
      strcpy(copyNode, node);
      nodeNames.insertAt(i, copyNode);
    }
    //jenv_->ReleaseObjectArrayElements(jNodeNames, strObj, JNI_ABORT);
    jenv_->ReleaseStringUTFChars(strObj, node);
  }

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::getRegionsNodeName()", tblName);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_HBTI_EXCEPTION;
  }

  if (jresult == false)
  {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::getRegionsNodeName()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_HBTI_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;  // Table exists.

}
//////////////////////////////////////////////////////////////////////////////
// Get Hbase Table information. Currently the following info is requested:
// 1. index levels : This info is obtained from trailer block of Hfiles of randomly chosen region
// 2. block size : This info is obtained for HColumnDescriptor
////////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::getHbaseTableInfo(const char* tblName,
                                              Int32& indexLevels,
                                              Int32& blockSize)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::getHbaseTableInfo(%s) called.", tblName);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tblName);
  if (js_tblName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GET_HBTI_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_HBTI_PARAM;
  }

  jintArray jHtabInfo = jenv_->NewIntArray(2);
  tsRecentJMFromJNI = JavaMethods_[JM_GET_HBTI].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_GET_HBTI].methodID,
                                              js_tblName, jHtabInfo);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::getHbaseTableInfo()", tblName);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_HBTI_EXCEPTION;
  }
  jboolean isCopy;
  jint* arrayElems = jenv_->GetIntArrayElements(jHtabInfo, &isCopy);
  indexLevels = arrayElems[0];
  blockSize = arrayElems[1];
  if (isCopy == JNI_TRUE)
     jenv_->ReleaseIntArrayElements(jHtabInfo, arrayElems, JNI_ABORT);
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;  // Table exists.
}

NABoolean HBaseClient_JNI::isExistTrigger(BeforeAndAfterTriggers *triggers)
{
  if (triggers != NULL &&
      (triggers->getBeforeRowTriggers() != NULL ||
       triggers->getAfterRowTriggers() != NULL))
    return TRUE;
  else
    return FALSE;
}

NABoolean HBaseClient_JNI::matchTriggerOperation(Trigger *trigger,
						 ComOperation type)
{
  ComOperation op = trigger->getOperation();
  if (type == COM_INSERT)
    {
      if (op == COM_INSERT
	  || op == COM_INSERT_UPDATE
	  || op == COM_INSERT_DELETE
	  || op == COM_INSERT_UPDATE_DELETE)
	return TRUE;
    }
  else if (type == COM_UPDATE)
    {
      if (op == COM_UPDATE
	  || op == COM_INSERT_UPDATE
	  || op == COM_INSERT_UPDATE_DELETE
	  || op == COM_UPDATE_DELETE)
	return TRUE;
    }
  else if (type == COM_DELETE)
    {
      if (op == COM_DELETE
	  || op == COM_INSERT_DELETE
	  || op == COM_INSERT_UPDATE_DELETE
	  || op == COM_UPDATE_DELETE)
	return TRUE;
    }
  return FALSE;
}

void HBaseClient_JNI::getTriggerInfo(NAString &triggerInfo,
				     ComOperation type,
				     NABoolean isBefore,
				     BeforeAndAfterTriggers *triggers,
				     NABoolean isStatement)
{
  TriggerList *trList = NULL;
  if (isStatement)
    {
      if (isBefore)
	{
	  trList = triggers->getBeforeStatementTriggers();
	}
      else
	{
	  trList = triggers->getAfterStatementTriggers();
	}
    }
  else
    {
      if (isBefore)
	{
	  trList = triggers->getBeforeRowTriggers();
	}
      else
	{
	  trList = triggers->getAfterRowTriggers();
	}
    }

  if (trList == NULL)
    return;

  Trigger *trigger = NULL;
  NABoolean addTableId = FALSE;
  for (int i = 0; i < trList->entries(); i++)
    {
      trigger = trList->at(i);
      if (matchTriggerOperation(trigger, type))
	{
	  if (!addTableId)
	    {
	      triggerInfo.format("%ld,", trigger->getTableId());
	      addTableId = TRUE;
	    }
	  else
	    triggerInfo.append(",");
	  triggerInfo.append(trigger->getTriggerName());
	}
    }
}

/*
 * if trigger columns size max than define size, return FALSE
 */
HBC_RetCode HBaseClient_JNI::execTriggers(NAHeap *heap,
					  const char *tableName,
					  ComOperation type,
					  NABoolean isBefore,
					  BeforeAndAfterTriggers *triggers,
					  HbaseStr rowID,
					  HbaseStr row,
					  unsigned char* base64rowVal,
					  int base64ValLen,
					  unsigned char* base64rowIDVal,
					  int base64RowLen,
					  short rowIDLen,
					  const char * curExecSql,
					  NABoolean isStatement)
{
  if (triggers == NULL ||
      (type != COM_INSERT && type != COM_UPDATE && type != COM_DELETE))
    {
      return HBC_OK;
    }

  if (strlen(tableName) + 1 > TRIGGER_NAME_LEN)
    {
      return HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION;
    }
  
  NAString triggerInfo;
  getTriggerInfo(triggerInfo,
		 type,
		 isBefore,
		 triggers,
		 isStatement);
  if (triggerInfo.length() == 0)
    {
      return HBC_OK;
    }

  char *oldSKVBuffer = NULL;
  char *oldBufSize = NULL;
  char *oldRowIDs = NULL;
  char *oldRowIDsLen = NULL;
  Lng32 newRowIDLen = 0;
  char *newRowIDs = NULL;
  Lng32 newRowIDsLen = 0;
  char *newRows = NULL;
  Lng32 newRowsLen = 0;

  // Setup NEW and OLD rows data for row triggers
  if (!isStatement)
    {
      // if do upsert operation, maybe the type is update
      if (type == COM_UPDATE && (skvBuffer_ == NULL || srowIDs_ == NULL))
        {
            type = COM_INSERT;
        }
      if (type == COM_INSERT || type == COM_UPDATE)
        {
          if (base64RowLen + 1 > TRIGGER_ROW_LEN
              || base64ValLen + 1 > TRIGGER_ROW_LEN)
            {
              return HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION;
            }
          
          newRowIDLen = rowIDLen;
          newRowIDs = (char*)base64rowIDVal;
          newRowIDsLen = rowID.len;
          newRows = (char*)base64rowVal;
          newRowsLen = row.len;
        }
      if (type == COM_DELETE || type == COM_UPDATE)
        {
          if (strlen(skvBuffer_) + 1 > TRIGGER_ROW_LEN
              || strlen(sBufSize_) + 1 > TRIGGER_ID_LEN
              || strlen(srowIDs_) + 1 > TRIGGER_ROW_LEN
              || strlen(srowIDsLen_) + 1 > TRIGGER_ID_LEN)
            {
              return HBC_ERROR_TRIGGER_PARAMETER_EXCEPTION;
            }
          
          oldSKVBuffer = skvBuffer_;
          oldBufSize = sBufSize_;
          oldRowIDs = srowIDs_;
          oldRowIDsLen = srowIDsLen_;
        }
    }

  Int32 cliRC = GetCliGlobals()->currContext()->prepareCallTrigger();
  if (cliRC < 0) {
    return HBC_ERROR_TRIGGER_EXECUTE_EXCEPTION;
  }

  cliRC = GetCliGlobals()->currContext()->executeCallTrigger((char*)triggerInfo.data(),
							     isBefore,
							     type,
							     oldSKVBuffer,
							     oldBufSize,
							     oldRowIDs,
							     oldRowIDsLen,
							     newRowIDLen,
							     newRowIDs,
							     newRowIDsLen,
							     newRows,
							     newRowsLen,
							     (char*)curExecSql);
  if (cliRC < 0)
    {
      return HBC_ERROR_TRIGGER_EXECUTE_EXCEPTION;
    }
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::insertRow(NAHeap *heap,
				       const char *tableName,
				       ExHbaseAccessStats *hbs,
				       Int64 transID,
                                       Int64 savepointID,
                                       Int64 pSavepointId,
				       HbaseStr rowID,
				       HbaseStr row,
				       Int64 timestamp,
				       bool checkAndPut,
                                       UInt32 flags,
                                       const char * encryptionInfo,
				       const char * triggers,
				       const char * curExecSql,
                                       short colIndexToCheck,
				       HTableClient_JNI **outHtc,
				       ExDDLValidator * ddlValidator)
{
  
  HTableClient_JNI *htc = NULL;

  if (ddlValidator && !ddlValidator->isDDLValid())
    return HBC_ERROR_INSERTROW_INVALID_DDL;

  if (htc->asyncOper(flags)) {
     htc = new (heap) HTableClient_JNI(heap, (jobject)-1);
     if (htc->init() != HTC_OK) {
         NADELETE(htc, HTableClient_JNI, heap);
         return HBC_ERROR_INSERTROW_EXCEPTION;
     }
     htc->setTableName(tableName);
     htc->setHbaseStats(hbs);
  }

  if (initJNIEnv() != JOI_OK) {
    if (htc != NULL) 
       NADELETE(htc, HTableClient_JNI, heap);
     return HBC_ERROR_INIT_PARAM;
  }

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INSERTROW_PARAM;
  }

  char* queryContext = getClientInfoFromContext();
  jstring js_QueryContext = NULL;
  if (queryContext != NULL) {
      js_QueryContext = jenv_->NewStringUTF(queryContext);
  } else {
      js_QueryContext = jenv_->NewStringUTF("");
  }
  if (js_QueryContext == NULL) {
      GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROW_PARAM));
      if (htc != NULL)
          NADELETE(htc, HTableClient_JNI, heap);
      jenv_->PopLocalFrame(NULL);
      return HBC_ERROR_INSERTROW_PARAM;
    }

  //call trigger
  int base64ValLen = 0;
  int base64RowLen = 0;
  unsigned char* base64rowVal = NULL;
  unsigned char* base64rowIDVal = NULL;

  if (isExistTrigger((BeforeAndAfterTriggers *)triggers))
    {
      if (base64rowVal == NULL)
	{
	  base64rowVal = new (heap) unsigned char[3 * row.len + 1];
	  base64ValLen = EVP_EncodeBlock(base64rowVal, (unsigned char *)row.val, row.len);
	}
      if (base64rowIDVal == NULL)
	{
	  base64rowIDVal = new (heap) unsigned char[3 * rowID.len + 1];
	  base64RowLen = EVP_EncodeBlock(base64rowIDVal, (unsigned char *)rowID.val, rowID.len);
	}
    }

  HBC_RetCode reCode = execTriggers(heap,
				  tableName,
				  gettrigger_operation() == COM_UPDATE ? COM_UPDATE : COM_INSERT,
                                  TRUE,
				  (BeforeAndAfterTriggers *)triggers,
				  rowID,
				  row,
				  base64rowVal,
				  base64ValLen,
				  base64rowIDVal,
				  base64RowLen,
				  0,
				  curExecSql);
  if (reCode != HBC_OK)
    {
      NADELETE(htc, HTableClient_JNI, heap);
      if (base64rowVal != NULL)
	{
	  NADELETEBASIC(base64rowVal, heap);
	}
      if (base64rowIDVal != NULL)
	{
	  NADELETEBASIC(base64rowIDVal, heap);
	}
      return reCode;
    }
  //end call trigger
  
  Text encRowIdText;
  char * rowIdVal = rowID.val;
  int rowIdLen = rowID.len;

  jobject jRow = NULL;
  unsigned char encDataBuffer[ENC_STATIC_BUFFER_LEN];
  unsigned char * encRowData = NULL;
  Int32 encRowLen = 0;
  if (encryptionInfo) {
    encRowData = encDataBuffer;
    if (ComEncryption::isFlagSet(*(ComEncryption::EncryptionInfo*)encryptionInfo,
                                 ComEncryption::ROWID_ENCRYPT)) {
      encryptRowId(encryptionInfo, heap, 1, rowIdVal, rowIdLen, encRowIdText);
      rowIdVal = (char *)encRowIdText.c_str();
      rowIdLen = encRowIdText.length();
    }

    encRowData = encDataBuffer;
    short rc =
      encryptRowsData(encryptionInfo, heap, 1,
                      row.val, row.len, encRowData, encRowLen);
    if (rc) {
      if (encRowData && (encRowData != encDataBuffer))
        NADELETEBASIC(encRowData, heap);
      GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROW_PARAM));
      if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
      if (base64rowVal != NULL)
	{
	  NADELETEBASIC(base64rowVal, heap);
	}
      if (base64rowIDVal != NULL)
	{
	  NADELETEBASIC(base64rowIDVal, heap);
	}
      jenv_->PopLocalFrame(NULL);
      return HBC_ERROR_INSERTROW_PARAM;
    }

    if (encRowData) {
      jRow = jenv_->NewDirectByteBuffer(encRowData, encRowLen);
    }
  }
  else
    jRow = jenv_->NewDirectByteBuffer(row.val, row.len);

  if (jRow == NULL) {
    if (encRowData && (encRowData != encDataBuffer))
      NADELETEBASIC(encRowData, heap);

    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (base64rowVal != NULL)
      {
	NADELETEBASIC(base64rowVal, heap);
      }
    if (base64rowIDVal != NULL)
      {
	NADELETEBASIC(base64rowIDVal, heap);
      }
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INSERTROW_PARAM;
  }

  jbyteArray jba_rowID = jenv_->NewByteArray(rowIdLen);
  if (jba_rowID == NULL) {
    if (encRowData && (encRowData != encDataBuffer))
      NADELETEBASIC(encRowData, heap);
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (base64rowVal != NULL)
      {
	NADELETEBASIC(base64rowVal, heap);
      }
    if (base64rowIDVal != NULL)
      {
	NADELETEBASIC(base64rowIDVal, heap);
      }
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INSERTROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowIdLen, (const jbyte*)rowIdVal);

  jlong j_htc = (long)htc;
  jlong j_tid = transID;  
  jlong j_sid = savepointID;
  jlong j_psid = pSavepointId;
  jlong j_ts = timestamp;
  jboolean j_checkAndPut = checkAndPut;
  jshort j_colIndexToCheck = colIndexToCheck;
  jboolean j_noConfCheckForIdx = noConflictCheckForIndex();
  jint j_nodeId = GetCliGlobals()->myNodeNumber();

  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_HBC_DIRECT_INSERT_ROW].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_,
					      JavaMethods_[JM_HBC_DIRECT_INSERT_ROW].methodID,
					      j_htc,
					      js_tblName,
					      j_tid,
                                              j_sid,
                                              j_psid,
					      jba_rowID,
					      jRow,
					      j_ts,
					      j_checkAndPut,
                                              j_colIndexToCheck,
                                              (jint)flags, j_nodeId, js_QueryContext);
  if (hbs) {
      hbs->incHbaseCalls();
      if (! htc->asyncOper(flags))
         hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }

  if (encRowData && (encRowData != encDataBuffer))
    NADELETEBASIC(encRowData, heap);
 
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::insertRow()", tableName);

    HBC_RetCode ret_Lock = HBC_OK;
    if (gEnableRowLevelLock) {
      ret_Lock = getLockError();
    }

    jenv_->PopLocalFrame(NULL);
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (base64rowVal != NULL)
      {
	NADELETEBASIC(base64rowVal, heap);
      }
    if (base64rowIDVal != NULL)
      {
	NADELETEBASIC(base64rowIDVal, heap);
      }
    if (ret_Lock != HBC_OK){
      return ret_Lock;
    }
    return HBC_ERROR_INSERTROW_EXCEPTION;
  }
  if (hbs)
    hbs->incBytesRead(rowID.len + row.len);
  if (jresult == false) {
     jenv_->PopLocalFrame(NULL);
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (base64rowVal != NULL)
      {
	NADELETEBASIC(base64rowVal, heap);
      }
    if (base64rowIDVal != NULL)
      {
	NADELETEBASIC(base64rowIDVal, heap);
      }
     return HBC_ERROR_INSERTROW_DUP_ROWID;
  }
  *outHtc = htc;
  jenv_->PopLocalFrame(NULL);

  //call trigger
  reCode = execTriggers(heap,
			tableName,
			gettrigger_operation() == COM_UPDATE ? COM_UPDATE : COM_INSERT,
			FALSE,
			(BeforeAndAfterTriggers *)triggers,
			rowID,
			row,
			base64rowVal,
			base64ValLen,
			base64rowIDVal,
			base64RowLen,
			0,
			curExecSql);
  if (reCode != HBC_OK)
    {
      NADELETE(htc, HTableClient_JNI, heap);
      if (base64rowVal != NULL)
	{
	  NADELETEBASIC(base64rowVal, heap);
	}
      if (base64rowIDVal != NULL)
	{
	  NADELETEBASIC(base64rowIDVal, heap);
	}
      return reCode;
    }
  //end call trigger
  
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::insertRows(NAHeap *heap, const char *tableName,
					ExHbaseAccessStats *hbs,
					Int64 transID,
                                        Int64 savepointID,
                                        Int64 pSavepointId,
					short rowIDLen,
					HbaseStr rowIDs,
					HbaseStr rows,
					Int64 timestamp,
                                        Int32 flags,
                                        const char * encryptionInfo,
					const char * triggers,
					const char * curExecSql,
					HTableClient_JNI **outHtc,
					ExDDLValidator * ddlValidator)
{
  
  HTableClient_JNI *htc = NULL;

  if (ddlValidator && !ddlValidator->isDDLValid())
    return HBC_ERROR_INSERTROWS_INVALID_DDL;

  if (htc->asyncOper(flags)) {
     htc = new (heap) HTableClient_JNI(heap, (jobject)-1);
     if (htc->init() != HTC_OK) {
         NADELETE(htc, HTableClient_JNI, heap);
         return HBC_ERROR_INSERTROWS_EXCEPTION;
     }
     htc->setTableName(tableName);
     htc->setHbaseStats(hbs);
  }
  if (initJNIEnv() != JOI_OK) {
     if (htc != NULL) 
       NADELETE(htc, HTableClient_JNI, heap);
     return HBC_ERROR_INIT_PARAM;
  }

  char* queryContext = getClientInfoFromContext();
  jstring js_QueryContext = NULL;
  if (queryContext != NULL) {
   js_QueryContext = jenv_->NewStringUTF(queryContext);
  } else {
    js_QueryContext = jenv_->NewStringUTF("");
  }
  if (js_QueryContext == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROWS_EXCEPTION));
    if (htc != NULL)
      NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INSERTROWS_EXCEPTION;
  }

  //call procedure
  int base64ValLen = 0;
  int base64RowLen = 0;
  unsigned char *base64rowVal = NULL;
  unsigned char *base64rowIDVal = NULL;

  if (isExistTrigger((BeforeAndAfterTriggers *)triggers))
    {
      if (base64rowVal == NULL)
	{
	  base64rowVal = new (heap) unsigned char[3 * rows.len + 1];
	  base64ValLen = EVP_EncodeBlock(base64rowVal, (unsigned char *)rows.val, rows.len);
	}
      if (base64rowIDVal == NULL)
	{
	  base64rowIDVal = new (heap) unsigned char[3 * rowIDs.len + 1];
	  base64RowLen = EVP_EncodeBlock(base64rowIDVal, (unsigned char *)rowIDs.val, rowIDs.len);
	}
    }

  HBC_RetCode reCode = execTriggers(heap,
				  tableName,
				  gettrigger_operation() == COM_UPDATE ? COM_UPDATE : COM_INSERT,
				  TRUE,
				  (BeforeAndAfterTriggers *)triggers,
				  rowIDs,
				  rows,
				  base64rowVal,
				  base64ValLen,
				  base64rowIDVal,
				  base64RowLen,
				  rowIDLen,
				  curExecSql);
  if (reCode != HBC_OK)
    {
      NADELETE(htc, HTableClient_JNI, heap);
      if (base64rowVal != NULL)
        {
	  NADELETEBASIC(base64rowVal, heap);
	}
      if (base64rowIDVal != NULL)
	{
	  NADELETEBASIC(base64rowIDVal, heap);
	}
      return reCode;
    }
  
  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROWS_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (base64rowVal != NULL)
      {
	NADELETEBASIC(base64rowVal, heap);
      }
    if (base64rowIDVal != NULL)
      {
	NADELETEBASIC(base64rowIDVal, heap);
      }
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INSERTROWS_PARAM;
  }
  jobject jRowIDs = jenv_->NewDirectByteBuffer(rowIDs.val, rowIDs.len);
  if (jRowIDs == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROWS_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (base64rowVal != NULL)
      {
	NADELETEBASIC(base64rowVal, heap);
      }
    if (base64rowIDVal != NULL)
      {
	NADELETEBASIC(base64rowIDVal, heap);
      }
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INSERTROWS_PARAM;
  }

  jobject jRows = NULL;
  unsigned char encDataBuffer[ENC_STATIC_BUFFER_LEN];
  unsigned char * encRowsData = NULL;
  Int32 encRowsLen = 0;
  if (encryptionInfo) {
    encRowsData = encDataBuffer;
    short numRows = bswap_16(*(short*)rowIDs.val);
    short rc =
      encryptRowsData(encryptionInfo, heap, numRows,
                      rows.val, rows.len, encRowsData, encRowsLen);
    if (rc) {
      if (encRowsData && (encRowsData != encDataBuffer))
        NADELETEBASIC(encRowsData, heap);
      GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROW_PARAM));
      if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
      if (base64rowVal != NULL)
	{
	  NADELETEBASIC(base64rowVal, heap);
	}
      if (base64rowIDVal != NULL)
	{
	  NADELETEBASIC(base64rowIDVal, heap);
	}
      jenv_->PopLocalFrame(NULL);
      return HBC_ERROR_INSERTROW_PARAM;
    }
    
    if (encRowsData) {
      jRows = jenv_->NewDirectByteBuffer(encRowsData, encRowsLen);
    }
  }
  else
    jRows = jenv_->NewDirectByteBuffer(rows.val, rows.len);
  if (jRows == NULL) {
    if (encRowsData && (encRowsData != encDataBuffer))
      NADELETEBASIC(encRowsData, heap);
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROWS_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (base64rowVal != NULL)
      {
	NADELETEBASIC(base64rowVal, heap);
      }
    if (base64rowIDVal != NULL)
      {
	NADELETEBASIC(base64rowIDVal, heap);
      }
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INSERTROWS_PARAM;
  }
  jlong j_tid = transID; 
  jlong j_sid = savepointID;
  jlong j_psid = pSavepointId;
  jlong j_ts = timestamp;
  jlong j_htc = (long)htc;
  jshort j_rowIDLen = rowIDLen;

  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_HBC_DIRECT_INSERT_ROWS].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_,
					      JavaMethods_[JM_HBC_DIRECT_INSERT_ROWS].methodID, 
					      j_htc,
					      js_tblName,
					      j_tid,
                                              j_sid,
                                              j_psid,
					      j_rowIDLen,
					      jRowIDs,
					      jRows,
					      j_ts,
                                              (jint)flags, js_QueryContext);
  if (hbs) {
      hbs->incHbaseCalls();
      if (! htc->asyncOper(flags))
         hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }

  if (encRowsData && (encRowsData != encDataBuffer))
    NADELETEBASIC(encRowsData, heap);    

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::insertRows()", tableName);

    HBC_RetCode ret_Lock = HBC_OK;
    if (gEnableRowLevelLock) {
      ret_Lock = getLockError();
    }

    jenv_->PopLocalFrame(NULL);
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (base64rowVal != NULL)
      {
	NADELETEBASIC(base64rowVal, heap);
      }
    if (base64rowIDVal != NULL)
      {
	NADELETEBASIC(base64rowIDVal, heap);
      }
    if (ret_Lock != HBC_OK){
      return ret_Lock;
    }
    return HBC_ERROR_INSERTROWS_EXCEPTION;
  }
  if (jresult == false) {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::insertRows()", getLastError());
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (base64rowVal != NULL)
      {
	NADELETEBASIC(base64rowVal, heap);
      }
    if (base64rowIDVal != NULL)
      {
	NADELETEBASIC(base64rowIDVal, heap);
      }
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_INSERTROWS_EXCEPTION;
  }
  if (hbs)
    hbs->incBytesRead(rowIDs.len + rows.len);
  *outHtc = htc;
  jenv_->PopLocalFrame(NULL);

  reCode = execTriggers(heap,
			tableName,
			gettrigger_operation() == COM_UPDATE ? COM_UPDATE : COM_INSERT,
			FALSE,
			(BeforeAndAfterTriggers *)triggers,
			rowIDs,
			rows,
			base64rowVal,
			base64ValLen,
			base64rowIDVal,
			base64RowLen,
			rowIDLen,
			curExecSql);
  if (reCode != HBC_OK)
    {
      NADELETE(htc, HTableClient_JNI, heap);
      if (base64rowVal != NULL)
	{
	  NADELETEBASIC(base64rowVal, heap);
	}
      if (base64rowIDVal != NULL)
	{
	  NADELETEBASIC(base64rowIDVal, heap);
	}
      return reCode;
    }

  return HBC_OK;
}
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::updateVisibility(NAHeap *heap, const char *tableName,
                                        ExHbaseAccessStats *hbs, 
                                        bool useTRex, Int64 transID, 
                                        HbaseStr rowID,
                                        HbaseStr tagsRow, 
                                        HTableClient_JNI **outHtc)
{
  
  HTableClient_JNI *htc = NULL;
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_UPDATEVISIBILITY_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_UPDATEVISIBILITY_PARAM;
  }
  char* queryContext = getClientInfoFromContext();
  jstring js_QueryContext = NULL;
  if (queryContext != NULL) {
   js_QueryContext = jenv_->NewStringUTF(queryContext);
  } else {
    js_QueryContext = jenv_->NewStringUTF("");
  }
  if (js_QueryContext == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_UPDATEVISIBILITY_PARAM));
      if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
      jenv_->PopLocalFrame(NULL);
      return HBC_ERROR_UPDATEVISIBILITY_PARAM;
  }
  jbyteArray jba_rowID = jenv_->NewByteArray(rowID.len);
  if (jba_rowID == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_UPDATEVISIBILITY_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowID.len, (const jbyte*)rowID.val);

  jobject jTagsRow = jenv_->NewDirectByteBuffer(tagsRow.val, tagsRow.len);
  if (jTagsRow == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_UPDATEVISIBILITY_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_UPDATEVISIBILITY_PARAM;
  }
  jboolean j_useTRex = useTRex;
  jlong j_tid = transID;  
  jlong j_htc = (long)htc;
 
  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_HBC_DIRECT_UPDATE_TAGS].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_HBC_DIRECT_UPDATE_TAGS].methodID, 
               	j_htc, js_tblName, j_useTRex, j_tid, jba_rowID, jTagsRow, js_QueryContext);
  if (hbs) {
      hbs->incHbaseCalls();
      hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::updateVisibility()", tableName);
    jenv_->PopLocalFrame(NULL);
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    return HBC_ERROR_UPDATEVISIBILITY_EXCEPTION;
  }
  if (jresult == false) {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::updateVisibility()", getLastError());
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_UPDATEVISIBILITY_EXCEPTION;
  }
  //  if (hbs)
  //    hbs->incBytesRead(rowIDs.len + rows.len);
  *outHtc = htc;
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}
//
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::checkAndUpdateRow(NAHeap *heap, const char *tableName,
					       ExHbaseAccessStats *hbs,
					       Int64 transID,
                                               Int64 savepointID,
                                               Int64 pSavepointId,
					       HbaseStr rowID,
					       HbaseStr row,
					       HbaseStr columnToCheck,
					       HbaseStr columnValToCheck,
					       Int64 timestamp,
                                               Int32 flags,
                                               const char * encryptionInfo,
					       const char * triggers,
					       const char * curExecSql,
					       HTableClient_JNI **outHtc)
{
  
  HTableClient_JNI *htc = NULL;

  if (htc->asyncOper(flags)) {
     htc = new (heap) HTableClient_JNI(heap, (jobject)-1);
     if (htc->init() != HTC_OK) {
         NADELETE(htc, HTableClient_JNI, heap);
         return HBC_ERROR_CHECKANDUPDATEROW_EXCEPTION;
     }
     htc->setTableName(tableName);
     htc->setHbaseStats(hbs);
  }
  if (initJNIEnv() != JOI_OK) {
    if (htc != NULL) 
       NADELETE(htc, HTableClient_JNI, heap);
     return HBC_ERROR_INIT_PARAM;
  }

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDUPDATEROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CHECKANDUPDATEROW_PARAM;
  }

  char* queryContext = getClientInfoFromContext();
  jstring js_QueryContext = NULL;
  if (queryContext != NULL) {
   js_QueryContext = jenv_->NewStringUTF(queryContext);
  } else {
    js_QueryContext = jenv_->NewStringUTF("");
  }
  if (js_QueryContext == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDUPDATEROW_PARAM));
    if (htc != NULL)
      NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CHECKANDUPDATEROW_PARAM;
  }
  Text encRowIdText;
  const char * rowIdVal = rowID.val;
  int rowIdLen = rowID.len;

  jobject jRow = NULL;
  if (encryptionInfo) {
    if (ComEncryption::isFlagSet(*(ComEncryption::EncryptionInfo*)encryptionInfo,
                                 ComEncryption::ROWID_ENCRYPT)) {
      encryptRowId(encryptionInfo, heap, 1, rowIdVal, rowIdLen, encRowIdText);
      rowIdVal = encRowIdText.c_str();
      rowIdLen = encRowIdText.length();
    }

    unsigned char encDataBuffer[ENC_STATIC_BUFFER_LEN];
    unsigned char * encRowData = NULL;
    Int32 encRowLen = 0;
    encRowData = encDataBuffer;
    short rc =
      encryptRowsData(encryptionInfo, heap, 1,
                      row.val, row.len, encRowData, encRowLen);
    if (rc) {
      if (encRowData && (encRowData != encDataBuffer))
        NADELETEBASIC(encRowData, heap);
      GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_INSERTROW_PARAM));
      if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
      jenv_->PopLocalFrame(NULL);
      return HBC_ERROR_INSERTROW_PARAM;
    }

    if (encRowData) {
      jRow = jenv_->NewDirectByteBuffer(encRowData, encRowLen);
      if (encRowData != encDataBuffer)
        NADELETEBASIC(encRowData, heap);    
    }
  }
  else
    jRow = jenv_->NewDirectByteBuffer(row.val, row.len);

  if (jRow == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDUPDATEROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CHECKANDUPDATEROW_PARAM;
  }

  jbyteArray jba_rowID = jenv_->NewByteArray(rowIdLen);
  if (jba_rowID == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDUPDATEROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CHECKANDUPDATEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowIdLen, (const jbyte*)rowIdVal);

  jbyteArray jba_columnToCheck = jenv_->NewByteArray(columnToCheck.len);
  if (jba_columnToCheck == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDUPDATEROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CHECKANDUPDATEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_columnToCheck, 0, columnToCheck.len, (const jbyte*)columnToCheck.val);

  jbyteArray jba_columnValToCheck = jenv_->NewByteArray(columnValToCheck.len);
  if (jba_columnValToCheck == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDUPDATEROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CHECKANDUPDATEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_columnValToCheck, 0, columnValToCheck.len, (const jbyte*)columnValToCheck.val);
  jlong j_htc = (long)htc;
  jlong j_tid = transID;  
  jlong j_sid = savepointID;
  jlong j_psid = pSavepointId;
  jlong j_ts = timestamp;
  jint j_nodeId = GetCliGlobals()->myNodeNumber();

  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_HBC_DIRECT_CHECKANDUPDATE_ROW].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_,
					      JavaMethods_[JM_HBC_DIRECT_CHECKANDUPDATE_ROW].methodID, 
					      j_htc,
					      js_tblName,
					      j_tid,
                                              j_sid,
                                              j_psid,
					      jba_rowID,
					      jRow,
					      jba_columnToCheck,
					      jba_columnValToCheck,
					      j_ts,
					      (jint)flags, j_nodeId, js_QueryContext);
  if (hbs) {
      hbs->incHbaseCalls();
      if (! htc->asyncOper(flags))
         hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::checkAndUpdateRow()", tableName);

    HBC_RetCode ret_Lock = HBC_OK;
    if (gEnableRowLevelLock) {
      ret_Lock = getLockError();
    }

    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    if (ret_Lock != HBC_OK){
      return ret_Lock;
    }
    return HBC_ERROR_CHECKANDUPDATEROW_EXCEPTION;
  }
  if (hbs)
    hbs->incBytesRead(rowID.len + row.len);
  if (jresult == false) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDUPDATEROW_NOTFOUND));
     jenv_->PopLocalFrame(NULL);
     if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
     return HBC_ERROR_CHECKANDUPDATEROW_NOTFOUND;
  }
  *outHtc = htc;
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::deleteRow(NAHeap *heap, const char *tableName,
				       ExHbaseAccessStats *hbs,
				       Int64 transID,
                                       Int64 savepointID,
                                       Int64 pSavepointId,
				       HbaseStr rowID,
				       const LIST(HbaseStr) *cols, 
				       Int64 timestamp,
                                       const char * hbaseAuths,
                                       Int32 flags,
                                       const char * encryptionInfo,
				       const char * triggers,
				       const char * curExecSql,
				       HTableClient_JNI **outHtc,
				       ExDDLValidator * ddlValidator)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::deleteRow(%ld, %s) called.", transID, rowID.val);

  HTableClient_JNI *htc = NULL;

  if (ddlValidator && !ddlValidator->isDDLValid())
    return HBC_ERROR_DELETEROW_INVALID_DDL;

  //call trigger
  int base64RowIdLen = 0;
  unsigned char* base64RowIdVal = NULL;
  if (isExistTrigger((BeforeAndAfterTriggers *)triggers))
    {
      base64RowIdVal = new (heap) unsigned char[3 * rowID.len + 1];
      base64RowIdLen = EVP_EncodeBlock(base64RowIdVal, (unsigned char *)rowID.val, rowID.len);
    }
  HBC_RetCode reCode = execTriggers(heap,
				  tableName,
				  COM_DELETE,
				  TRUE,
				  (BeforeAndAfterTriggers *)triggers,
				  rowID,
				  rowID,
				  NULL,
				  0,
				  base64RowIdVal,
				  base64RowIdLen,
				  0,
				  curExecSql);
  if (reCode != HBC_OK)
    {
      NADELETE(htc, HTableClient_JNI, heap);
      return reCode;
    }
  //end call trigger
  
  if (htc->asyncOper(flags)) {
     htc = new (heap) HTableClient_JNI(heap, (jobject)-1);
     if (htc->init() != HTC_OK) {
         NADELETE(htc, HTableClient_JNI, heap);
         return HBC_ERROR_DELETEROW_EXCEPTION;
     }
     htc->setTableName(tableName);
     htc->setHbaseStats(hbs);
  }
  if (initJNIEnv() != JOI_OK) {
    if (htc != NULL) 
       NADELETE(htc, HTableClient_JNI, heap);
     return HBC_ERROR_INIT_PARAM;
  }

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DELETEROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETEROW_PARAM;
  }

  char* queryContext = getClientInfoFromContext();
  jstring js_QueryContext = NULL;
  if (queryContext != NULL) {
   js_QueryContext = jenv_->NewStringUTF(queryContext);
  } else {
    js_QueryContext = jenv_->NewStringUTF("");
  }
  if (js_QueryContext == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DELETEROW_PARAM));
    if (htc != NULL)
      NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETEROW_PARAM;
  }

  Text encRowIdText;
  const char * rowIdVal = rowID.val;
  int rowIdLen = rowID.len;
  if (encryptionInfo) {
   if (ComEncryption::isFlagSet(*(ComEncryption::EncryptionInfo*)encryptionInfo,
                                 ComEncryption::ROWID_ENCRYPT)) {
     encryptRowId(encryptionInfo, heap, 1, rowIdVal, rowIdLen, encRowIdText);
     rowIdVal = encRowIdText.c_str();
     rowIdLen = encRowIdText.length();
   }
  }

  jbyteArray jba_rowID = jenv_->NewByteArray(rowIdLen);
  if (jba_rowID == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DELETEROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_DELETEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowIdLen, (const jbyte*)rowIdVal);
  jobjectArray j_cols = NULL;
  if (cols != NULL && !cols->isEmpty()) {
     j_cols = convertToByteArrayObjectArray(*cols);
     if (j_cols == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::deleteRow()", tableName);
        if (htc != NULL)
           NADELETE(htc, HTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return HBC_ERROR_DELETEROW_PARAM;
     }
  }  
  jlong j_htc = (jlong)htc;
  jlong j_tid = transID;
  jlong j_sid = savepointID;
  jlong j_psid = pSavepointId;
  jlong j_ts = timestamp;

  jstring js_hbaseAuths = NULL;
  if (hbaseAuths)
    {
      js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
      if (js_hbaseAuths == NULL)
        {
          GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DELETEROW_PARAM));
          NADELETE(htc, HTableClient_JNI, heap);
          jenv_->PopLocalFrame(NULL);
          return HBC_ERROR_DELETEROW_PARAM;
        }
    }

  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_HBC_DELETE_ROW].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
          JavaMethods_[JM_HBC_DELETE_ROW].methodID,
					      j_htc,
					      js_tblName,
					      j_tid,
                                              j_sid,
                                              j_psid,
					      jba_rowID,
					      j_cols,
					      j_ts,
                                              js_hbaseAuths,
                                              (jint)flags, js_QueryContext);
  if (hbs) {
      hbs->incHbaseCalls();
      if (! htc->asyncOper(flags))
        hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::deleteRow()", tableName);

    HBC_RetCode ret_Lock = HBC_OK;
    if (gEnableRowLevelLock) {
      ret_Lock = getLockError();
    }

    jenv_->PopLocalFrame(NULL);
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (ret_Lock != HBC_OK){
      return ret_Lock;
    }
    return HBC_ERROR_DELETEROW_EXCEPTION;
  }

  if (jresult == false) {
     logError(CAT_SQL_HBASE, "HBaseClient_JNI::deleteRow()", getLastError());
     jenv_->PopLocalFrame(NULL);
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
     return HBC_ERROR_DELETEROW_EXCEPTION;
  }

  if (hbs)
    hbs->incBytesRead(rowID.len);
  *outHtc = htc;
  jenv_->PopLocalFrame(NULL);

  //call trigger
  reCode = execTriggers(heap,
			tableName,
			COM_DELETE,
			FALSE,
			(BeforeAndAfterTriggers *)triggers,
			rowID,
			rowID,
			NULL,
			0,
			base64RowIdVal,
			base64RowIdLen,
			0,
			curExecSql);
  if (reCode != HBC_OK)
    {
      NADELETE(htc, HTableClient_JNI, heap);
      return reCode;
    }
  //end call trigger
  
  return HBC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::deleteRows(NAHeap *heap,
					const char *tableName,
					ExHbaseAccessStats *hbs,
					Int64 transID,
                                        Int64 savepointID,
                                        Int64 pSavepointId,
					short rowIDLen,
					HbaseStr rowIDs, 
                                        const LIST(HbaseStr) *cols, 
					Int64 timestamp,
                                        const char * hbaseAuths,
                                        Int32 flags,
                                        const char * encryptionInfo,
					const char * triggers,
					const char * curExecSql,
					HTableClient_JNI **outHtc,
					ExDDLValidator * ddlValidator)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::deleteRows(%ld, %s) called.", transID, rowIDs.val);

  HTableClient_JNI *htc = NULL;

  if (ddlValidator && !ddlValidator->isDDLValid())
    return HBC_ERROR_DELETEROWS_INVALID_DDL;

  //call trigger
  int base64RowIdsLen = 0;
  unsigned char* base64RowIdsVal = NULL;

  if (isExistTrigger((BeforeAndAfterTriggers *)triggers))
    {
      base64RowIdsVal = new (heap) unsigned char[3 * rowIDs.len + 1];
      base64RowIdsLen = EVP_EncodeBlock(base64RowIdsVal, (unsigned char *)rowIDs.val, rowIDs.len);
    }

  HBC_RetCode reCode = execTriggers(heap,
				  tableName,
				  COM_DELETE,
				  TRUE,
				  (BeforeAndAfterTriggers *)triggers,
				  rowIDs,
				  rowIDs,
				  NULL,
				  0,
				  base64RowIdsVal,
				  base64RowIdsLen,
				  rowIDLen,
				  curExecSql);
  if (reCode != HBC_OK)
    {
      NADELETE(htc, HTableClient_JNI, heap);
      return reCode;
    }
  //end call trigger
  
  if (htc->asyncOper(flags)) {
      htc = new (heap) HTableClient_JNI(heap, (jobject)-1);
     if (htc->init() != HTC_OK) {
         NADELETE(htc, HTableClient_JNI, heap);
         return HBC_ERROR_DELETEROWS_EXCEPTION;
     }
     htc->setTableName(tableName);
     htc->setHbaseStats(hbs);
  }
  if (initJNIEnv() != JOI_OK) {
    if (htc != NULL) 
       NADELETE(htc, HTableClient_JNI, heap);
     return HBC_ERROR_INIT_PARAM;
  }

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DELETEROWS_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETEROWS_PARAM;
  }

  char* queryContext = getClientInfoFromContext();
  jstring js_QueryContext = NULL;
  if (queryContext != NULL) {
   js_QueryContext = jenv_->NewStringUTF(queryContext);
  } else {
    js_QueryContext = jenv_->NewStringUTF("");
  }
  if (js_QueryContext == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DELETEROWS_PARAM));
    if (htc != NULL)
      NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETEROWS_PARAM;
  }

  jobject jRowIDs = jenv_->NewDirectByteBuffer(rowIDs.val, rowIDs.len);
  if (jRowIDs == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DELETEROWS_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETEROWS_PARAM;
  }

  jobjectArray j_cols = NULL;
  if (cols != NULL && !cols->isEmpty()) {
     j_cols = convertToByteArrayObjectArray(*cols);
     if (j_cols == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::deleteRows()", tableName);
        if (htc != NULL)
           NADELETE(htc, HTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return HBC_ERROR_DELETEROWS_PARAM;
     }
  }  

  jlong j_htc = (jlong)htc;
  jlong j_tid = transID;
  jlong j_sid = savepointID;
  jlong j_psid = pSavepointId;
  jshort j_rowIDLen = rowIDLen;
  jlong j_ts = timestamp;

  jstring js_hbaseAuths = NULL;
  if (hbaseAuths)
    {
      js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
      if (js_hbaseAuths == NULL)
        {
          GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DELETEROWS_PARAM));
          NADELETE(htc, HTableClient_JNI, heap);
          jenv_->PopLocalFrame(NULL);
          return HBC_ERROR_DELETEROWS_PARAM;
        }
    }

  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_HBC_DIRECT_DELETE_ROWS].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
					      JavaMethods_[JM_HBC_DIRECT_DELETE_ROWS].methodID,
					      j_htc,
					      js_tblName,
					      j_tid,
                                              j_sid,
                                              j_psid,
					      j_rowIDLen,
					      jRowIDs,
                                              j_cols,
					      j_ts,
                                              js_hbaseAuths,
                                              (jint)flags, js_QueryContext);
  if (hbs) {
      hbs->incHbaseCalls();
      if (! htc->asyncOper(flags))
         hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::deleteRows()", tableName);

    HBC_RetCode ret_Lock = HBC_OK;
    if (gEnableRowLevelLock) {
      ret_Lock = getLockError();
    }

    jenv_->PopLocalFrame(NULL);
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (ret_Lock != HBC_OK){
      return ret_Lock;
    }
    return HBC_ERROR_DELETEROWS_EXCEPTION;
  }
  if (jresult == false) {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::deleteRows()", getLastError());
    jenv_->PopLocalFrame(NULL);
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    return HBC_ERROR_DELETEROWS_EXCEPTION;
  }
  if (hbs)
    hbs->incBytesRead(rowIDs.len);
  *outHtc = htc;
  jenv_->PopLocalFrame(NULL);

  //call trigger
  reCode = execTriggers(heap,
			tableName,
			COM_DELETE,
			FALSE,
			(BeforeAndAfterTriggers *)triggers,
			rowIDs,
			rowIDs,
			NULL,
			0,
			base64RowIdsVal,
			base64RowIdsLen,
			rowIDLen,
			curExecSql);
  if (reCode != HBC_OK)
    {
      NADELETE(htc, HTableClient_JNI, heap);
      return reCode;
    }
  //end call trigger
  
  return HBC_OK;
}
//
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HBC_RetCode HBaseClient_JNI::checkAndDeleteRow(NAHeap *heap,
					       const char *tableName,
					       ExHbaseAccessStats *hbs,
					       Int64 transID,
                                               Int64 savepointID,
                                               Int64 pSavepointId,
					       HbaseStr rowID, 
                                               const LIST(HbaseStr) *cols, 
					       HbaseStr columnToCheck,
					       HbaseStr columnValToCheck,
					       Int64 timestamp,
                                               const char * hbaseAuths,
                                               Int32 flags,
                                               const char * encryptionInfo,
					       const char * triggers,
					       const char * curExecSql,
					       HTableClient_JNI **outHtc,
					       ExDDLValidator * ddlValidator)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::checkAndDeleteRow(%ld, %s) called.", transID, rowID.val);

  HTableClient_JNI *htc = NULL;

  if (ddlValidator && !ddlValidator->isDDLValid())
    return HBC_ERROR_CHECKANDDELETEROW_INVALID_DDL;

  if (htc->asyncOper(flags)) {
     htc = new (heap) HTableClient_JNI(heap, (jobject)-1);
     if (htc->init() != HTC_OK) {
         NADELETE(htc, HTableClient_JNI, heap);
         return HBC_ERROR_CHECKANDDELETEROW_EXCEPTION;
     }
     htc->setTableName(tableName);
     htc->setHbaseStats(hbs);
  }
  if (initJNIEnv() != JOI_OK) {
    if (htc != NULL) 
       NADELETE(htc, HTableClient_JNI, heap);
     return HBC_ERROR_INIT_PARAM;
  }

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDDELETEROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CHECKANDDELETEROW_PARAM;
  }

  char* queryContext = getClientInfoFromContext();
    jstring js_QueryContext = NULL;
    if (queryContext != NULL) {
     js_QueryContext = jenv_->NewStringUTF(queryContext);
    } else {
      js_QueryContext = jenv_->NewStringUTF("");
    }
    if (js_QueryContext == NULL) {
      GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDDELETEROW_PARAM));
      if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
      jenv_->PopLocalFrame(NULL);
      return HBC_ERROR_CHECKANDDELETEROW_PARAM;
    }

  Text encRowIdText;
  const char * rowIdVal = rowID.val;
  int rowIdLen = rowID.len;
  if (encryptionInfo && 
      ComEncryption::isFlagSet(*(ComEncryption::EncryptionInfo*)encryptionInfo,
                               ComEncryption::ROWID_ENCRYPT)) {
    encryptRowId(encryptionInfo, heap, 1, rowIdVal, rowIdLen, encRowIdText);
    rowIdVal = encRowIdText.c_str();
    rowIdLen = encRowIdText.length();
  }
  
  jbyteArray jba_rowID = jenv_->NewByteArray(rowIdLen);
  if (jba_rowID == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDDELETEROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_CHECKANDDELETEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowIdLen, (const jbyte*)rowIdVal);

  jobjectArray j_cols = NULL;
  if (cols != NULL && !cols->isEmpty()) {
     j_cols = convertToByteArrayObjectArray(*cols);
     if (j_cols == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::checkAndDeleteRow()", tableName);
        if (htc != NULL)
           NADELETE(htc, HTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return HBC_ERROR_DELETEROW_PARAM;
     }
  }  
  jbyteArray jba_columnToCheck = jenv_->NewByteArray(columnToCheck.len);
  if (jba_columnToCheck == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDDELETEROW_PARAM));
    if (htc != NULL)
      NADELETE(htc, HTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CHECKANDDELETEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_columnToCheck, 0, columnToCheck.len, (const jbyte*)columnToCheck.val);
  jbyteArray jba_columnValToCheck = jenv_->NewByteArray(columnValToCheck.len);
  if (jba_columnValToCheck == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDDELETEROW_PARAM));
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_CHECKANDDELETEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_columnValToCheck, 0, columnValToCheck.len, (const jbyte*)columnValToCheck.val);
  jlong j_htc = (jlong)htc;
  jlong j_tid = transID;  
  jlong j_sid = savepointID;
  jlong j_psid = pSavepointId;
  jlong j_ts = timestamp;

  jstring js_hbaseAuths = NULL;
  if (hbaseAuths)
    {
      js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
      if (js_hbaseAuths == NULL)
        {
          GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CHECKANDDELETEROW_PARAM));
          if (htc != NULL)
            NADELETE(htc, HTableClient_JNI, heap);
          jenv_->PopLocalFrame(NULL);
          return HBC_ERROR_CHECKANDDELETEROW_PARAM;
        }
    }

  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_HBC_CHECKANDDELETE_ROW].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
					      JavaMethods_[JM_HBC_CHECKANDDELETE_ROW].methodID,
					      j_htc,
					      js_tblName,
					      j_tid,
                                              j_sid,
                                              j_psid,
					      jba_rowID, 
                                              j_cols,
					      jba_columnToCheck,
					      jba_columnValToCheck,
					      j_ts,
                                              js_hbaseAuths,
                                              (jint)flags, js_QueryContext);
  if (hbs) {
      hbs->incHbaseCalls();
      if (! htc->asyncOper(flags))
         hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::checkAndDeleteRow()", tableName);

    HBC_RetCode ret_Lock = HBC_OK;
    if (gEnableRowLevelLock) {
      ret_Lock = getLockError();
    }

    jenv_->PopLocalFrame(NULL);
    if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
    if (ret_Lock != HBC_OK){
      return ret_Lock;
    }
    return HBC_ERROR_CHECKANDDELETEROW_EXCEPTION;
  }

  if (jresult == false) {
     jenv_->PopLocalFrame(NULL);
     if (htc != NULL)
        NADELETE(htc, HTableClient_JNI, heap);
     return HBC_ERROR_CHECKANDDELETEROW_NOTFOUND;
  }

  if (hbs)
    hbs->incBytesRead(rowID.len);
  *outHtc = htc;
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::addTablesToHDFSCache(const TextVec& tables, const char* poolName)
{
  if (initJNIEnv() != JOI_OK) 
     return HBC_ERROR_INIT_PARAM;

  jobjectArray j_tables = NULL;
  if (!tables.empty())
  {
    QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  Adding %d tables.", tables.size());
    j_tables = convertToStringObjectArray(tables);
    if (j_tables == NULL)
    {
       getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::addTablesToHDFSSCache()");
       jenv_->PopLocalFrame(NULL);
       return HBC_ERROR_ADDHDFSCACHE_EXCEPTION;
    }
  }

  jstring js_poolName = jenv_->NewStringUTF(poolName);
  if (js_poolName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_ADDHDFSCACHE_EXCEPTION));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_ADDHDFSCACHE_EXCEPTION;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_ADD_TABLES_TO_HDFS_CACHE].jm_full_name;
  jint jresult = jenv_->CallIntMethod(javaObj_, JavaMethods_[JM_ADD_TABLES_TO_HDFS_CACHE].methodID, j_tables, js_poolName);

  if (jenv_->ExceptionCheck())
  { 
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::addTablesToHDFSSCache()");
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_ADDHDFSCACHE_EXCEPTION;
  }

  jenv_->DeleteLocalRef(js_poolName);
  if (j_tables != NULL) 
    jenv_->DeleteLocalRef(j_tables);  

  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, 
       "Exit HBaseClient_JNI::addTablesToHDFSCache() called.");
  jenv_->PopLocalFrame(NULL);
  return jresult==0 ? HBC_OK : HBC_ERROR_POOL_NOT_EXIST_EXCEPTION;
}

HBC_RetCode HBaseClient_JNI::removeTablesFromHDFSCache(const TextVec& tables, const char* poolName)
{
  if (initJNIEnv() != JOI_OK) 
     return HBC_ERROR_INIT_PARAM;

  jobjectArray j_tables = NULL;
  if (!tables.empty())
  {
    QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  removing %d tables.", tables.size());
    j_tables = convertToStringObjectArray(tables);
    if (j_tables == NULL)
    {
       getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::removeTablesFromHDFSSCache()");
       jenv_->PopLocalFrame(NULL);
       return HBC_ERROR_ADDHDFSCACHE_EXCEPTION;
    }
  }

  jstring js_poolName = jenv_->NewStringUTF(poolName);
  if (js_poolName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_ADDHDFSCACHE_EXCEPTION));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_ADDHDFSCACHE_EXCEPTION;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_REMOVE_TABLES_FROM_HDFS_CACHE].jm_full_name;
  jint jresult = jenv_->CallIntMethod(javaObj_, JavaMethods_[JM_REMOVE_TABLES_FROM_HDFS_CACHE].methodID, j_tables, js_poolName);

  if (jenv_->ExceptionCheck())
  { 
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::removeTablesFromHDFSSCache()");
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_ADDHDFSCACHE_EXCEPTION;
  }

  jenv_->DeleteLocalRef(js_poolName);
  if (j_tables != NULL) 
    jenv_->DeleteLocalRef(j_tables);  

  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, 
       "Exit HBaseClient_JNI::addTablesToHDFSCache() called.");
  jenv_->PopLocalFrame(NULL);
  return jresult==0 ? HBC_OK : HBC_ERROR_POOL_NOT_EXIST_EXCEPTION;
}

NAArray<HbaseStr > *HBaseClient_JNI::showTablesHDFSCache(NAHeap *heap, const TextVec& tables)
{
  if (initJNIEnv() != JOI_OK) 
     return NULL;

  jobjectArray j_tables = NULL;
  if (!tables.empty())
  {
    QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  showing %d tables.", tables.size());
    j_tables = convertToStringObjectArray(tables);
    if (j_tables == NULL)
    {
       getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::showTablesHDFSSCache()");
       jenv_->PopLocalFrame(NULL);
       return NULL;
    }
  }

  tsRecentJMFromJNI = JavaMethods_[JM_SHOW_TABLES_HDFS_CACHE].jm_full_name;
  jarray j_tableHdfsCacheStats = (jarray) jenv_->CallObjectMethod(javaObj_, JavaMethods_[JM_SHOW_TABLES_HDFS_CACHE].methodID, j_tables);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::showTablesHDFSSCache()");
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  NAArray<HbaseStr> *tableHdfsCacheStats;
  jint retcode = convertByteArrayObjectArrayToNAArray(heap, j_tableHdfsCacheStats, &tableHdfsCacheStats);
  if (retcode == 0)
     return NULL;
  else
     return tableHdfsCacheStats;
}

NAArray<HbaseStr>* HBaseClient_JNI::getStartKeys(NAHeap *heap, const char *tableName, bool useTRex)
{
   return HBaseClient_JNI::getKeys(JM_HBC_GETSTARTKEYS, heap, tableName, useTRex);
}

NAArray<HbaseStr>* HBaseClient_JNI::getEndKeys(NAHeap *heap, const char * tableName, bool useTRex)
{
   return HBaseClient_JNI::getKeys(JM_HBC_GETENDKEYS, heap, tableName, useTRex);
}

NAArray<HbaseStr>* HBaseClient_JNI::getKeys(Int32 funcIndex, NAHeap *heap, const char *tableName, bool useTRex)
{
  if (initJNIEnv() != JOI_OK) 
     return NULL;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GETKEYS));
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
  jboolean j_useTRex = useTRex;
  tsRecentJMFromJNI = JavaMethods_[funcIndex].jm_full_name;
  jarray j_keyArray=
     (jarray)jenv_->CallObjectMethod(javaObj_, JavaMethods_[funcIndex].methodID, js_tblName, j_useTRex);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::getKeys()", tableName);
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
  NAArray<HbaseStr> *retArray;
  jint retcode = convertByteArrayObjectArrayToNAArray(heap, j_keyArray, &retArray);
  jenv_->PopLocalFrame(NULL);
  if (retcode == 0)
     return NULL;
  else
     return retArray;  
}

int HBaseClient_JNI::getReplicaId(int numReplications)
{
   int replicaId = -1;
   Int32 myNodeId;
   if (numReplications > 1) {
      myNodeId = GetCliGlobals()->myCpu();
      replicaId = (myNodeId % numReplications);
   }
   return replicaId;
}

HBC_RetCode HBaseClient_JNI::createSnapshot( const NAString&  tableName, const NAString&  snapshotName)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::createSnapshot(%s, %s) called.",
      tableName.data(), snapshotName.data());

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_tableName = jenv_->NewStringUTF(tableName.data());
  if (js_tableName == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CREATE_SNAPSHOT_PARAM));
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_CREATE_SNAPSHOT_PARAM;
  }
  jstring js_snapshotName= jenv_->NewStringUTF(snapshotName.data());
  if (js_snapshotName == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_CREATE_SNAPSHOT_PARAM));
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_CREATE_SNAPSHOT_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_HBC_CREATE_SNAPSHOT].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_HBC_CREATE_SNAPSHOT].methodID, js_tableName, js_snapshotName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::createSnapshot()", tableName.data());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CREATE_SNAPSHOT_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::restoreSnapshot( const NAString&  snapshotName, const NAString&  tableName)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::restoreSnapshot(%s, %s) called.",
      snapshotName.data(), tableName.data());

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_tableName = jenv_->NewStringUTF(tableName.data());
  if (js_tableName == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_RESTORE_SNAPSHOT_PARAM));
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_RESTORE_SNAPSHOT_PARAM;
  }
  jstring js_snapshotName= jenv_->NewStringUTF(snapshotName.data());
  if (js_snapshotName == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_RESTORE_SNAPSHOT_PARAM));
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_RESTORE_SNAPSHOT_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_HBC_RESTORE_SNAPSHOT].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_HBC_RESTORE_SNAPSHOT].methodID, js_snapshotName, js_tableName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::restoreSnapshot()", tableName.data());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_RESTORE_SNAPSHOT_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::verifySnapshot( const NAString&  tableName, const NAString&  snapshotName,
                                                NABoolean & exist)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::verifySnapshot(%s, %s) called.",
      tableName.data(), snapshotName.data());

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_tableName = jenv_->NewStringUTF(tableName.data());
  if (js_tableName == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_VERIFY_SNAPSHOT_PARAM));
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_VERIFY_SNAPSHOT_PARAM;
  }
  jstring js_snapshotName= jenv_->NewStringUTF(snapshotName.data());
  if (js_snapshotName == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_VERIFY_SNAPSHOT_PARAM));
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_VERIFY_SNAPSHOT_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_HBC_VERIFY_SNAPSHOT].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_HBC_VERIFY_SNAPSHOT].methodID, js_tableName, js_snapshotName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::verifySnapshot()", tableName.data());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_VERIFY_SNAPSHOT_EXCEPTION;
  }

  exist = jresult;

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::deleteSnapshot( const NAString&  snapshotName)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::deleteSnapshot(%s) called.",
      snapshotName.data());

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_snapshotName= jenv_->NewStringUTF(snapshotName.data());
  if (js_snapshotName == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_DELETE_SNAPSHOT_PARAM));
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_DELETE_SNAPSHOT_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_HBC_DELETE_SNAPSHOT].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_HBC_DELETE_SNAPSHOT].methodID, js_snapshotName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::deleteSnapshot()", snapshotName.data());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETE_SNAPSHOT_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode  HBaseClient_JNI::getNextValue(NAString& tabName, NAString& rowId,
                                           NAString& famName, NAString& qualName,
                                           Int64 incrBy, Int64 &nextValue,
                                           NABoolean skipWAL)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::getNextValue() called.");
  if (initJNIEnv() != JOI_OK)
    return HBC_ERROR_INIT_PARAM;

  jstring js_tabName = jenv_->NewStringUTF(tabName.data());
  if (js_tabName == NULL) {
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_NEXT_VALUE_PARAM;
  }

  jstring js_rowId = jenv_->NewStringUTF(rowId.data());
  if (js_rowId == NULL) {
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_NEXT_VALUE_PARAM;
  }

  jstring js_famName = jenv_->NewStringUTF(famName.data());
  if (js_famName == NULL) {
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_NEXT_VALUE_PARAM;
  }

  jstring js_qualName = jenv_->NewStringUTF(qualName.data());
  if (js_qualName == NULL) {
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_NEXT_VALUE_PARAM;
  }

  jlong j_incrBy = incrBy;
  jboolean j_skipWAL = skipWAL;

  tsRecentJMFromJNI = JavaMethods_[JM_GET_NEXT_VALUE].jm_full_name;
  jlong jnextVal = jenv_->CallLongMethod(javaObj_, JavaMethods_[JM_GET_NEXT_VALUE].methodID, js_tabName, js_rowId, js_famName, js_qualName, j_incrBy, j_skipWAL);

  nextValue = jnextVal;
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBulkLoadClient_JNI::getNextValue(), rowId = %s", rowId.data());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_NEXT_VALUE_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

 HBC_RetCode  HBaseClient_JNI::getTableDefForBinlog(NAString& tabName, NAHeap *heap, NAArray<HbaseStr>* *retNames )
 {
   QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::getTableDefForBinlog() called.");
   if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

   jstring js_tabName = jenv_->NewStringUTF(tabName.data());
   if (js_tabName == NULL) {
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_DELETE_SEQ_ROW_PARAM;
   }

   tsRecentJMFromJNI = JavaMethods_[JM_GET_TBL_DEF_BINLOG].jm_full_name;

   jobjectArray j_tableDefObjs = (jobjectArray)
   jenv_->CallLongMethod(javaObj_, JavaMethods_[JM_GET_TBL_DEF_BINLOG].methodID, js_tabName);

   if (jenv_->ExceptionCheck())
   {
     getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::getTableDefForBinlog()");
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_GET_TABLE_DEF_FOR_BINLOG_PARAM;
   }

   if ((j_tableDefObjs != NULL) && (retNames != NULL)) {
     jint retcode =
       convertStringObjectArrayToNAArray(heap, j_tableDefObjs, retNames);
     if(retcode == 0) {
       jenv_->PopLocalFrame(NULL);
       return HBC_ERROR_GET_TABLE_DEF_FOR_BINLOG_ERROR;
     }
   }
   return HBC_OK;
 }

HBC_RetCode  HBaseClient_JNI::updateTableDefForBinlog(NAString& tabName, NAString& cols, NAString& keys, long ts )
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::updateTableDefForBinlog() called.");
  if (initJNIEnv() != JOI_OK)
    return HBC_ERROR_INIT_PARAM;

  jstring js_tabName = jenv_->NewStringUTF(tabName.data());
  if (js_tabName == NULL) {
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETE_SEQ_ROW_PARAM;
  }

  jstring js_cols= jenv_->NewStringUTF(cols.data());
  if (js_cols == NULL) {
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETE_SEQ_ROW_PARAM;
  }

  jstring js_keys= jenv_->NewStringUTF(keys.data());
  if (js_keys == NULL) {
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETE_SEQ_ROW_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_UPDATE_TBL_DEF_BINLOG].jm_full_name;
  jenv_->CallLongMethod(javaObj_, JavaMethods_[JM_UPDATE_TBL_DEF_BINLOG].methodID, js_tabName, js_cols, js_keys, (jlong)ts);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBulkLoadClient_JNI::updateTableDefForBinlog(), cols = %s", cols.data());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETE_SEQ_ROW_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode  HBaseClient_JNI::deleteSeqRow(NAString& tabName, NAString& rowId)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::deleteSeqRow() called.");
  if (initJNIEnv() != JOI_OK)
    return HBC_ERROR_INIT_PARAM;

  jstring js_tabName = jenv_->NewStringUTF(tabName.data());
  if (js_tabName == NULL) {
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETE_SEQ_ROW_PARAM;
  }

  jstring js_rowId = jenv_->NewStringUTF(rowId.data());
  if (js_rowId == NULL) {
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETE_SEQ_ROW_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_DELETE_SEQ_ROW].jm_full_name;
  jenv_->CallLongMethod(javaObj_, JavaMethods_[JM_DELETE_SEQ_ROW].methodID, js_tabName, js_rowId);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBulkLoadClient_JNI::deleteSeqRow(), rowId = %s", rowId.data());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_DELETE_SEQ_ROW_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::lockRequired(NAHeap *heap, const char *tableName, ExHbaseAccessStats *hbs, Int64 transactionId, Int64 savepointId, Int64 pSavepointId, Int32 lockMode, NABoolean registerRegion, HTableClient_JNI **outHtc)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::lockRequired(%s) called.", tableName);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jstring js_tableName = jenv_->NewStringUTF(tableName);
  if (js_tableName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GET_HBTI_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_GET_HBTI_PARAM;
  }

  char* queryContext = getClientInfoFromContext();
   jstring js_QueryContext = NULL;
   if (queryContext != NULL) {
     js_QueryContext = jenv_->NewStringUTF(queryContext);
   } else {
     js_QueryContext = jenv_->NewStringUTF("");
   }
   if (js_QueryContext == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_GET_HBTI_PARAM));
     jenv_->PopLocalFrame(NULL);
     return HBC_ERROR_GET_HBTI_PARAM;
   }


  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_LOCKREQUIRED].methodID,
                                              js_tableName, (jlong)transactionId, (jlong)savepointId, (jlong)pSavepointId, (jint)lockMode, (jboolean)registerRegion, js_QueryContext);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::lockRequired()");

    HBC_RetCode ret_Lock = HBC_OK;
    if (gEnableRowLevelLock) {
      ret_Lock = getLockError();
    }

    jenv_->PopLocalFrame(NULL);
    if (ret_Lock != HBC_OK){
      return ret_Lock;
    }
    return HBC_ERROR_LOCKREQUIRED_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

HBC_RetCode HBaseClient_JNI::cancelOperation(Int64 transID) 
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::cancelOperation called.");

  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jlong j_tid = transID;

  tsRecentJMFromJNI = JavaMethods_[JM_CANCLE_OPERATION].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_CANCLE_OPERATION].methodID, j_tid);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::cancelOperation()");
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_CANCEL_OPERATION_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}

// ===========================================================================
// ===== Class HTableClient
// ===========================================================================

JavaMethodInit* HTableClient_JNI::JavaMethods_ = NULL;
jclass HTableClient_JNI::javaClass_ = 0;
bool HTableClient_JNI::javaMethodsInitialized_ = false;
pthread_mutex_t HTableClient_JNI::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

static const char* const htcErrorEnumStr[] = 
{
  "Preparing parameters for initConnection()."
 ,"Java exception in initConnection()."
 ,"Java exception in setTransactionID()."
 ,"Java exception in cleanup()."
 ,"Java exception in close()."
 ,"Preparing parameters for scanOpen()."
 ,"Java exception in scanOpen()."
 ,"Java exception in fetchRows()."
 ,"DDL for plan has become invalid."
 ,"Java exception in scanClose()."
 ,"Java exception in getClose()."
 ,"Preparing parameters for deleteRow()."
 ,"Java exception in deleteRow()."
 ,"Preparing parameters for create()."
 ,"Java exception in create()."
 ,"Preparing parameters for drop()."
 ,"Java exception in drop()."
 ,"Preparing parameters for exists()."
 ,"Java exception in exists()."
 ,"Preparing parameters for coProcAggr()."
 ,"Java exception in coProcAggr()."
 ,"Preparing parameters for grant()."
 ,"Java exception in grant()."
 ,"Preparing parameters for revoke()."
 ,"Java exception in revoke()."
 ,"Java exception in getendkeys()."
 ,"Java exception in getColName()."
 ,"Java exception in getColVal()."
 ,"Java exception in getRowID()."
 ,"Java exception in nextCell()."
 ,"Java exception in completeAsyncOperation()."
 ,"Async Hbase Operation not yet complete."
 ,"Java exception in setWriteToWal()."
 ,"Java exception in setWriteBufferSize()."
 ,"Java exception in prepareForNextCell()."
};

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
void HTableClient_JNI::setFlags(UInt32 &flags, NABoolean useTRex,
                                NABoolean syncRepl, NABoolean useRegionXn,
                                NABoolean incrBackup, NABoolean asyncOper,
                                NABoolean noConflictCheck, NABoolean updateIsUpsert)
{
  setUseTRex(flags, useTRex);
  setSyncRepl(flags, syncRepl);
  setUseRegionXn(flags, useRegionXn);
  setIncrementalBackup(flags, incrBackup);
  setAsyncOper(flags, asyncOper);
  setNoConflictCheck(flags, noConflictCheck);
  setPutIsUpsert(flags, updateIsUpsert);
}

char* HTableClient_JNI::getErrorText(HTC_RetCode errEnum)
{
  if (errEnum < (HTC_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)errEnum);
  else    
    return (char*)htcErrorEnumStr[errEnum-HTC_FIRST-1];
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HTableClient_JNI::~HTableClient_JNI()
{
  //QRLogger::log(CAT_JNI_TOP, LL_DEBUG, "HTableClient destructor called.");
  cleanupResultInfo();
  if (tableName_ != NULL)
  {
     NADELETEBASIC(tableName_, heap_);
  }
  if (colNameAllocLen_ != 0)
     NADELETEBASIC(colName_, heap_);
  if (numCellsAllocated_ > 0)
  {
      NADELETEBASIC(p_kvValLen_, heap_);
      NADELETEBASIC(p_kvValOffset_, heap_);
      NADELETEBASIC(p_kvFamLen_, heap_);
      NADELETEBASIC(p_kvFamOffset_, heap_);
      NADELETEBASIC(p_kvQualLen_, heap_);
      NADELETEBASIC(p_kvQualOffset_, heap_);
      NADELETEBASIC(p_timestamp_, heap_);
      numCellsAllocated_ = 0;
  }

  cleanupOldValueInfo();
}
 
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HTC_RetCode HTableClient_JNI::init()
{
  static char className[]="org/trafodion/sql/HTableClient";
  HTC_RetCode rc;
  
  if (isInitialized())
    return HTC_OK;
  
  if (javaMethodsInitialized_)
    return (HTC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
  else
  {
    pthread_mutex_lock(&javaMethodsInitMutex_);
    if (javaMethodsInitialized_)
    {
      pthread_mutex_unlock(&javaMethodsInitMutex_);
      return (HTC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    }
    JavaMethods_ = new JavaMethodInit[JM_LAST];
    JavaMethods_[JM_SCAN_OPEN  ].jm_name      = "startScan";
    JavaMethods_[JM_SCAN_OPEN  ].jm_signature = "(JJJI[B[B[Ljava/lang/Object;JZZI[Ljava/lang/Object;[Ljava/lang/Object;[Ljava/lang/Object;FFZIZZZILjava/lang/String;Ljava/lang/String;IIJJLjava/lang/String;IZZLjava/lang/String;)Z";
    JavaMethods_[JM_DELETE     ].jm_name      = "deleteRow";
    JavaMethods_[JM_DELETE     ].jm_signature = "(JJJ[B[Ljava/lang/Object;JLjava/lang/String;ILjava/lang/String;)Z";
    JavaMethods_[JM_COPROC_AGGR     ].jm_name      = "coProcAggr";
    JavaMethods_[JM_COPROC_AGGR     ].jm_signature = "(JJJIII[B[B[B[BZILjava/lang/String;)[B";
    JavaMethods_[JM_SET_WB_SIZE ].jm_name      = "setWriteBufferSize";
    JavaMethods_[JM_SET_WB_SIZE ].jm_signature = "(J)Z";
    JavaMethods_[JM_SET_WRITE_TO_WAL ].jm_name      = "setWriteToWAL";
    JavaMethods_[JM_SET_WRITE_TO_WAL ].jm_signature = "(Z)Z";
    JavaMethods_[JM_FETCH_ROWS ].jm_name      = "fetchRows";
    JavaMethods_[JM_FETCH_ROWS ].jm_signature = "()I";
    JavaMethods_[JM_COMPLETE_PUT ].jm_name      = "completeAsyncOperation";
    JavaMethods_[JM_COMPLETE_PUT ].jm_signature = "(I[Z)Z";
   
    rc = (HTC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    if (rc == HTC_OK)
       javaMethodsInitialized_ = TRUE;
    pthread_mutex_unlock(&javaMethodsInitMutex_);
  }
  return rc;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HTC_RetCode HTableClient_JNI::startScan(Int64 transID, Int64 savepointID, Int64 pSavepointId,
                                        Int32 isolationLevel, const Text& startRowID,
                                        const Text& stopRowID,
                                        const LIST(HbaseStr) & cols, Int64 timestamp,
                                        bool cacheBlocks, bool smallScanner,
                                        Lng32 numCacheRows, NABoolean preFetch,
                                        Int32 lockMode,
                                        NABoolean skipReadConflict,
					NABoolean skipTransaction,
					const LIST(NAString) *inColNamesToFilter,
					const LIST(NAString) *inCompareOpList,
					const LIST(NAString) *inColValuesToCompare,
                                        int numReplications,
                                        Float32 dopParallelScanner,
					Float32 samplePercent,
					NABoolean useSnapshotScan,
					Lng32 snapTimeout,
					char * snapName,
					char * tmpLoc,
					Lng32 espNum,
                                        Lng32 versions,
                                        Int64 minTS,
                                        Int64 maxTS,
                                        const char * hbaseAuths,
                                        const char * encryptionInfo,
                                        NABoolean waitOnSelectForUpdate,
                                        NABoolean firstReadBypassTm)
{
  ENTRY_FUNCTION();

  if (initJNIEnv() != JOI_OK) 
  {
     EXIT_FUNCTION(HTC_ERROR_INIT_PARAM);
     return HTC_ERROR_INIT_PARAM;
  }

  HTC_RetCode retValue = HTC_OK;
  int len = startRowID.size();
  const char * startRowidData = startRowID.data();
  const char * stopRowidData = stopRowID.data();
  Text encStartRowId;
  Text encStopRowId;
  if (encryptionInfo && 
      ComEncryption::isFlagSet(*(ComEncryption::EncryptionInfo*)encryptionInfo,
                               ComEncryption::ROWID_ENCRYPT)) {
    encryptRowId(encryptionInfo, NULL, 1, startRowidData, len, encStartRowId);
    
    len = encStartRowId.size();
    startRowidData = encStartRowId.data();
  }

  jstring js_tmp_loc = NULL;
  jstring js_snapName = NULL;
  jstring js_hbaseAuths = NULL;
  jstring js_QueryContext = NULL;
  jobjectArray j_cols = NULL;
  jbyteArray jba_stopRowID = NULL;
  jobjectArray j_colnamestofilter = NULL;
  jobjectArray j_compareoplist = NULL; 
  jobjectArray j_colvaluestocompare = NULL;  
  jbyteArray jba_startRowID = jenv_->NewByteArray(len);
  CHECK_RESULT_AND_SET_RETURN_ERROR(jba_startRowID, startRowidData, (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_SCANOPEN_PARAM);
  jenv_->SetByteArrayRegion(jba_startRowID, 0, len, (const jbyte*)startRowidData);

  len = stopRowID.size();
  if (encryptionInfo && 
      ComEncryption::isFlagSet(*(ComEncryption::EncryptionInfo*)encryptionInfo,
                               ComEncryption::ROWID_ENCRYPT)) {
    encryptRowId(encryptionInfo, NULL, 1, stopRowidData, len, encStopRowId);
    
    len = encStopRowId.size();
    stopRowidData = encStopRowId.data();
  }

  jba_stopRowID = jenv_->NewByteArray(len);
  CHECK_RESULT_AND_SET_RETURN_ERROR(jba_stopRowID, stopRowidData, (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_SCANOPEN_PARAM);
  jenv_->SetByteArrayRegion(jba_stopRowID, 0, len, (const jbyte*)stopRowidData);

  if (!cols.isEmpty())
  {
    j_cols = convertToByteArrayObjectArray(cols);
    CHECK_RESULT_AND_RETURN_ERROR(j_cols, tableName_, HTC_ERROR_SCANOPEN_PARAM);
    numColsInScan_ = cols.entries();
  }
  else
     numColsInScan_ = 0;

  {
    jlong j_tid = transID;
    jlong j_sid = savepointID;
    jlong j_psid = pSavepointId;
    jint j_isolationLevel = isolationLevel;
    jint j_lockMode = lockMode;
    jlong j_ts = timestamp;

    jboolean j_cb = cacheBlocks;
    jboolean j_firstReadBypassTm = firstReadBypassTm;
    jboolean j_smallScanner = smallScanner;
    jboolean j_preFetch = preFetch;
    jint j_ncr = numCacheRows;
    numReqRows_ = numCacheRows;
    currentRowNum_ = -1;
    currentRowCellNum_ = -1;

    if ((inColNamesToFilter) && (!inColNamesToFilter->isEmpty()))
    {
      j_colnamestofilter = convertToByteArrayObjectArray(*inColNamesToFilter);
      CHECK_RESULT_AND_RETURN_ERROR(j_colnamestofilter, tableName_, HTC_ERROR_SCANOPEN_PARAM);
    }

    if ((inCompareOpList) && (! inCompareOpList->isEmpty()))
    {
      j_compareoplist = convertToByteArrayObjectArray(*inCompareOpList);
      CHECK_RESULT_AND_RETURN_ERROR(j_compareoplist, tableName_, HTC_ERROR_SCANOPEN_PARAM);
    }

    if ((inColValuesToCompare) && (!inColValuesToCompare->isEmpty()))
    {
      j_colvaluestocompare = convertToByteArrayObjectArray(*inColValuesToCompare);
      CHECK_RESULT_AND_RETURN_ERROR(j_colvaluestocompare, tableName_, HTC_ERROR_SCANOPEN_PARAM);
    }
    jfloat j_dopParallelScanner = dopParallelScanner;
    jfloat j_smplPct = samplePercent;
    jboolean j_useSnapshotScan = useSnapshotScan;
    jint j_snapTimeout = snapTimeout;
    jint j_espNum = espNum;
    jint j_versions = versions;
    int j_replicaId = 0;

    jlong j_minTS = minTS;  
    jlong j_maxTS = maxTS;  
 
    jboolean jresult = 0;
    char* queryContext = getClientInfoFromContext();
    js_snapName = jenv_->NewStringUTF(snapName != NULL ? snapName : "Dummy");
    CHECK_RESULT_AND_SET_RETURN_ERROR(js_snapName, snapName, (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_SCANOPEN_PARAM);

    js_tmp_loc = jenv_->NewStringUTF(tmpLoc != NULL ? tmpLoc : "Dummy");
    CHECK_RESULT_AND_SET_RETURN_ERROR(js_tmp_loc, tmpLoc, (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_SCANOPEN_PARAM);
  
    if (hbaseAuths)
    {
      js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
      CHECK_RESULT_AND_SET_RETURN_ERROR(js_hbaseAuths, hbaseAuths, (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_SCANOPEN_PARAM);
    }

    if (queryContext != NULL) {
      js_QueryContext = jenv_->NewStringUTF(queryContext);
    } else {
      js_QueryContext = jenv_->NewStringUTF("");
    }
    CHECK_RESULT_AND_SET_RETURN_ERROR(js_QueryContext, queryContext, (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_SCANOPEN_PARAM);
    j_replicaId = HBaseClient_JNI::getReplicaId(numReplications); 
    if (hbs_)
        hbs_->getHbaseTimer().start();

    tsRecentJMFromJNI = JavaMethods_[JM_SCAN_OPEN].jm_full_name;
    jresult = jenv_->CallBooleanMethod(
            javaObj_, 
            JavaMethods_[JM_SCAN_OPEN].methodID, 
            j_tid, j_sid, j_psid, j_isolationLevel,
            jba_startRowID, jba_stopRowID, j_cols, j_ts, j_cb, j_smallScanner, j_ncr,
            j_colnamestofilter, j_compareoplist, j_colvaluestocompare, 
            j_dopParallelScanner,
            j_smplPct, j_preFetch, j_lockMode, (jboolean)skipReadConflict, (jboolean)skipTransaction,
            j_useSnapshotScan, j_snapTimeout, js_snapName, js_tmp_loc, j_espNum,
            j_versions, j_minTS, j_maxTS,
            js_hbaseAuths, j_replicaId, false, j_firstReadBypassTm, js_QueryContext);

    if (hbs_)
    {
      hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
      hbs_->incHbaseCalls();
    }

    CHECK_EXCEPTION_AND_RETURN_LOCK_ERROR(tableName_, HTC_ERROR_SCANOPEN_EXCEPTION);
    CHECK_AND_REPORT_ERROR(!jresult, HTC_ERROR_SCANOPEN_EXCEPTION);
  }
  fetchMode_ = SCAN_FETCH;

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(jba_startRowID);
  CHECK_AND_DELETE_NEWOBJ(jba_stopRowID);
  CHECK_AND_DELETE_NEWOBJ(j_cols);
  CHECK_AND_DELETE_NEWOBJ(j_colnamestofilter);
  CHECK_AND_DELETE_NEWOBJ(j_compareoplist);
  CHECK_AND_DELETE_NEWOBJ(j_colvaluestocompare);
  CHECK_AND_DELETE_NEWOBJ(js_snapName);
  CHECK_AND_DELETE_NEWOBJ(js_tmp_loc);
  CHECK_AND_DELETE_NEWOBJ(js_hbaseAuths);
  CHECK_AND_DELETE_NEWOBJ(js_QueryContext);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HTC_RetCode HTableClient_JNI::deleteRow(Int64 transID, Int64 savepointID, Int64 pSavepointId,
                                        HbaseStr &rowID, const LIST(HbaseStr) *cols, Int64 timestamp,
                                        const char * hbaseAuths,
                                        const char * encryptionInfo)
{
  FUNCTION_DEBUG("%s(%ld, %s) called.", __FUNCTION__, transID, rowID.val);
  if (initJNIEnv() != JOI_OK)
  {
     EXIT_FUNCTION(HTC_ERROR_INIT_PARAM);
     return HTC_ERROR_INIT_PARAM;
  }

  HTC_RetCode retValue = HTC_OK;
  int flags = 0;
  jboolean jresult = 0;
  jlong j_tid = transID;  
  jlong j_sid = savepointID;
  jlong j_psid = pSavepointId;
  jlong j_ts = timestamp;
  jstring js_hbaseAuths = NULL;
  jstring js_QueryContext = NULL;
  char* queryContext = getClientInfoFromContext();
  jobjectArray j_cols = NULL;
  jbyteArray jba_rowID = jenv_->NewByteArray(rowID.len);
  CHECK_RESULT_AND_SET_RETURN_ERROR(jba_rowID, rowID.val, (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_DELETEROW_PARAM);
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowID.len, (const jbyte*)rowID.val);
  if (cols != NULL && !cols->isEmpty())
  {
     j_cols = convertToByteArrayObjectArray(*cols);
     CHECK_RESULT_AND_RETURN_ERROR(j_cols, tableName_, HTC_ERROR_DELETEROW_PARAM);
  }  
  if (hbaseAuths)
    js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
  else
    js_hbaseAuths = jenv_->NewStringUTF("");
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_hbaseAuths, hbaseAuths, (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_DELETEROW_PARAM);

  if (queryContext != NULL) {
    js_QueryContext = jenv_->NewStringUTF(queryContext);
  } else {
    js_QueryContext = jenv_->NewStringUTF("");
  }

  CHECK_RESULT_AND_SET_RETURN_ERROR(js_QueryContext, queryContext, (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_DELETEROW_PARAM);

  if (hbs_)
    hbs_->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_DELETE].jm_full_name;
  jresult = jenv_->CallBooleanMethod(javaObj_, 
                                     JavaMethods_[JM_DELETE].methodID, j_tid, j_sid, j_psid,
                                     jba_rowID, j_cols, j_ts, 
                                     js_hbaseAuths, (jint)flags, js_QueryContext);
  if (hbs_)
    {
      hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
      hbs_->incHbaseCalls();
    }

  CHECK_EXCEPTION_AND_RETURN_LOCK_ERROR(tableName_, HTC_ERROR_DELETEROW_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, HTC_ERROR_DELETEROW_EXCEPTION);
  if (hbs_)
    hbs_->incBytesRead(rowID.len);

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(jba_rowID);
  CHECK_AND_DELETE_NEWOBJ(j_cols);
  CHECK_AND_DELETE_NEWOBJ(js_hbaseAuths);
  CHECK_AND_DELETE_NEWOBJ(js_QueryContext);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
HTC_RetCode HTableClient_JNI::setWriteBufferSize(Int64 size)
{
  ENTRY_FUNCTION();

  if (initJNIEnv() != JOI_OK)
  {
     EXIT_FUNCTION(HTC_ERROR_INIT_PARAM);
     return HTC_ERROR_INIT_PARAM;
  }

  HTC_RetCode retValue = HTC_OK;
  jlong j_size = size;
  jboolean jresult = 0;

  tsRecentJMFromJNI = JavaMethods_[JM_SET_WB_SIZE].jm_full_name;
  jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_SET_WB_SIZE].methodID, j_size);
  CHECK_EXCEPTION_AND_RETURN_ERROR(tableName_, HTC_ERROR_WRITEBUFFERSIZE_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, HTC_ERROR_WRITEBUFFERSIZE_EXCEPTION);

errorLabel:

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

HTC_RetCode HTableClient_JNI::setWriteToWAL(bool WAL)
{
  ENTRY_FUNCTION();
  if (initJNIEnv() != JOI_OK)
  {
     EXIT_FUNCTION(HTC_ERROR_INIT_PARAM);
     return HTC_ERROR_INIT_PARAM;
  }

  HTC_RetCode retValue = HTC_OK;
  jboolean j_WAL = WAL;
  jboolean jresult = 0;

  tsRecentJMFromJNI = JavaMethods_[JM_SET_WRITE_TO_WAL].jm_full_name;
  jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_SET_WRITE_TO_WAL].methodID, j_WAL);
  CHECK_EXCEPTION_AND_RETURN_ERROR(tableName_, HTC_ERROR_WRITETOWAL_EXCEPTION);
  CHECK_AND_REPORT_ERROR(!jresult, HTC_ERROR_WRITETOWAL_EXCEPTION);

errorLabel:
  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
const char *HTableClient_JNI::getTableName()
{
  return tableName_;
}

HTC_RetCode HTableClient_JNI::coProcAggr(Int64 transID,
					 Int64 svptId,
					 Int64 pSvptId,
                                         int isolationLevel, 
                                         int lockMode, 
					 int aggrType, // 0:count, 1:min, 2:max, 3:sum, 4:avg
					 const Text& startRowID, 
					 const Text& stopRowID, 
					 const Text &colFamily,
					 const Text &colName,
					 const NABoolean cacheBlocks,
					 const Lng32 numCacheRows,
					 Text &aggrVal) // returned value
{
  ENTRY_FUNCTION();
  if (initJNIEnv() != JOI_OK)
  {
     EXIT_FUNCTION(HTC_ERROR_INIT_PARAM);
     return HTC_ERROR_INIT_PARAM;
  }

  HTC_RetCode retValue = HTC_OK;
  int len = 0;
  jbyteArray jba_startrowid = NULL;
  jbyteArray jba_stoprowid = NULL;
  jbyteArray jba_colfamily = NULL;
  jbyteArray jba_colname = NULL;
  jstring js_QueryContext = NULL;
  char* queryContext = NULL;

  len = startRowID.size();
  jba_startrowid = jenv_->NewByteArray(len);
  CHECK_RESULT_AND_SET_RETURN_ERROR(jba_startrowid, startRowID.data(), (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_COPROC_AGGR_PARAM);
  jenv_->SetByteArrayRegion(jba_startrowid, 0, len, (const jbyte*)startRowID.data());

  len = stopRowID.size();
  jba_stoprowid = jenv_->NewByteArray(len);
  CHECK_RESULT_AND_SET_RETURN_ERROR(jba_stoprowid, stopRowID.data(), (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_COPROC_AGGR_PARAM);
  jenv_->SetByteArrayRegion(jba_stoprowid, 0, len, (const jbyte*)stopRowID.data());
 
  len = colFamily.size();
  jba_colfamily = jenv_->NewByteArray(len);
  CHECK_RESULT_AND_SET_RETURN_ERROR(jba_colfamily, colFamily.data(), (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_COPROC_AGGR_PARAM);
  jenv_->SetByteArrayRegion(jba_colfamily, 0, len, (const jbyte*)colFamily.data());
 
  len = colName.size();
  jba_colname = jenv_->NewByteArray(len);
  CHECK_RESULT_AND_SET_RETURN_ERROR(jba_colname, colName.data(), (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_COPROC_AGGR_PARAM);
  jenv_->SetByteArrayRegion(jba_colname, 0, len, (const jbyte*)colName.data());

  queryContext = getClientInfoFromContext();
  if (queryContext != NULL) {
      js_QueryContext = jenv_->NewStringUTF(queryContext);
  } else {
      js_QueryContext = jenv_->NewStringUTF("");
  }
  CHECK_RESULT_AND_SET_RETURN_ERROR(js_QueryContext, queryContext, (HTC_RetCode)JOI_ERROR_NEWOBJ, HTC_ERROR_COPROC_AGGR_PARAM);

  {
    jarray jresult = 0;
    Text *val = NULL;
    jlong j_tid = transID;
    jlong j_svptId = svptId;
    jlong j_pSvptId = pSvptId;
    jint j_aggrtype = aggrType;
    jint j_isolationLevel = isolationLevel;
    jint j_lockMode = lockMode;

    jboolean j_cb = cacheBlocks;
    jint j_ncr = numCacheRows;

    if (hbs_)
        hbs_->getHbaseTimer().start();
    tsRecentJMFromJNI = JavaMethods_[JM_COPROC_AGGR].jm_full_name;
    jresult = (jarray)jenv_->CallObjectMethod(javaObj_, 
            JavaMethods_[JM_COPROC_AGGR].methodID, j_tid, j_svptId, j_pSvptId, j_isolationLevel,
            j_lockMode, j_aggrtype, jba_startrowid, jba_stoprowid, jba_colfamily,
            jba_colname, j_cb, j_ncr, js_QueryContext);
    if (hbs_)
    {
      hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
      hbs_->incHbaseCalls();
    }

    CHECK_EXCEPTION_AND_RETURN_LOCK_ERROR(tableName_, HTC_ERROR_COPROC_AGGR_EXCEPTION);

    if (jresult != NULL)
    {
      jbyte *result = jenv_->GetByteArrayElements((jbyteArray)jresult, NULL);
      int len = jenv_->GetArrayLength(jresult);
      val = new (heap_) Text((char *)result, len);
      jenv_->ReleaseByteArrayElements((jbyteArray)jresult, result, JNI_ABORT);
    }
    CHECK_RESULT_AND_SET_RETURN_ERROR(val, tableName_, HTC_ERROR_COPROC_AGGR_PARAM, HTC_ERROR_COPROC_AGGR_PARAM);
    aggrVal = *val;
  }

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(jba_startrowid);
  CHECK_AND_DELETE_NEWOBJ(jba_stoprowid);
  CHECK_AND_DELETE_NEWOBJ(jba_colfamily);
  CHECK_AND_DELETE_NEWOBJ(jba_colname);
  CHECK_AND_DELETE_NEWOBJ(js_QueryContext);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}

#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jint JNICALL Java_org_trafodion_sql_HTableClient_setResultInfo
  (JNIEnv *jenv, jobject jobj, jlong jniObject, 
   jintArray jKvValLen, jintArray jKvValOffset, 
   jintArray jKvQualLen, jintArray jKvQualOffset,
   jintArray jKvFamLen, jintArray jKvFamOffset, 
   jlongArray jTimestamp, 
   jobjectArray jKvBuffer, jobjectArray jKvTag, 
   jobjectArray jKvFamArray, jobjectArray jkvQualArray, jobjectArray jRowIDs,
   jintArray jKvsPerRow, jint numCellsReturned, jint numRowsReturned)
{
   HTableClient_JNI *htc = (HTableClient_JNI *)jniObject;
   if (htc->getFetchMode() == HTableClient_JNI::GET_ROW ||
          htc->getFetchMode() == HTableClient_JNI::BATCH_GET)
      htc->setJavaObject(jobj);
   htc->setResultInfo(jKvValLen, jKvValOffset,
                jKvQualLen, jKvQualOffset, jKvFamLen, jKvFamOffset,
                jTimestamp, jKvBuffer, jKvTag, jKvFamArray, jkvQualArray,
                jRowIDs, jKvsPerRow, numCellsReturned, numRowsReturned);  
   return 0;
}

JNIEXPORT jint JNICALL Java_org_trafodion_sql_HTableClient_setJavaObject
  (JNIEnv *jenv, jobject jobj, jlong jniObject)
{
   HTableClient_JNI *htc = (HTableClient_JNI *)jniObject;
   htc->setJavaObject(jobj);
   return 0;
}

JNIEXPORT void JNICALL Java_org_trafodion_sql_HTableClient_cleanup
  (JNIEnv *jenv, jobject jobj, jlong jniObject)
{
   HTableClient_JNI *htc = (HTableClient_JNI *)jniObject;
   NADELETE(htc, HTableClient_JNI, htc->getHeap()); 
}

#ifdef __cplusplus
}
#endif

void HTableClient_JNI::setResultInfo( jintArray jKvValLen, jintArray jKvValOffset,
                                      jintArray jKvQualLen, jintArray jKvQualOffset,
                                      jintArray jKvFamLen, jintArray jKvFamOffset,
                                      jlongArray jTimestamp, 
                                      jobjectArray jKvBuffer, jobjectArray jKvTag,
                                      jobjectArray jKvFamArray, jobjectArray jKvQualArray, jobjectArray jRowIDs,
                                      jintArray jKvsPerRow, jint numCellsReturned, jint numRowsReturned)
{
   if (numRowsReturned_ > 0)
      cleanupResultInfo();
   NABoolean exceptionFound = FALSE;
   if (numCellsReturned != 0) {
     CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jKvValLen_, jintArray, jKvValLen);
     CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jKvValOffset_, jintArray, jKvValOffset);
     CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jKvQualLen_, jintArray, jKvQualLen);
     CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jKvQualOffset_, jintArray, jKvQualOffset);
     CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jKvFamLen_, jintArray, jKvFamLen);
     CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jKvFamOffset_, jintArray, jKvFamOffset);
     CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jTimestamp_, jlongArray, jTimestamp);
     CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jKvBuffer_, jobjectArray, jKvBuffer);
     CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jKvTag_, jobjectArray, jKvTag);
     CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jKvFamArray_, jobjectArray, jKvFamArray);
     CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jKvQualArray_, jobjectArray, jKvQualArray);
   }

   CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jRowIDs_, jobjectArray, jRowIDs);
   CHECK_AND_NEW_GLOBAL_REF(exceptionFound, jKvsPerRow_, jintArray, jKvsPerRow);
   numCellsReturned_ = numCellsReturned;
   numRowsReturned_ = numRowsReturned;
   prevRowCellNum_ = 0;
   currentRowNum_ = -1;
   cleanupDone_ = FALSE;
   ex_assert(! exceptionFound, "Exception in HTableClient_JNI::setResultInfo");

   if (getUseTrigger())
     {
       HbaseStr rowID;
       rowID.val = NULL;
       rowID.len = 0;
       setOldValueInfo(rowID);
     }
   return;
} 

void HTableClient_JNI::cleanupResultInfo()
{
   if (cleanupDone_)
      return;
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jKvValLen_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jKvValOffset_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jKvQualLen_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jKvQualOffset_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jKvFamLen_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jKvFamOffset_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jTimestamp_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jKvBuffer_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jKvTag_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jKvFamArray_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jKvQualArray_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jRowIDs_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jba_kvBuffer_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jba_kvFamArray_);
   CHECK_AND_DELETE_GLOBAL_NEWOBJ(jba_kvQualArray_);

   if (p_rowID_ != NULL)
   {
      jenv_->ReleaseByteArrayElements(jba_rowID_, p_rowID_, JNI_ABORT);
      p_rowID_ = NULL;
      CHECK_AND_DELETE_GLOBAL_NEWOBJ(jba_rowID_);
   }
   if (p_kvsPerRow_ != NULL)
   {
      jenv_->ReleaseIntArrayElements(jKvsPerRow_, p_kvsPerRow_, JNI_ABORT);
      p_kvsPerRow_ = NULL;
      CHECK_AND_DELETE_GLOBAL_NEWOBJ(jKvsPerRow_);
   }

   cleanupOldValueInfo();

   cleanupDone_ = TRUE;
   return;
}

void HTableClient_JNI::setOldValueInfo(HbaseStr rowID)
{
  cleanupOldValueInfo();
  
  int i = 0;
  int j = 0;
  int rowIDLen;
  int cellNum = 0;  
  int tmpkvValLen = 0;
  unsigned char * rowVal = NULL;
  jbyte *rowId;

  NAString srowIDsLen;
  NAString sBufSize;

  jobject rowIDObj;
  jbyteArray jba_rowID = NULL;  
  jobject kvBufferObj;
  jbyteArray jba_kvBuffer = NULL;

  if (numCellsReturned_ == 0 || jKvValLen_ == NULL ||
      jKvValOffset_ == NULL)
    return;
  
  jint *kvsPerRow = jenv_->GetIntArrayElements(jKvsPerRow_, NULL);
  jint *kvValLen = new(heap_) jint[numCellsReturned_];
  jenv_->GetIntArrayRegion(jKvValLen_, 0, numCellsReturned_, kvValLen);
  jint *jKvValOffset = new(heap_) jint[numCellsReturned_];
  jenv_->GetIntArrayRegion(jKvValOffset_, 0, numCellsReturned_, jKvValOffset);

  char sz[10];
  // if only get one row data, set numRows is 1
  if (rowID.len != 0)
    {
      itoa(1 ,sz, 10);
    }
  else
    {
      itoa(numRowsReturned_ ,sz, 10);
    }
  
  sBufSize.append(sz);
  sBufSize.append(",");

  char *szTemp = NULL;
  int maxRowIDSize = 8 * numRowsReturned_ + 1;  //default rowisize is 8
  char *szRowIDs = new(heap_) char[maxRowIDSize];
  int curUseRowIDSize = 0;

  int maxRowSize = 0;
  int curUseRowSize = 0;
  char *szBuffers = NULL;

  NABoolean FindRow = FALSE;
  for (i =0; i < numRowsReturned_; i++)
    {
      if (FindRow)
	break;
      
      if (kvsPerRow[i] < 1)
	continue;
      rowIDObj = jenv_->GetObjectArrayElement(jRowIDs_, i);
      jba_rowID = (jbyteArray)jenv_->NewLocalRef(rowIDObj);
      jenv_->DeleteLocalRef(rowIDObj);
      if (jba_rowID == NULL)
	continue;
      rowId = jenv_->GetByteArrayElements(jba_rowID, NULL);
      rowIDLen = jenv_->GetArrayLength(jba_rowID);
      itoa(rowIDLen ,sz, 10);
      if (rowID.len != 0)
	{
	  if (rowID.len != rowIDLen ||
	      memcmp(rowID.val, (char*)rowId, rowIDLen) != 0)
	    {
	      cellNum += kvsPerRow[i];
	      continue;
	    }
	  else
	    FindRow = TRUE;
	}
      
      srowIDsLen.append(sz);
      if (i < numRowsReturned_ - 1)
	{
	  srowIDsLen.append(",");
	}

      if ((maxRowIDSize < curUseRowIDSize + rowIDLen) &&
	  szRowIDs != NULL)
	{
	  if (curUseRowIDSize > 0)
	    {
	      szTemp = new(heap_) char[maxRowIDSize];
	      str_cpy_all(szTemp, szRowIDs, curUseRowIDSize);
	    }

	  while(1)
	    {
	      maxRowIDSize = 2 * maxRowIDSize;
	      if (maxRowIDSize > curUseRowIDSize + rowIDLen)
		break;
	    }
	  NADELETEBASIC(szRowIDs, heap_);
	  szRowIDs = new(heap_) char[maxRowIDSize];
	  if (szTemp != NULL)
	    {
	      str_cpy_all(szRowIDs, szTemp, curUseRowIDSize);
	      NADELETEBASIC(szTemp, heap_);
	    }
	}

      str_cpy_all(szRowIDs + curUseRowIDSize, (const char*)rowId, rowIDLen);
      curUseRowIDSize += rowIDLen;
      
      jenv_->ReleaseByteArrayElements((jbyteArray)jba_rowID, rowId, JNI_ABORT);
      jenv_->DeleteLocalRef(jba_rowID);
      
      for (j = 0; j < kvsPerRow[i]; j++)
	{
	  kvBufferObj = jenv_->GetObjectArrayElement(jKvBuffer_, cellNum);
	  jba_kvBuffer = (jbyteArray)jenv_->NewLocalRef(kvBufferObj);
	  jenv_->DeleteLocalRef(kvBufferObj);

	  if (tmpkvValLen < kvValLen[cellNum])
	    {
	      if (rowVal != NULL)
	  	{
	  	  NADELETEBASIC(rowVal, heap_);
	  	}
	      rowVal = new(heap_) BYTE[kvValLen[cellNum]];
	      tmpkvValLen = kvValLen[cellNum];
	    }
	  jenv_->GetByteArrayRegion(jba_kvBuffer, jKvValOffset[cellNum], kvValLen[cellNum],(jbyte *)rowVal);

	  if (maxRowSize == 0)
	    {
	      maxRowSize = numRowsReturned_ * kvsPerRow[i] * kvValLen[cellNum] + 1;
	      szBuffers = new(heap_) char[maxRowSize];
	    }
	  else if (maxRowSize < curUseRowSize + kvValLen[cellNum])
	    {
	      szTemp = new(heap_) char[maxRowSize];
	      str_cpy_all(szTemp, szBuffers, curUseRowSize);
	      while(1)
		{
		  maxRowSize = 2 * maxRowSize;
		  if (maxRowSize > curUseRowSize + kvValLen[cellNum])
		    break;
		}
	      NADELETEBASIC(szBuffers, heap_);
	      szBuffers = new(heap_) char[maxRowSize];
	      str_cpy_all(szBuffers, szTemp, curUseRowSize);
	      NADELETEBASIC(szTemp, heap_);
	    }
	  str_cpy_all(szBuffers + curUseRowSize, (char*)rowVal, kvValLen[cellNum]);
	  curUseRowSize += kvValLen[cellNum];
	  
	  jenv_->DeleteGlobalRef(jba_kvBuffer);
	  
	  if (j == 0)
	    {
	      itoa(kvsPerRow[j] ,sz, 10);
	      sBufSize.append(sz);
	      sBufSize.append(",");
	    }
	  itoa(kvValLen[cellNum] ,sz, 10);
	  sBufSize.append(sz);
	  if (cellNum < numCellsReturned_ - 1)
	    {
	      sBufSize.append(",");
	    }
	  cellNum++;
	}
    }
  jenv_->ReleaseIntArrayElements(jKvsPerRow_, kvsPerRow, JNI_ABORT);
  NADELETEBASIC(kvValLen, heap_);
  NADELETEBASIC(jKvValOffset, heap_);
  if (rowVal != NULL)
    {
      NADELETEBASIC(rowVal, heap_);
    }

  unsigned char* base64rowVal = NULL;
  unsigned char* base64rowIDVal = NULL;

  base64rowVal = new (heap_) unsigned char[3 * curUseRowSize + 1];
  int base64ValLen = EVP_EncodeBlock(base64rowVal, (unsigned char *)szBuffers, curUseRowSize);
  base64rowIDVal = new (heap_) unsigned char[3 * curUseRowIDSize + 1];
  int base64RowIDLen = EVP_EncodeBlock(base64rowIDVal, (unsigned char *)szRowIDs, curUseRowIDSize);
  skvBuffer_ = new (heap_) char[base64ValLen + 1];
  str_cpy_all(skvBuffer_, (const char*)base64rowVal, base64ValLen + 1);
  sBufSize_ = new (heap_) char[sBufSize.length() + 1];
  str_cpy_all(sBufSize_, sBufSize.data(), sBufSize.length() + 1);
  srowIDs_ = new (heap_) char[base64RowIDLen + 1];
  str_cpy_all(srowIDs_, (const char*)base64rowIDVal, base64RowIDLen + 1);
  srowIDsLen_ = new (heap_) char[srowIDsLen.length() + 1];
  str_cpy_all(srowIDsLen_, srowIDsLen.data(), srowIDsLen.length() + 1);
  if (szBuffers != NULL)
    {
      NADELETEBASIC(szBuffers, heap_);
      szBuffers = NULL;
    }
  if (base64rowVal != NULL)
    {
      NADELETEBASIC(base64rowVal, heap_);
      base64rowVal = NULL;
    }
  if (base64rowIDVal != NULL)
    {
      NADELETEBASIC(base64rowIDVal, heap_);
      base64rowIDVal = NULL;
    }
  if (szRowIDs != NULL)
    {
      NADELETEBASIC(szRowIDs, heap_);
      szRowIDs = NULL;
    }
}

void HTableClient_JNI::cleanupOldValueInfo()
{
  if (skvBuffer_ != NULL)
    {
      NADELETEBASIC(skvBuffer_, heap_);
      skvBuffer_ = NULL;
    }
  if (sBufSize_ != NULL)
    {
      NADELETEBASIC(sBufSize_, heap_);
      sBufSize_ = NULL;
    }
  if (srowIDs_ != NULL)
    {
      NADELETEBASIC(srowIDs_, heap_);
      srowIDs_ = NULL;
    }
  if (srowIDsLen_ != NULL)
    {
      NADELETEBASIC(srowIDsLen_, heap_);
      srowIDsLen_ = NULL;
    }
}

void HBaseClient_JNI::setOldValueInfo(char *skvBuffer,
				      char *sbufsize,
				      char *srowIDs,
				      char *srowIDsLen)
{
  long sz = 0;
  cleanupOldValueInfo();

  if (skvBuffer != NULL)
    {
      sz = strlen(skvBuffer);
      skvBuffer_ = new (heap_) char[sz + 1];
      str_cpy_all(skvBuffer_, skvBuffer, sz + 1);
    }

  if (sbufsize != NULL)
    {
      sz = strlen(sbufsize);
      sBufSize_ = new (heap_) char[sz + 1];
      str_cpy_all(sBufSize_, sbufsize, sz + 1);
    }

  if (srowIDs != NULL)
    {
      sz = strlen(srowIDs);
      srowIDs_ = new (heap_) char[sz + 1];
      str_cpy_all(srowIDs_, srowIDs, sz + 1);
    }

  if (srowIDsLen != NULL)
    {
      sz = strlen(srowIDsLen);
      srowIDsLen_ = new (heap_) char[sz + 1];
      str_cpy_all(srowIDsLen_, srowIDsLen, sz + 1);
    }
}

void HBaseClient_JNI::cleanupOldValueInfo()
{
  if (skvBuffer_ != NULL)
    {
      NADELETEBASIC(skvBuffer_, heap_);
      skvBuffer_ = NULL;
    }
  if (sBufSize_ != NULL)
    {
      NADELETEBASIC(sBufSize_, heap_);
      sBufSize_ = NULL;
    }
  if (srowIDs_ != NULL)
    {
      NADELETEBASIC(srowIDs_, heap_);
      srowIDs_ = NULL;
    }
  if (srowIDsLen_ != NULL)
    {
      NADELETEBASIC(srowIDsLen_, heap_);
      srowIDsLen_ = NULL;
    }
}

HTC_RetCode HTableClient_JNI::nextRow(ExDDLValidator * ddlValidator)
{
    HTC_RetCode retCode;

    ex_assert(fetchMode_ != UNKNOWN, "invalid fetchMode_");
    switch (fetchMode_) {
       case GET_ROW:
          if (numRowsReturned_ == -1)
             return HTC_DONE;
          if (currentRowNum_ == -1)
          {
             getResultInfo();
             return HTC_OK;
	  }
          else
          {
             cleanupResultInfo();   
             return HTC_DONE;
          }
          break;
       case BATCH_GET:
          if (numRowsReturned_ == -1)
             return HTC_DONE_RESULT;
          if (currentRowNum_ == -1)
          {
             getResultInfo();
             return HTC_OK;
          }
          else
          if ((currentRowNum_+1) >= numRowsReturned_)
          {
             cleanupResultInfo();   
             return HTC_DONE_RESULT;
          }
          break;
       default:
          break;
    }
    if (fetchMode_ == SCAN_FETCH && (currentRowNum_ == -1 || ((currentRowNum_+1) >= numRowsReturned_)))
    {
        if (currentRowNum_ != -1 && (numRowsReturned_ < numReqRows_))
        {
            cleanupResultInfo();
            return HTC_DONE;
        }   
        if (ddlValidator && !ddlValidator->isDDLValidForReads())
        {
            cleanupResultInfo();
            return HTC_ERROR_FETCHROWS_INVALID_DDL;
        }
        retCode = fetchRows();
        if (retCode != HTC_OK)
        {
           cleanupResultInfo();
           return retCode;
        }
        getResultInfo();
    }
    else
    {
        // Add the number of previous cells returned
        jint kvsPerRow = p_kvsPerRow_[currentRowNum_];
        prevRowCellNum_ += kvsPerRow;  
        currentRowNum_++;
        currentRowCellNum_ = 0;
    }
    // clean the rowID of the previous row
    if (p_rowID_ != NULL)
    {
       jenv_->ReleaseByteArrayElements(jba_rowID_, p_rowID_, JNI_ABORT);
       p_rowID_ = NULL;
       jenv_->DeleteGlobalRef(jba_rowID_);
    }
    return HTC_OK;
}

void HTableClient_JNI::getResultInfo()
{
   // Allocate Buffer and copy the cell info
   int numCellsNeeded;
   if (numCellsReturned_ == 0)
   {
      p_kvsPerRow_ = jenv_->GetIntArrayElements(jKvsPerRow_, NULL);
      currentRowNum_ = 0;
      currentRowCellNum_ = 0;
      prevRowCellNum_ = 0;
      return;
   }
   if (numCellsAllocated_ == 0 || 
		numCellsAllocated_ < numCellsReturned_) {
      NAHeap *heap = getHeap();
      if (numCellsAllocated_ > 0) {
          NADELETEBASIC(p_kvValLen_, heap);
          NADELETEBASIC(p_kvValOffset_, heap);
          NADELETEBASIC(p_kvFamLen_, heap);
          NADELETEBASIC(p_kvFamOffset_, heap);
          NADELETEBASIC(p_kvQualLen_, heap);
          NADELETEBASIC(p_kvQualOffset_, heap);
          NADELETEBASIC(p_timestamp_, heap);
          numCellsNeeded = numCellsReturned_;
       }
       else {  
          if (numColsInScan_ == 0)
              numCellsNeeded = numCellsReturned_;
          else    
              numCellsNeeded = 2 * numReqRows_ * numColsInScan_;
       }
       p_kvValLen_ = new (heap) jint[numCellsNeeded];
       p_kvValOffset_ = new (heap) jint[numCellsNeeded];
       p_kvFamLen_ = new (heap) jint[numCellsNeeded];
       p_kvFamOffset_ = new (heap) jint[numCellsNeeded];
       p_kvQualLen_ = new (heap) jint[numCellsNeeded];
       p_kvQualOffset_ = new (heap) jint[numCellsNeeded];
       p_timestamp_ = new (heap) jlong[numCellsNeeded];
       numCellsAllocated_ = numCellsNeeded;
    }
    jenv_->GetIntArrayRegion(jKvValLen_, 0, numCellsReturned_, p_kvValLen_);
    jenv_->GetIntArrayRegion(jKvValOffset_, 0, numCellsReturned_, p_kvValOffset_);
    jenv_->GetIntArrayRegion(jKvQualLen_, 0, numCellsReturned_, p_kvQualLen_);
    jenv_->GetIntArrayRegion(jKvQualOffset_, 0, numCellsReturned_, p_kvQualOffset_);
    jenv_->GetIntArrayRegion(jKvFamLen_, 0, numCellsReturned_, p_kvFamLen_);
    jenv_->GetIntArrayRegion(jKvFamOffset_, 0, numCellsReturned_, p_kvFamOffset_);
    jenv_->GetLongArrayRegion(jTimestamp_, 0, numCellsReturned_, p_timestamp_);
    p_kvsPerRow_ = jenv_->GetIntArrayElements(jKvsPerRow_, NULL);
    currentRowNum_ = 0;
    currentRowCellNum_ = 0;
    prevRowCellNum_ = 0;
}

HTC_RetCode HTableClient_JNI::prepareForNextCell(int idx)
{
    ENTRY_FUNCTION();

    HTC_RetCode retValue = HTC_OK;
    jobject kvBufferObj = NULL;

    CHECK_AND_DELETE_GLOBAL_NEWOBJ(jba_kvFamArray_);
    kvBufferObj = jenv_->GetObjectArrayElement(jKvFamArray_, idx);
    CHECK_EXCEPTION_AND_RETURN_ERROR(tableName_, HTC_PREPARE_FOR_NEXTCELL_EXCEPTION);

    jba_kvFamArray_ = (jbyteArray)jenv_->NewGlobalRef(kvBufferObj);
    CHECK_EXCEPTION_AND_RETURN_ERROR(tableName_, HTC_PREPARE_FOR_NEXTCELL_EXCEPTION);
    CHECK_AND_DELETE_NEWOBJ(kvBufferObj);

    CHECK_AND_DELETE_GLOBAL_NEWOBJ(jba_kvQualArray_);
    kvBufferObj = jenv_->GetObjectArrayElement(jKvQualArray_, idx);
    CHECK_EXCEPTION_AND_RETURN_ERROR(tableName_, HTC_PREPARE_FOR_NEXTCELL_EXCEPTION);

    jba_kvQualArray_ = (jbyteArray)jenv_->NewGlobalRef(kvBufferObj);
    CHECK_EXCEPTION_AND_RETURN_ERROR(tableName_, HTC_PREPARE_FOR_NEXTCELL_EXCEPTION);
    CHECK_AND_DELETE_NEWOBJ(kvBufferObj);

    CHECK_AND_DELETE_GLOBAL_NEWOBJ(jba_kvBuffer_);
    kvBufferObj = jenv_->GetObjectArrayElement(jKvBuffer_, idx);
    CHECK_EXCEPTION_AND_RETURN_ERROR(tableName_, HTC_PREPARE_FOR_NEXTCELL_EXCEPTION);

    jba_kvBuffer_ = (jbyteArray)jenv_->NewGlobalRef(kvBufferObj);
    CHECK_EXCEPTION_AND_RETURN_ERROR(tableName_, HTC_PREPARE_FOR_NEXTCELL_EXCEPTION);

errorLabel:
    CHECK_AND_DELETE_NEWOBJ(kvBufferObj);
    EXIT_FUNCTION(retValue);
    return retValue;
}

HTC_RetCode HTableClient_JNI::getColName(int colNo,
              char **outColName, 
              short &colNameLen,
              Int64 &timestamp)
{
    HTC_RetCode retcode;

    jint kvsPerRow = p_kvsPerRow_[currentRowNum_];
    if (kvsPerRow == 0 || colNo >= kvsPerRow)
    {
       *outColName == NULL;
       timestamp = 0;
       return HTC_OK;
    }
    int idx = prevRowCellNum_ + colNo;
    ex_assert((idx < numCellsReturned_), "Buffer overflow");
    jint kvQualLen = p_kvQualLen_[idx];
    jint kvQualOffset = p_kvQualOffset_[idx];
    jint kvFamLen = p_kvFamLen_[idx];
    jint kvFamOffset = p_kvFamOffset_[idx];

    if ((retcode = prepareForNextCell(idx)) != HTC_OK)
       return retcode;
   
    colNameLen = kvQualLen + kvFamLen + 1; // 1 for ':'
    char * colName;
    if (colNameAllocLen_ == 0  && colNameLen <= INLINE_COLNAME_LEN)
    	colName = inlineColName_;
    else
    {
        if (colNameLen > colNameAllocLen_)
        {
	   if (colNameAllocLen_ != 0)
              NADELETEBASIC(colName_, heap_);
           colName_ = new (heap_) char[colNameLen+1];
           colNameAllocLen_ = colNameLen;
        }
        colName = colName_; 
    }
    jenv_->GetByteArrayRegion(jba_kvFamArray_, kvFamOffset, kvFamLen, 
            (jbyte *)colName);
    colName[kvFamLen] = ':';
    char *temp = colName+ kvFamLen+1;
    jenv_->GetByteArrayRegion(jba_kvQualArray_, kvQualOffset, kvQualLen, 
            (jbyte *)temp);
    timestamp = p_timestamp_[idx];
    *outColName = colName;
    if (hbs_)
      hbs_->incBytesRead(sizeof(timestamp) + colNameLen);
    return HTC_OK; 
}

HTC_RetCode HTableClient_JNI::getColVal(int colNo, BYTE *colVal, 
                                        Lng32 &colValLen, 
                                        NABoolean nullable, BYTE &nullVal,
                                        BYTE *tag, Lng32 &tagLen,
                                        const char * encryptionInfo)
{
    jint kvsPerRow = p_kvsPerRow_[currentRowNum_];
    if (kvsPerRow == 0 || colNo >= kvsPerRow)
       return HTC_GET_COLVAL_EXCEPTION;
    int idx = prevRowCellNum_ + colNo;
    ex_assert((idx < numCellsReturned_), "Buffer overflow");
    jint kvValLen = p_kvValLen_[idx];
    jint kvValOffset = p_kvValOffset_[idx];
    Lng32 copyLen;
    Lng32 dataLen;
    jbyte nullByte;

    // encryption only supported for aligned format.
    // 'nullable' parameter should be FALSE in that case as the null info
    // is embedded inside the aligned format.
    // If nullable param is true with encryption, return error.
    if ((nullable) && (encryptionInfo))
       return HTC_GET_COLVAL_EXCEPTION;

    // If the column is nullable, get the first byte
    // The first byte determines if the column is null(0xff) or not (0)
    int adjust=0;
    if (nullable)
    {
      adjust = 1;
      dataLen = kvValLen - adjust; 
      copyLen = MINOF(dataLen, colValLen);
      jenv_->GetByteArrayRegion(jba_kvBuffer_, kvValOffset, adjust, &nullByte); 
    }
    else 
    {
      dataLen = kvValLen;
      copyLen = MINOF(dataLen, colValLen);
      nullByte = 0;
    }

    // data is encrypted. Decrypt it before returning in colVal.
    // Encrypted value of length kvValLen is first moved from java layer 
    // to a local buffer(encData).
    // It is then decrypted into another local buffer (decData).
    // After it has been decrypted, data is moved to user provided 
    // return buffer(colVal)
    // For optimization, 2 static buffers of size 4K are allocated on stack.
    // If datalen (kvValLen) is greater than that, then buffer is allocated
    // on heap.
    if (encryptionInfo)
      {
         ComEncryption::EncryptionInfo * ei =
          (ComEncryption::EncryptionInfo*)encryptionInfo;
        unsigned char encDataBuffer[ENC_STATIC_BUFFER_LEN];
        unsigned char decDataBuffer[ENC_STATIC_BUFFER_LEN];
        unsigned char * encData = encDataBuffer;
        unsigned char * decData = decDataBuffer;
        NABoolean encDataAlloc = FALSE;
        if (kvValLen > ENC_STATIC_BUFFER_LEN)
          {
            encDataAlloc = TRUE;
            encData = new(getHeap()) BYTE[kvValLen];
            decData = new(getHeap()) BYTE[kvValLen];
          }

        // allocate local space for encrypted data.
        jenv_->GetByteArrayRegion(jba_kvBuffer_, kvValOffset, kvValLen, 
                                  (jbyte *)encData); 
        
        // get the initVec from encData
        unsigned char * initVec = encData;
        short rc = 0;
        Lng32 decryptedValLen = colValLen;
        rc = ComEncryption::decryptData(ei->dataCipherType,
                                        encData + ei->dataInitVecLen, 
                                        kvValLen - ei->dataInitVecLen,
                                        ei->dataKey,
                                        initVec,
                                        decData, decryptedValLen);
        
        if (rc)
          {
            if (encDataAlloc)
              {
                NADELETEBASIC(encData, getHeap());
                NADELETEBASIC(decData, getHeap());
              }

            return HTC_GET_COLVAL_EXCEPTION;
          }

        dataLen = decryptedValLen;
        copyLen = MINOF(dataLen, colValLen);

        str_cpy_all((char *)colVal, (const char *)decData, copyLen);

        if (encDataAlloc)
          {
            NADELETEBASIC(encData, getHeap());
            NADELETEBASIC(decData, getHeap());
          }
       }
    else
      {
        jenv_->GetByteArrayRegion(jba_kvBuffer_, kvValOffset+adjust, copyLen, 
                                  (jbyte *)colVal); 
      }

    nullVal = nullByte;
    if (dataLen > colValLen)
      colValLen = dataLen;
    else
      colValLen = copyLen;
    if (hbs_)
      hbs_->incBytesRead(copyLen);
    return HTC_OK;
}

HTC_RetCode HTableClient_JNI::getColVal(NAHeap *heap, int colNo, BYTE **colVal, 
                                        Lng32 &colValLen,
                                        const char * encryptionInfo)
{
    jint kvsPerRow = p_kvsPerRow_[currentRowNum_];
    if (kvsPerRow == 0 || colNo >= kvsPerRow)
       return HTC_GET_COLVAL_EXCEPTION;
    int idx = prevRowCellNum_ + colNo;
    ex_assert((idx < numCellsReturned_), "Buffer overflow");
    jint kvValLen = p_kvValLen_[idx];
    jint kvValOffset = p_kvValOffset_[idx];
   
    BYTE *colValTmp;
    int colValLenTmp;
    if (heap == NULL)
    {
       colValTmp = *colVal; 
       colValLenTmp = colValLen;
       if (colValLenTmp > kvValLen)
          colValLenTmp = kvValLen;
    }
    else
    {
       colValTmp = new (heap) BYTE[kvValLen];
       colValLenTmp = kvValLen;
    }
    jenv_->GetByteArrayRegion(jba_kvBuffer_, kvValOffset, colValLenTmp,
             (jbyte *)colValTmp); 
    *colVal = colValTmp;
    colValLen = colValLenTmp;
    if (hbs_)
      hbs_->incBytesRead(colValLen);
    return HTC_OK;
}

HTC_RetCode HTableClient_JNI::getNumCellsPerRow(int &numCells)
{
    jint kvsPerRow = p_kvsPerRow_[currentRowNum_];
    numCells = kvsPerRow;
    if (numCells == 0)
       return HTC_DONE_DATA;
    else
       return HTC_OK;
}  


HTC_RetCode HTableClient_JNI::getRowID(HbaseStr &rowID,
                                       const char * encryptionInfo)
{
    jint kvsPerRow = p_kvsPerRow_[currentRowNum_];
    if (p_rowID_ != NULL)
    {
      jenv_->ReleaseByteArrayElements(jba_rowID_, p_rowID_, JNI_ABORT);
      p_rowID_ = NULL;
      jenv_->DeleteGlobalRef(jba_rowID_);
    }

    if (kvsPerRow == 0) 
    {
       rowID.len = 0;
       rowID.val = NULL;
    }
    else
    {
       jobject rowIDObj;
       rowIDObj = jenv_->GetObjectArrayElement(jRowIDs_, currentRowNum_);
       jba_rowID_ = (jbyteArray)jenv_->NewGlobalRef(rowIDObj);
       if (jenv_->ExceptionCheck())
       {
          getExceptionDetails(__FILE__, __LINE__, "HTableClient_JNI::getRowID()", tableName_);
          return HTC_GET_ROWID_EXCEPTION;
       }
       jenv_->DeleteLocalRef(rowIDObj);
       p_rowID_ = jenv_->GetByteArrayElements(jba_rowID_, NULL);
       rowIDLen_ = jenv_->GetArrayLength(jba_rowID_); 
       rowID.len = rowIDLen_;
       rowID.val = (char *)p_rowID_;
    }
    return HTC_OK;
}

static Int64 costTh = -2;
static pid_t pid = getpid();
HTC_RetCode HTableClient_JNI::fetchRows()
{
   QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HTableClient_JNI::fetchRows() called.", tableName_);
   Int64 timeCost = JULIANTIMESTAMP();

   if (initJNIEnv() != JOI_OK)
      return HTC_ERROR_INIT_PARAM;

   if (costTh == -2)
   {  
     char *costThreshold = getenv("RECORD_TIME_COST");
     if (costThreshold != NULL)
       costTh = atoi(costThreshold);
     if (costTh == -2)
       costTh = -1;
   }

   jlong jniObject = (jlong)this;
   if (hbs_)
     hbs_->getHbaseTimer().start();
   tsRecentJMFromJNI = JavaMethods_[JM_FETCH_ROWS].jm_full_name;
   jint jRowsReturned = jenv_->CallIntMethod(javaObj_, 
             JavaMethods_[JM_FETCH_ROWS].methodID,
             jniObject);
   if (hbs_)
    {
      hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
      hbs_->incHbaseCalls();
    }

   if (jenv_->ExceptionCheck())
   {
     getExceptionDetails(__FILE__, __LINE__, "HTableClient_JNI::fetchRows()", tableName_);
 
     HTC_RetCode ret_Lock = HTC_OK;
     if (gEnableRowLevelLock) {
       ret_Lock = getLockError();
     }

      jenv_->PopLocalFrame(NULL);
      if (ret_Lock != HTC_OK){
        return ret_Lock;
      }
      return HTC_ERROR_FETCHROWS_EXCEPTION;
   }

   if (costTh >= 0) {
     timeCost = JULIANTIMESTAMP() - timeCost;
     timeCost = timeCost / 1000;
     if (timeCost >= costTh)
       QRLogger::log(CAT_SQL_HBASE, LL_WARN, "fetchRows PID %ld txID %ld TC %ld %s ",
                     pid, (GetCliGlobals() ? GetCliGlobals()->getTransactionId() : -1), timeCost, tableName_);
   }
   numRowsReturned_ = jRowsReturned;
   if (numRowsReturned_ == 0) {
      jenv_->PopLocalFrame(NULL);
      return HTC_DONE;
   }
   if (hbs_)
      hbs_->incAccessedRows(numRowsReturned_);
   jenv_->PopLocalFrame(NULL);
   return HTC_OK; 
}

HTC_RetCode HTableClient_JNI::nextCell(
        	 HbaseStr &rowId,
                 HbaseStr &colFamName,
                 HbaseStr &colQualName,
                 HbaseStr &colVal,
                 Int64 &timestamp)
{
   HTC_RetCode retcode;
   jint kvsPerRow = p_kvsPerRow_[currentRowNum_];
   if (currentRowCellNum_ >= kvsPerRow)
   {
      currentRowCellNum_ = -1;
      return HTC_DONE;
   }
   if (p_rowID_ != NULL)
   {
      rowId.val = (char *)p_rowID_;
      rowId.len = rowIDLen_;
   }
   else
   {
      retcode = getRowID(rowId);
      if (retcode != HTC_OK)
         return retcode;
   }
   int idx = prevRowCellNum_ + currentRowCellNum_;
   ex_assert((idx < numCellsReturned_), "Buffer overflow");
   jint kvQualLen = p_kvQualLen_[idx];
   jint kvQualOffset = p_kvQualOffset_[idx];
   jint kvFamLen = p_kvFamLen_[idx];
   jint kvFamOffset = p_kvFamOffset_[idx];

   if ((retcode = prepareForNextCell(idx)) != HTC_OK)
       return retcode;

   int colNameLen = kvQualLen + kvFamLen + 1; // 1 for ':'
   char * colName;
   if (colNameAllocLen_ == 0  && colNameLen <= INLINE_COLNAME_LEN)
      colName = inlineColName_;
   else
   {
      if (colNameLen > colNameAllocLen_)
      {
         if (colNameAllocLen_ != 0)
             NADELETEBASIC(colName_, heap_);
         colName_ = new (heap_) char[colNameLen+1];
         colNameAllocLen_ = colNameLen;
      }
      colName = colName_;
   }
   jenv_->GetByteArrayRegion(jba_kvFamArray_, kvFamOffset, kvFamLen,
            (jbyte *)colName);
   colName[kvFamLen] = ':';
   colFamName.val = colName;
   colFamName.len = kvFamLen; 
   char *temp = colName+ kvFamLen+1;
   jenv_->GetByteArrayRegion(jba_kvQualArray_, kvQualOffset, kvQualLen,
            (jbyte *)temp);
   colQualName.val = temp;
   colQualName.len = kvQualLen;
   timestamp = p_timestamp_[idx];
   retcode = getColVal(NULL, currentRowCellNum_, (BYTE **)&colVal.val,
                         colVal.len);
   if (retcode != HTC_OK)
      return retcode;
   currentRowCellNum_++;
   return HTC_OK;
}

HTC_RetCode HTableClient_JNI::completeAsyncOperation(Int32 timeout, NABoolean *resultArray, Int16 resultArrayLen)
{
  ENTRY_FUNCTION();

  if (initJNIEnv() != JOI_OK) {
     if (hbs_)
        hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
     EXIT_FUNCTION(HTC_ERROR_INIT_PARAM);
     return HTC_ERROR_INIT_PARAM;
  }

  HTC_RetCode retValue = HTC_OK;
  jint jtimeout = timeout;
  jboolean jresult = false;
  jboolean *returnArray = NULL;
  jbooleanArray jresultArray =  jenv_->NewBooleanArray(resultArrayLen);
  CHECK_EXCEPTION_AND_RETURN_ERROR(tableName_, HTC_ERROR_COMPLETEASYNCOPERATION_EXCEPTION);

  tsRecentJMFromJNI = JavaMethods_[JM_COMPLETE_PUT].jm_full_name;
  jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_COMPLETE_PUT].methodID,
                                     jtimeout, jresultArray);

  CHECK_EXCEPTION_AND_RETURN_LOCK_ERROR(tableName_, HTC_ERROR_COMPLETEASYNCOPERATION_EXCEPTION);
  if (jresult == false)
  {
      retValue = HTC_ERROR_ASYNC_OPERATION_NOT_COMPLETE;
      goto errorLabel;
  }

  returnArray = jenv_->GetBooleanArrayElements(jresultArray, NULL);
  for (int i = 0; i < resultArrayLen; i++) 
      resultArray[i] = returnArray[i]; 
  jenv_->ReleaseBooleanArrayElements(jresultArray, returnArray, JNI_ABORT);
  if (hbs_)
      hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());

errorLabel:
  CHECK_AND_DELETE_NEWOBJ(jresultArray);

  jenv_->PopLocalFrame(NULL);
  EXIT_FUNCTION(retValue);
  return retValue;
}


HTC_RetCode HTableClient_JNI::getLockError()
{
  char *errMsg = (char*)getSqlJniErrorStr();
  if (errMsg == "" || errMsg == NULL)
  {
      return HTC_OK;
  }

  NAString error_msg(errMsg);
  if (error_msg.contains("LockTimeOutException failed to get lock for transaction rollbacked"))
    {
      return HTC_ERROR_LOCK_ROLLBACK_EXCEPTION;
    }
  else if (error_msg.contains("LockTimeOutException"))
    {
      return HTC_ERROR_LOCK_TIME_OUT_EXCEPTION;
    }
  else if (error_msg.contains("DeadLockException"))
    {
      return HTC_ERROR_DEAD_LOCK_EXCEPTION;
    }
  else if (error_msg.contains("RPCTimeOutException"))
    {
      return HTC_ERROR_RPC_TIME_OUT_EXCEPTION;
    }
  else if (error_msg.contains("LockCancelOperationException"))
    {
      return HTC_ERROR_CANCEL_OPERATION;
    }
  else if (error_msg.contains("FailedToLockException") && error_msg.contains("region move"))
    {
      return HTC_ERROR_LOCK_REGION_MOVE;
    }
  else if (error_msg.contains("FailedToLockException") && error_msg.contains("region split"))
    {
      return HTC_ERROR_LOCK_REGION_SPLIT;
    }
    else if (error_msg.contains("LockNotEnoughResourcsException"))
    {
      return HTC_ERROR_LOCK_NOT_ENOUGH_RESOURCE_EXCEPTION;
    }
  else
    {
      return HTC_OK;
    }
  
  return HTC_OK;
}

jobjectArray convertToByteArrayObjectArray(const LIST(NAString) &vec)
{
   int vecLen = vec.entries();
   int i = 0;
   jobjectArray j_objArray = NULL;
   for ( ; i < vec.entries(); i++)
   {
       const NAString *naStr = &vec.at(i);
       jbyteArray j_obj = jenv_->NewByteArray(naStr->length());
       if (jenv_->ExceptionCheck())
       {
          if (j_objArray != NULL)
             jenv_->DeleteLocalRef(j_objArray);
          return NULL; 
       }
       jenv_->SetByteArrayRegion(j_obj, 0, naStr->length(), (const jbyte *)naStr->data());
       if (j_objArray == NULL)
       {
          j_objArray = jenv_->NewObjectArray(vecLen,
                 jenv_->GetObjectClass(j_obj), NULL);
          if (jenv_->ExceptionCheck())
          {
             jenv_->DeleteLocalRef(j_obj);
             return NULL;
          }
       }
       jenv_->SetObjectArrayElement(j_objArray, i, (jobject)j_obj);
       jenv_->DeleteLocalRef(j_obj);
   }
   return j_objArray;
}

jobjectArray convertToByteArrayObjectArray(const LIST(HbaseStr) &vec)
{
   int vecLen = vec.entries();
   int i = 0;
   jobjectArray j_objArray = NULL;
   for ( ; i < vec.entries(); i++)
   {
       const HbaseStr *hbStr = &vec.at(i);
       jbyteArray j_obj = jenv_->NewByteArray(hbStr->len);
       if (jenv_->ExceptionCheck())
       {
          if (j_objArray != NULL)
             jenv_->DeleteLocalRef(j_objArray);
          return NULL; 
       }
       jenv_->SetByteArrayRegion(j_obj, 0, hbStr->len, (const jbyte *)hbStr->val);
       if (j_objArray == NULL)
       {
          j_objArray = jenv_->NewObjectArray(vecLen,
                 jenv_->GetObjectClass(j_obj), NULL);
          if (jenv_->ExceptionCheck())
          {
             jenv_->DeleteLocalRef(j_obj);
             return NULL;
          }
       }
       jenv_->SetObjectArrayElement(j_objArray, i, (jobject)j_obj);
       jenv_->DeleteLocalRef(j_obj);
   }
   return j_objArray;
}

jobjectArray convertToByteArrayObjectArray(const char **array,
                   int numElements, int elementLen)
{
   int i = 0;
   jobjectArray j_objArray = NULL;
   for (i = 0; i < numElements; i++)
   {
       jbyteArray j_obj = jenv_->NewByteArray(elementLen);
       if (jenv_->ExceptionCheck())
       {
          if (j_objArray != NULL)
             jenv_->DeleteLocalRef(j_objArray);
          return NULL; 
       }
       jenv_->SetByteArrayRegion(j_obj, 0, elementLen,
             (const jbyte *)(array[i]));
       if (j_objArray == NULL)
       {
          j_objArray = jenv_->NewObjectArray(numElements,
                 jenv_->GetObjectClass(j_obj), NULL);
          if (jenv_->ExceptionCheck())
          {
             jenv_->DeleteLocalRef(j_obj);
             return NULL;
          }
       }
       jenv_->SetObjectArrayElement(j_objArray, i, (jobject)j_obj);
       jenv_->DeleteLocalRef(j_obj);
   }
   return j_objArray;
}

jobjectArray convertToByteArrayObjectArray(const TextVec &vec)
{
   int vecLen = vec.size();
   int i = 0;
   jobjectArray j_objArray = NULL;
   for ( ; i < vec.size(); i++)
   {
       const Text &t = vec[i];
     //       const HbaseStr *hbStr = &vec.at(i);
       jbyteArray j_obj = jenv_->NewByteArray(t.size());
       if (jenv_->ExceptionCheck())
       {
          if (j_objArray != NULL)
             jenv_->DeleteLocalRef(j_objArray);
          return NULL; 
       }
       jenv_->SetByteArrayRegion(j_obj, 0, t.size(), (const jbyte *)t.data());
       if (j_objArray == NULL)
       {
          j_objArray = jenv_->NewObjectArray(vecLen,
                 jenv_->GetObjectClass(j_obj), NULL);
          if (jenv_->ExceptionCheck())
          {
             jenv_->DeleteLocalRef(j_obj);
             return NULL;
          }
       }
       jenv_->SetObjectArrayElement(j_objArray, i, (jobject)j_obj);
       jenv_->DeleteLocalRef(j_obj);
   }
   return j_objArray;
}

jobjectArray convertToStringObjectArray(const TextVec &vec)
{
   int vecLen = vec.size();
   int i = 0;
   jobjectArray j_objArray = NULL;
   for (std::vector<Text>::const_iterator it = vec.begin(); 
           it != vec.end(); ++it, i++)
   {
       jstring j_obj = jenv_->NewStringUTF((*it).data());
       if (jenv_->ExceptionCheck())
       {
          if (j_objArray != NULL)
             jenv_->DeleteLocalRef(j_objArray);
          return NULL; 
       }
       if (j_objArray == NULL)
       {
          j_objArray = jenv_->NewObjectArray(vecLen,
                 jenv_->GetObjectClass(j_obj), NULL);
          if (jenv_->ExceptionCheck())
          {
             jenv_->DeleteLocalRef(j_obj);
             return NULL;
          }
       }
       jenv_->SetObjectArrayElement(j_objArray, i, (jobject)j_obj);
       jenv_->DeleteLocalRef(j_obj);
   }
   return j_objArray;
}

jobjectArray convertToStringObjectArray(const HBASE_NAMELIST& nameList)
{
   int listLen = nameList.entries();
   int i = 0;
   jobjectArray j_objArray = NULL;
   for ( i = 0; i < listLen ; i++)
   {
       jstring j_obj = jenv_->NewStringUTF(nameList.at(i).val);
       if (jenv_->ExceptionCheck())
       {
          if (j_objArray != NULL)
             jenv_->DeleteLocalRef(j_objArray);
          return NULL; 
       }
       if (j_objArray == NULL)
       {
          j_objArray = jenv_->NewObjectArray(listLen,
                 jenv_->GetObjectClass(j_obj), NULL);
          if (jenv_->ExceptionCheck())
          {
             jenv_->DeleteLocalRef(j_obj);
             return NULL;
          }
       }
       jenv_->SetObjectArrayElement(j_objArray, i, (jobject)j_obj);
       jenv_->DeleteLocalRef(j_obj);
   }
   return j_objArray;
}

jobjectArray convertToStringObjectArray(const NAText *textArray, int arrayLen)
{
   int i = 0;
   jobjectArray j_objArray = NULL;
   for ( i = 0; i < arrayLen ; i++)
   {
       jstring j_obj = jenv_->NewStringUTF(textArray[i].c_str());
       if (jenv_->ExceptionCheck())
       {
          if (j_objArray != NULL)
             jenv_->DeleteLocalRef(j_objArray);
          return NULL; 
       }
       if (j_objArray == NULL)
       {
          j_objArray = jenv_->NewObjectArray(arrayLen,
                 jenv_->GetObjectClass(j_obj), NULL);
          if (jenv_->ExceptionCheck())
          {
             jenv_->DeleteLocalRef(j_obj);
             return NULL;
          }
       }
       jenv_->SetObjectArrayElement(j_objArray, i, (jobject)j_obj);
       jenv_->DeleteLocalRef(j_obj);
   }
   return j_objArray;
}

jobjectArray convertToStringObjectArray(const set<string> &setOfNames)
{
  jobjectArray j_objArray = NULL;
  int i = 0;
  for (set<string>::const_iterator it = setOfNames.begin(); it != setOfNames.end(); ++it)
    {
      jstring j_obj = jenv_->NewStringUTF((*it).c_str());
      if (jenv_->ExceptionCheck())
        {
          if (j_objArray != NULL)
            jenv_->DeleteLocalRef(j_objArray);
          return NULL; 
        }
      if (j_objArray == NULL)
        {
          j_objArray = jenv_->NewObjectArray(setOfNames.size(),
                 jenv_->GetObjectClass(j_obj), NULL);
          if (jenv_->ExceptionCheck())
            {
              jenv_->DeleteLocalRef(j_obj);
              return NULL;
            }
        }
      jenv_->SetObjectArrayElement(j_objArray, i, (jobject)j_obj);
      i++;
      jenv_->DeleteLocalRef(j_obj);
    }
  return j_objArray;
}

jobjectArray convertMapToStringObjectArray(const map<Lng32,char *> & columnNumberToNameMap,
                                           vector<Lng32> &positionToColumnNumberVector /* out */)
{
  jobjectArray j_objArray = NULL;
  int i = 0;
  for (map<Lng32,char *>::const_iterator it = columnNumberToNameMap.begin(); 
       it != columnNumberToNameMap.end(); ++it)
    {
      jstring j_obj = jenv_->NewStringUTF(it->second);
      if (jenv_->ExceptionCheck())
        {
          if (j_objArray != NULL)
            jenv_->DeleteLocalRef(j_objArray);
          return NULL; 
        }
      if (j_objArray == NULL)
        {
          j_objArray = jenv_->NewObjectArray(columnNumberToNameMap.size(),
                 jenv_->GetObjectClass(j_obj), NULL);
          if (jenv_->ExceptionCheck())
            {
              jenv_->DeleteLocalRef(j_obj);
              return NULL;
            }
        }
      jenv_->SetObjectArrayElement(j_objArray, i, (jobject)j_obj);
      positionToColumnNumberVector[i] = it->first;
      i++;
      jenv_->DeleteLocalRef(j_obj);
    }
  return j_objArray;
}

int convertStringObjectArrayToList(NAHeap *heap, jarray j_objArray, 
                                         LIST(Text *)&list)
{

    if (j_objArray == NULL)
        return 0;
    int arrayLen = jenv_->GetArrayLength(j_objArray);
    jstring j_str;
    const char *str;
    jboolean isCopy;

    for (int i = 0; i < arrayLen; i++)
    {
        j_str = (jstring)jenv_->GetObjectArrayElement((jobjectArray)j_objArray, i);
        str = jenv_->GetStringUTFChars(j_str, &isCopy);
        list.insert(new (heap) Text(str));
        jenv_->ReleaseStringUTFChars(j_str, str);        
        jenv_->DeleteLocalRef(j_str);
    }
    return arrayLen;
}

int convertLongObjectArrayToList(NAHeap *heap, jlongArray j_longArray, LIST(Int64)&list)
{
    if (j_longArray == NULL)
        return 0;
    int arrayLen = jenv_->GetArrayLength(j_longArray);
    const char *str;
    jboolean isCopy;

    jlong *body = jenv_->GetLongArrayElements(j_longArray, NULL);

    for (int i = 0; i < arrayLen; i++)
    {
        jlong x = body[i];
        list.insert(Int64(body[i]));
    }

    jenv_->ReleaseLongArrayElements(j_longArray, body, JNI_ABORT);

    return arrayLen;
}

int convertIntObjectArrayToVector(jintArray j_intArray, vector<Lng32> &vec)
{
    if (j_intArray == NULL)
        return 0;
    int arrayLen = jenv_->GetArrayLength(j_intArray);

    jint *body = jenv_->GetIntArrayElements(j_intArray, NULL);

    for (int i = 0; i < arrayLen; i++)
    {
        jint x = body[i];
        vec[i] = x;
    }

    jenv_->ReleaseIntArrayElements(j_intArray, body, JNI_ABORT);

    return arrayLen;
}


jint convertByteArrayObjectArrayToNAArray(NAHeap *heap, jarray j_objArray, NAArray<HbaseStr> **retArray)
{
    if (j_objArray == NULL) {
       *retArray = NULL;
       return 0;
    }
    int arrayLen = jenv_->GetArrayLength(j_objArray);
    jbyteArray j_ba;
    jint j_baLen;
    BYTE *ba;
    jboolean isCopy;
    HbaseStr element; 
    NAArray<HbaseStr> *tmpArray = new (heap) NAArray<HbaseStr> (heap, arrayLen); 
    for (int i = 0; i < arrayLen; i++) {
        j_ba = (jbyteArray)jenv_->GetObjectArrayElement((jobjectArray)j_objArray, i);
        if (j_ba == NULL) {
           element.len = 0;
           element.val = NULL;
           tmpArray->insert(i,element);
           continue;
        }
        j_baLen = jenv_->GetArrayLength(j_ba);
        ba = new (heap) BYTE[j_baLen+1];
        ba[j_baLen] = '\0';
        jenv_->GetByteArrayRegion(j_ba, 0, j_baLen, (jbyte *)ba); 
        element.len = j_baLen;
        element.val = (char *)ba;
        tmpArray->insert(i,element);
        jenv_->DeleteLocalRef(j_ba);
    }
    *retArray = tmpArray;
    return arrayLen;
}

jint convertStringObjectArrayToNAArray(NAHeap *heap, jarray j_objArray, NAArray<HbaseStr> **retArray) 
{
    if (j_objArray == NULL) {
        *retArray = NULL;
        return 0;
    }
    int arrayLen = jenv_->GetArrayLength(j_objArray);
    NAArray<HbaseStr> *tmpArray = new (heap) NAArray<HbaseStr> (heap, arrayLen); 
    jstring j_str;
    char *str;
    jboolean isCopy;
    jint strLen;
    HbaseStr element; 

    for (int i = 0; i < arrayLen; i++)
    {
        j_str = (jstring)jenv_->GetObjectArrayElement((jobjectArray)j_objArray, i);
        if (j_str != NULL) {
          strLen = jenv_->GetStringUTFLength(j_str);
          str = new (heap) char[strLen+1]; 
          jenv_->GetStringUTFRegion(j_str, 0, strLen, str);
          str[strLen] = '\0';
          element.len = strLen+1;
          element.val = (char *)str;
          jenv_->DeleteLocalRef(j_str);
        } else {
          element.val = NULL;
          element.len = 0;
        }
        tmpArray->insert(i,element);
    }
    *retArray = tmpArray;
    return arrayLen;
}

void deleteNAArray(CollHeap *heap, NAArray<HbaseStr> *array)
{
  
  if (array == NULL)
     return;
  CollIndex entryCount = array->entries();
  for (CollIndex i = 0 ; i < entryCount; i++) {
      NADELETEBASIC(array->at(i).val, heap);
  }
  NADELETE(array, NAArray, heap);
}

char* getClientInfoFromContext()
{
    if (GetCliGlobals()->currContext()->getClientInfo()) {
        return GetCliGlobals()->currContext()->getClientInfo();
    } else {
        return NULL;
    }
}

HBC_RetCode HBaseClient_JNI::putData(Int64 eventID, const char* query, int eventType, const char* schemaName, unsigned char* params, long len)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "HBaseClient_JNI::putSqlToHbse(%s) called.", query);
  if (initJNIEnv() != JOI_OK)
     return HBC_ERROR_INIT_PARAM;

  jlong j_tid = eventID;

  jstring js_query = jenv_->NewStringUTF(query);
  if (js_query == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_PUT_SQL_TO_HBASE_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_PUT_SQL_TO_HBASE_PARAM;
  }

  jstring js_objNmae = jenv_->NewStringUTF(schemaName);
  if (js_objNmae == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_PUT_SQL_TO_HBASE_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_PUT_SQL_TO_HBASE_PARAM;
  }

  jint js_sqltype = eventType;

  jbyteArray js_sqlparms = jenv_->NewByteArray(len);
  if (js_sqlparms == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HBC_ERROR_PUT_SQL_TO_HBASE_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_PUT_SQL_TO_HBASE_PARAM;
  }
  jenv_->SetByteArrayRegion(js_sqlparms, 0, len, (const jbyte*)params);

                              
  tsRecentJMFromJNI = JavaMethods_[JM_PUT_SQL_TO_HBASE].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_PUT_SQL_TO_HBASE].methodID,
                                              j_tid, js_query, js_sqltype, js_objNmae, js_sqlparms);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HBaseClient_JNI::putData()", query);
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_PUT_SQL_TO_HBASE_ERROR;
  }

  if (jresult == false)
  {
    logError(CAT_SQL_HBASE, "HBaseClient_JNI::putData()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return HBC_ERROR_PUT_SQL_TO_HBASE_ERROR;
  }
  jenv_->PopLocalFrame(NULL);
  return HBC_OK;
}
