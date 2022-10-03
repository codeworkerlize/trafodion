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
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
// **********************************************************************

#include "cli/Context.h"
#include "cli/Globals.h"
#include "MonarchClient_JNI.h"
#include "qmscommon/QRLogger.h"
#include <signal.h>
#include "pthread.h"
#include "HBaseClient_JNI.h"

// ===========================================================================
// ===== Class MonarchClient_JNI
// ===========================================================================

JavaMethodInit* MonarchClient_JNI::JavaMethods_ = NULL;
jclass MonarchClient_JNI::javaClass_ = 0;
bool MonarchClient_JNI::javaMethodsInitialized_ = false;
pthread_mutex_t MonarchClient_JNI::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

// Keep in sync with MC_RetCode enum.
static const char* const hbcErrorEnumStr[] = 
{
  "Preparing parameters for initConnection()."
 ,"Java exception in initConnection()."
 ,"Java exception in getMTableClient()."
 ,"Java exception in releaseMTableClient()."
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
 ,"Java exception in releaseHBulkLoadClient()."
 ,"Java exception in getBlockCacheFraction()."
 ,"Preparing parameters for getLatestSnapshot()."
 ,"Java exception in getLatestSnapshot()."
 ,"Preparing parameters for cleanSnpTmpLocation()."
 ,"Java exception in cleanSnpTmpLocation()."
 ,"Preparing parameters for setArcPerms()."
 ,"Java exception in setArcPerms()."
 ,"Java exception in startGet()."
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
 ,"Preparing parameters for insertRows()."
 ,"Java exception in insertRows()."
 ,"Java exception in checkAndUpdateRow()."
 ,"Preparing parameters for checkAndUpdateRow()."
 ,"Row not found in checkAndUpdateRow()."
 ,"Preparing parameters for deleteRow()."
 ,"Java exception in deleteRow()."
 ,"Preparing parameters for deleteRows()."
 ,"Java exception in deleteRows()."
 ,"Preparing parameters for checkAndDeleteRow()."
 ,"Java exception in checkAndDeleteRow()."
 ,"Row not found in checkAndDeleteRow()."
 ,"Pool does not exist."
 ,"Preparing parameters for getKeys()"
};

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
// private default constructor
MonarchClient_JNI::MonarchClient_JNI(NAHeap *heap)
                 :  JavaObjectInterface(heap)
                   ,isConnected_(FALSE)
{
/*
  for (int i=0; i<NUM_HBASE_WORKER_THREADS; i++) {
    threadID_[i] = NULL;
  }
*/
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
char* MonarchClient_JNI::getErrorText(MC_RetCode errEnum)
{
  if (errEnum < (MC_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)errEnum);
  else    
    return (char*)hbcErrorEnumStr[errEnum-MC_FIRST-1];
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MonarchClient_JNI* MonarchClient_JNI::getInstance()
{
   CliGlobals *cliGlobals = GetCliGlobals(); 
   MonarchClient_JNI *monarchClient_JNI = cliGlobals->getMonarchClient();
   if (monarchClient_JNI == NULL)
   {
     NAHeap *heap = cliGlobals->getExecutorMemory();
    
     monarchClient_JNI  = new (heap) MonarchClient_JNI(heap);
     cliGlobals->setMonarchClient(monarchClient_JNI);
   }
   return monarchClient_JNI;
}

void MonarchClient_JNI::deleteInstance()
{
   CliGlobals *cliGlobals = GetCliGlobals(); 
   MonarchClient_JNI *monarchClient_JNI = cliGlobals->getMonarchClient();
   if (monarchClient_JNI != NULL)
   {
      NAHeap *heap = cliGlobals->getExecutorMemory();
      NADELETE(monarchClient_JNI, MonarchClient_JNI, heap);
      cliGlobals->setMonarchClient(NULL);
   }
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MonarchClient_JNI::~MonarchClient_JNI()
{
  //QRLogger::log(CAT_JNI_TOP, LL_DEBUG, "MonarchClient_JNI destructor called.");
/* 
  // worker threads need to go away and be joined. 
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
*/
  // Clean the Java Side
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::init()
{
  static char className[]="org/trafodion/sql/MonarchClient";
  MC_RetCode rc;
  
  if (isInitialized())
    return MC_OK;
  
  if (javaMethodsInitialized_)
    return (MC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
  else
  {
    pthread_mutex_lock(&javaMethodsInitMutex_);
    if (javaMethodsInitialized_)
    {
      pthread_mutex_unlock(&javaMethodsInitMutex_);
      return (MC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    }
    JavaMethods_ = new JavaMethodInit[JM_LAST];
    
    JavaMethods_[JM_CTOR       ].jm_name      = "<init>";
    JavaMethods_[JM_CTOR       ].jm_signature = "()V";
    JavaMethods_[JM_INIT       ].jm_name      = "init";
    JavaMethods_[JM_INIT       ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)Z";
    JavaMethods_[JM_GET_MTC    ].jm_name      = "getMTableClient";
    JavaMethods_[JM_GET_MTC    ].jm_signature = "(JLjava/lang/String;ZZ)Lorg/trafodion/sql/MTableClient;";
    JavaMethods_[JM_REL_MTC    ].jm_name      = "releaseMTableClient";
    JavaMethods_[JM_REL_MTC    ].jm_signature = "(Lorg/trafodion/sql/MTableClient;)V";
    JavaMethods_[JM_CREATE     ].jm_name      = "create";
    JavaMethods_[JM_CREATE     ].jm_signature = "(Ljava/lang/String;[Ljava/lang/Object;Z)Z";
    JavaMethods_[JM_CREATEK    ].jm_name      = "createk";
    JavaMethods_[JM_CREATEK    ].jm_signature = "(Ljava/lang/String;I[Ljava/lang/Object;[Ljava/lang/Object;[Ljava/lang/Object;JIIZ)Z";
/*
    JavaMethods_[JM_TRUNCABORT ].jm_name      = "registerTruncateOnAbort";
    JavaMethods_[JM_TRUNCABORT ].jm_signature = "(Ljava/lang/String;J)Z";
    JavaMethods_[JM_ALTER      ].jm_name      = "alter";
    JavaMethods_[JM_ALTER      ].jm_signature = "(Ljava/lang/String;[Ljava/lang/Object;J)Z";
*/
    JavaMethods_[JM_DROP       ].jm_name      = "drop";
    JavaMethods_[JM_DROP       ].jm_signature = "(Ljava/lang/String;J)Z";
/*
    JavaMethods_[JM_DROP_ALL       ].jm_name      = "dropAll";
    JavaMethods_[JM_DROP_ALL       ].jm_signature = "(Ljava/lang/String;)Z";
*/
    JavaMethods_[JM_LIST_ALL       ].jm_name      = "listAll";
    JavaMethods_[JM_LIST_ALL       ].jm_signature = "(Ljava/lang/String;)[Ljava/lang/String;";
/*
    JavaMethods_[JM_GET_REGION_STATS       ].jm_name      = "getRegionStats";
    JavaMethods_[JM_GET_REGION_STATS       ].jm_signature = "(Ljava/lang/String;)[[B";
    JavaMethods_[JM_COPY       ].jm_name      = "copy";
    JavaMethods_[JM_COPY       ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)Z";
*/
    JavaMethods_[JM_EXISTS     ].jm_name      = "exists";
    JavaMethods_[JM_EXISTS     ].jm_signature = "(Ljava/lang/String;)Z";
/*
    JavaMethods_[JM_GRANT      ].jm_name      = "grant";
    JavaMethods_[JM_GRANT      ].jm_signature = "([B[B[Ljava/lang/Object;)Z";
    JavaMethods_[JM_REVOKE     ].jm_name      = "revoke";
    JavaMethods_[JM_REVOKE     ].jm_signature = "([B[B[Ljava/lang/Object;)Z";
    JavaMethods_[JM_GET_HBLC   ].jm_name      = "getHBulkLoadClient";
    JavaMethods_[JM_GET_HBLC   ].jm_signature = "()Lorg/trafodion/sql/HBulkLoadClient;";
    JavaMethods_[JM_EST_RC     ].jm_name      = "estimateRowCount";
    JavaMethods_[JM_EST_RC     ].jm_signature = "(Ljava/lang/String;II[J)Z";
    JavaMethods_[JM_REL_HBLC   ].jm_name      = "releaseHBulkLoadClient";
    JavaMethods_[JM_REL_HBLC   ].jm_signature = "(Lorg/trafodion/sql/HBulkLoadClient;)V";
    JavaMethods_[JM_GET_CAC_FRC].jm_name      = "getBlockCacheFraction";
    JavaMethods_[JM_GET_CAC_FRC].jm_signature = "()F";
    JavaMethods_[JM_GET_LATEST_SNP].jm_name      = "getLatestSnapshot";
    JavaMethods_[JM_GET_LATEST_SNP].jm_signature = "(Ljava/lang/String;)Ljava/lang/String;";
    JavaMethods_[JM_CLEAN_SNP_TMP_LOC].jm_name      = "cleanSnpScanTmpLocation";
    JavaMethods_[JM_CLEAN_SNP_TMP_LOC].jm_signature = "(Ljava/lang/String;)Z";
    JavaMethods_[JM_SET_ARC_PERMS].jm_name      = "setArchivePermissions";
    JavaMethods_[JM_SET_ARC_PERMS].jm_signature = "(Ljava/lang/String;)Z";
*/
    JavaMethods_[JM_START_GET].jm_name      = "startGet";
    JavaMethods_[JM_START_GET].jm_signature = "(JLjava/lang/String;ZZJ[B[Ljava/lang/Object;JLjava/lang/String;)I";
    JavaMethods_[JM_START_GETS].jm_name      = "startGet";
    JavaMethods_[JM_START_GETS].jm_signature = "(JLjava/lang/String;ZZJ[Ljava/lang/Object;[Ljava/lang/Object;JLjava/lang/String;)I";
    JavaMethods_[JM_START_DIRECT_GETS].jm_name      = "startGet";
    JavaMethods_[JM_START_DIRECT_GETS].jm_signature = "(JLjava/lang/String;ZZJSLjava/lang/Object;[Ljava/lang/Object;Ljava/lang/String;)I";
/*
    JavaMethods_[JM_GET_HBTI].jm_name      = "getHbaseTableInfo";
    JavaMethods_[JM_GET_HBTI].jm_signature = "(Ljava/lang/String;[I)Z";
    JavaMethods_[JM_CREATE_COUNTER_TABLE ].jm_name      = "createCounterTable";
    JavaMethods_[JM_CREATE_COUNTER_TABLE ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)Z";
    JavaMethods_[JM_INCR_COUNTER         ].jm_name      = "incrCounter";
    JavaMethods_[JM_INCR_COUNTER         ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;J)J";
*/
    JavaMethods_[JM_GET_REGN_NODES].jm_name      = "getRegionsNodeName";
    JavaMethods_[JM_GET_REGN_NODES].jm_signature = "(Ljava/lang/String;[Ljava/lang/String;)Z";
    JavaMethods_[JM_MC_DIRECT_INSERT_ROW].jm_name      = "insertRow";
    JavaMethods_[JM_MC_DIRECT_INSERT_ROW].jm_signature = "(JLjava/lang/String;ZZJ[BLjava/lang/Object;JZZ)Z";
    JavaMethods_[JM_MC_DIRECT_INSERT_ROWS].jm_name      = "insertRows";
    JavaMethods_[JM_MC_DIRECT_INSERT_ROWS].jm_signature = "(JLjava/lang/String;ZZJSLjava/lang/Object;Ljava/lang/Object;JZ)Z";
/*
    JavaMethods_[JM_MC_DIRECT_UPDATE_TAGS].jm_name      = "updateVisibility";
    JavaMethods_[JM_MC_DIRECT_UPDATE_TAGS].jm_signature = "(JLjava/lang/String;ZJ[BLjava/lang/Object;)Z";
*/
    JavaMethods_[JM_MC_DIRECT_CHECKANDUPDATE_ROW].jm_name      = "checkAndUpdateRow";
    JavaMethods_[JM_MC_DIRECT_CHECKANDUPDATE_ROW].jm_signature = "(JLjava/lang/String;ZZJ[BLjava/lang/Object;[B[BJZ)Z";
    JavaMethods_[JM_MC_DELETE_ROW ].jm_name      = "deleteRow";
    JavaMethods_[JM_MC_DELETE_ROW ].jm_signature = "(JLjava/lang/String;ZZJ[B[Ljava/lang/Object;JZLjava/lang/String;)Z";
    JavaMethods_[JM_MC_DIRECT_DELETE_ROWS ].jm_name      = "deleteRows";
    JavaMethods_[JM_MC_DIRECT_DELETE_ROWS ].jm_signature = "(JLjava/lang/String;ZZJSLjava/lang/Object;JZLjava/lang/String;)Z";
    //JavaMethods_[JM_MC_DIRECT_DELETE_ROWS ].jm_signature = "(JLjava/lang/String;ZZJSLjava/lang/Object;[Ljava/lang/Object;JZLjava/lang/String;)Z";
    JavaMethods_[JM_MC_CHECKANDDELETE_ROW ].jm_name      = "checkAndDeleteRow";
    JavaMethods_[JM_MC_CHECKANDDELETE_ROW ].jm_signature = "(JLjava/lang/String;ZZJ[B[B[BJZLjava/lang/String;)Z";

//    JavaMethods_[JM_MC_CHECKANDDELETE_ROW ].jm_signature = "(JLjava/lang/String;ZZJ[B[Ljava/lang/Object;[B[BJZLjava/lang/String;)Z";
/*
    JavaMethods_[JM_SHOW_TABLES_HDFS_CACHE].jm_name = "showTablesHDFSCache";
    JavaMethods_[JM_SHOW_TABLES_HDFS_CACHE].jm_signature = "([Ljava/lang/Object;)[[B";
    JavaMethods_[JM_ADD_TABLES_TO_HDFS_CACHE].jm_name = "addTablesToHDFSCache";
    JavaMethods_[JM_ADD_TABLES_TO_HDFS_CACHE].jm_signature = "([Ljava/lang/Object;Ljava/lang/String;)I";
    JavaMethods_[JM_REMOVE_TABLES_FROM_HDFS_CACHE].jm_name = "removeTablesFromHDFSCache";
    JavaMethods_[JM_REMOVE_TABLES_FROM_HDFS_CACHE].jm_signature = "([Ljava/lang/Object;Ljava/lang/String;)I";
*/
    JavaMethods_[JM_MC_GETSTARTKEYS ].jm_name      = "getStartKeys";
    JavaMethods_[JM_MC_GETSTARTKEYS ].jm_signature = "(Ljava/lang/String;Z)[[B";
    JavaMethods_[JM_MC_GETENDKEYS ].jm_name      = "getEndKeys";
    JavaMethods_[JM_MC_GETENDKEYS ].jm_signature = "(Ljava/lang/String;Z)[[B";

    rc = (MC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    if (rc == MC_OK)
       javaMethodsInitialized_ = TRUE;
    pthread_mutex_unlock(&javaMethodsInitMutex_);
  }
  return rc;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::initConnection(const char* zkServers, const char* zkPort)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::initConnection(%s, %s) called.", zkServers, zkPort);

  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;

  jstring js_zkServers = jenv_->NewStringUTF(zkServers);
  if (js_zkServers == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_INIT_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_INIT_PARAM;
  }
  jstring js_zkPort = jenv_->NewStringUTF(zkPort);
  if (js_zkPort == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_INIT_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_INIT_PARAM;
  }
  tsRecentJMFromJNI = JavaMethods_[JM_INIT].jm_full_name;
  // boolean init(java.lang.String, java.lang.String); 
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_INIT].methodID, js_zkServers, js_zkPort);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::initConnection()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_INIT_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::initConnection()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_INIT_EXCEPTION;
  }

  isConnected_ = TRUE;
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MTableClient_JNI* MonarchClient_JNI::getMTableClient(NAHeap *heap, 
						   const char* tableName,
						   bool useTRex,
						   NABoolean replSync,
						   ExHbaseAccessStats *hbs)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::getMTableClient(%s) called.", tableName);

  if (javaObj_ == NULL || (!isInitialized()))
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_GET_MTC_EXCEPTION));
    return NULL;
  }

  if (initJNIEnv() != JOI_OK)
     return NULL;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_GET_MTC_EXCEPTION));
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  MTableClient_JNI *mtc = new (heap) MTableClient_JNI(heap, (jobject)-1);
  if (mtc->init() != MTC_OK)
  {
     NADELETE(mtc, MTableClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return NULL;
  }
  mtc->setTableName(tableName);
  mtc->setHbaseStats(hbs);

  tsRecentJMFromJNI = JavaMethods_[JM_GET_MTC].jm_full_name;
  jobject j_mtc = jenv_->CallObjectMethod(javaObj_, 
					  JavaMethods_[JM_GET_MTC].methodID, 
					  (jlong)mtc, 
					  js_tblName, 
					  (jboolean)useTRex, 
					  (jboolean) replSync);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::getMTableClient()");
    NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  if (j_mtc == NULL) 
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::getMTableClient()", getLastError());
    NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
  mtc->setJavaObject(j_mtc);
  jenv_->DeleteLocalRef(j_mtc);
  if (mtc->init() != MTC_OK)
  {
     jenv_->PopLocalFrame(NULL);
     releaseMTableClient(mtc);
     return NULL;
  }
  mtc->setTableName(tableName);
  mtc->setHbaseStats(hbs);
  jenv_->PopLocalFrame(NULL);
  return mtc;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::releaseMTableClient(MTableClient_JNI* mtc)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::releaseMTableClient() called.");

  jobject j_mtc = mtc->getJavaObject();

  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;

  if (j_mtc != (jobject)-1) {
      tsRecentJMFromJNI = JavaMethods_[JM_REL_MTC].jm_full_name;
      jenv_->CallVoidMethod(javaObj_, JavaMethods_[JM_REL_MTC].methodID, j_mtc);
      if (jenv_->ExceptionCheck()) {
         getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::releaseMTableClient()");
         jenv_->PopLocalFrame(NULL);
         return MC_ERROR_REL_MTC_EXCEPTION;
      }
  }
  NADELETE(mtc, MTableClient_JNI, mtc->getHeap()); 
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::create(const char* fileName, const LIST(HbaseStr) &cols, NABoolean isMVCC)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::create(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  jstring js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) 
  {
     GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CREATE_PARAM));
     jenv_->PopLocalFrame(NULL);
     return MC_ERROR_CREATE_PARAM;
  }
  jobjectArray j_cols = NULL;
  j_cols = convertToByteArrayObjectArray(cols);
  if (j_cols == NULL) {
     getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::create()");
     jenv_->PopLocalFrame(NULL);
     return MC_ERROR_CREATE_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_CREATE].jm_full_name;
  jboolean j_isMVCC = isMVCC;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
        JavaMethods_[JM_CREATE].methodID, js_fileName, j_cols, j_isMVCC);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::create()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CREATE_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::create()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CREATE_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}


//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::create(const char* fileName, 
                                    int tableType,  const NAList<HbaseStr> &cols,
                                    NAText* createOptionsArray,
                                    int numSplits, int keyLength,
                                    const char ** splitValues,
                                    Int64 transID,
                                    NABoolean isMVCC)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::create(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  jstring js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CREATE_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CREATE_PARAM;
  }
  jobjectArray j_opts = convertToStringObjectArray(createOptionsArray, 
                   HBASE_MAX_OPTIONS);
  if (j_opts == NULL)
  {
     getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::create()");
     jenv_->PopLocalFrame(NULL);
     return MC_ERROR_CREATE_PARAM;
  }

  jobjectArray j_cols = NULL;
  j_cols = convertToByteArrayObjectArray(cols);
  if (j_cols == NULL) {
     getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::create()");
     jenv_->PopLocalFrame(NULL);
     return MC_ERROR_CREATE_PARAM;
  }

  jobjectArray j_keys = NULL;    
  if (numSplits > 0)
  {
     j_keys = convertToByteArrayObjectArray(splitValues, numSplits, keyLength);
     if (j_keys == NULL)
     {
        getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::create()");
        jenv_->PopLocalFrame(NULL);
        return MC_ERROR_CREATE_PARAM;
     }
  }
  jlong j_tid = transID;
  jint j_numSplits = numSplits;
  jint j_keyLength = keyLength;
  jint j_tableType = tableType;

  tsRecentJMFromJNI = JavaMethods_[JM_CREATEK].jm_full_name;
  jboolean j_isMVCC = isMVCC;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
          JavaMethods_[JM_CREATEK].methodID, js_fileName, j_tableType, j_cols, j_opts, j_keys, j_tid, j_numSplits, j_keyLength, j_isMVCC);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::create()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CREATE_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::create()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CREATE_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}
/*
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::alter(const char* fileName,
                                   NAText* createOptionsArray,
                                   Int64 transID)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::alter(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;

  jstring js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CREATE_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_ALTER_PARAM;
  }
  jobjectArray j_opts = convertToStringObjectArray(createOptionsArray, 
                   HBASE_MAX_OPTIONS);
  if (j_opts == NULL)
  {
     getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::alter()");
     jenv_->PopLocalFrame(NULL);
     return MC_ERROR_ALTER_PARAM;
  }

  jlong j_tid = transID;

  tsRecentJMFromJNI = JavaMethods_[JM_ALTER].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
          JavaMethods_[JM_ALTER].methodID, js_fileName, j_opts, j_tid);

  jenv_->DeleteLocalRef(js_fileName); 
  jenv_->DeleteLocalRef(j_opts);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::alter()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_ALTER_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::alter()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_ALTER_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);

  return MC_OK;
}
*/
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::drop(const char* fileName, bool async, long transID)
{
  return drop(fileName, jenv_, transID); // not in worker thread

  return MC_OK;
}
/*
//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::registerTruncateOnAbort(const char* fileName, Int64 transID)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::drop(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  jstring js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_PARAM;
  }

  jlong j_tid = transID;

  tsRecentJMFromJNI = JavaMethods_[JM_TRUNCABORT].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_TRUNCABORT].methodID, js_fileName, j_tid);

  jenv_->DeleteLocalRef(js_fileName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::registerTruncateOnAbort()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_EXCEPTION;
  }

  if (jresult == false)
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::registerTruncateOnAbort()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}
*/
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::drop(const char* fileName, JNIEnv* jenv, Int64 transID)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::drop(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;

  jstring js_fileName = jenv->NewStringUTF(fileName);
  if (js_fileName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_PARAM;
  }

  jlong j_tid = transID;  
  // boolean drop(java.lang.String);
  tsRecentJMFromJNI = JavaMethods_[JM_DROP].jm_full_name;
  jboolean jresult = jenv->CallBooleanMethod(javaObj_, JavaMethods_[JM_DROP].methodID, js_fileName, j_tid);

  if (jenv->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::drop()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::drop()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}
/*
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::dropAll(const char* pattern, bool async)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::dropAll(%s) called.", pattern);

  if (async) {
    // not supported yet.
    return MC_ERROR_DROP_EXCEPTION;
  }
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;

  jstring js_pattern = jenv_->NewStringUTF(pattern);
  if (js_pattern == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_PARAM;
  }

  // boolean drop(java.lang.String);
  tsRecentJMFromJNI = JavaMethods_[JM_DROP_ALL].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_DROP_ALL].methodID, js_pattern);

  jenv_->DeleteLocalRef(js_pattern);  

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::dropAll()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::dropAll()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}
*/
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr>* MonarchClient_JNI::listAll(NAHeap *heap, const char* pattern)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::listAll(%s) called.", pattern);

  if (initJNIEnv() != JOI_OK)
     return NULL;
  jstring js_pattern = jenv_->NewStringUTF(pattern);
  if (js_pattern == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_LIST_ALL].jm_full_name;
  jarray j_monarchTables =
    (jarray)jenv_->CallObjectMethod(javaObj_, JavaMethods_[JM_LIST_ALL].methodID, js_pattern);

  jenv_->DeleteLocalRef(js_pattern);  

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::listAll()");
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  NAArray<HbaseStr> *monarchTables;
  jint retcode = convertStringObjectArrayToNAArray(heap, j_monarchTables, &monarchTables);
  jenv_->PopLocalFrame(NULL);
  if (retcode == 0)
     return NULL;
  else
     return monarchTables;
}
/*
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr>* MonarchClient_JNI::getRegionStats(const char* tblName)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::getRegionStats(%s) called.", tblName);

  if (initJNIEnv() != JOI_OK)
     return NULL;
  jstring js_tblName = jenv_->NewStringUTF(tblName);
  if (js_tblName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_GET_REGION_STATS].jm_full_name;
  jarray j_regionStats = 
    (jarray) jenv_->CallObjectMethod(javaObj_, JavaMethods_[JM_GET_REGION_STATS].methodID, js_tblName);


  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::getRegionStats()");
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  if (jByteArrayList == NULL) {
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
//
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::copy(const char* currTblName, const char* oldTblName)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::copy(%s,%s) called.", currTblName, oldTblName);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  jstring js_currTblName = jenv_->NewStringUTF(currTblName);
  if (js_currTblName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_PARAM;
  }

  jstring js_oldTblName = jenv_->NewStringUTF(oldTblName);
  if (js_oldTblName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DROP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_COPY].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_COPY].methodID, js_currTblName, js_oldTblName);

  jenv_->DeleteLocalRef(js_currTblName);  

  jenv_->DeleteLocalRef(js_oldTblName);  

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::copy()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::copy()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DROP_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}
*/
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::exists(const char* fileName)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::exists(%s) called.", fileName);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  jstring js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_EXISTS_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_EXISTS_PARAM;
  }

  // boolean exists(java.lang.String);
  tsRecentJMFromJNI = JavaMethods_[JM_EXISTS].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_EXISTS].methodID, js_fileName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::exists()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_EXISTS_EXCEPTION;
  }

  if (jresult == false) {
     jenv_->PopLocalFrame(NULL);
     return MC_DONE;  // Table does not exist
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;  // Table exists.
}

/*
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::grant(const Text& user, const Text& tblName, const TextVec& actions)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::grant(%s, %s, %s) called.", user.data(), tblName.data(), actions.data());
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  int len = user.size();
  jbyteArray jba_user = jenv_->NewByteArray(len);
  if (jba_user == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_GRANT_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GRANT_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_user, 0, len, (const jbyte*)user.data());

  len = tblName.size();
  jbyteArray jba_tblName = jenv_->NewByteArray(len);
  if (jba_tblName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_GRANT_PARAM));
    jenv_->DeleteLocalRef(jba_user);  
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GRANT_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_tblName, 0, len, (const jbyte*)tblName.data());

  jobjectArray j_actionCodes = NULL;
  if (!actions.empty())
  {
    QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  Adding %d actions.", actions.size());
    j_actionCodes = convertToStringObjectArray(actions);
    if (j_actionCodes == NULL)
    {
       getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::grant()");
       jenv_->PopLocalFrame(NULL);
       return MC_ERROR_GRANT_PARAM;
    }
  }
  tsRecentJMFromJNI = JavaMethods_[JM_GRANT].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
       JavaMethods_[JM_GRANT].methodID, jba_user, jba_tblName, j_actionCodes);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::grant()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GRANT_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::grant()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GRANT_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// Estimate row count for tblName by adding the entry counts from the trailer
// block of each HFile for the table, and dividing by the number of columns.
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::estimateRowCount(const char* tblName,
                                              Int32 partialRowSize,
                                              Int32 numCols,
                                              Int64& rowCount)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::estimateRowCount(%s) called.", tblName);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  jstring js_tblName = jenv_->NewStringUTF(tblName);
  if (js_tblName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_ROWCOUNT_EST_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_ROWCOUNT_EST_PARAM;
  }

  jint jPartialRowSize = partialRowSize;
  jint jNumCols = numCols;
  jlongArray jRowCount = jenv_->NewLongArray(1);
  tsRecentJMFromJNI = JavaMethods_[JM_EST_RC].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_EST_RC].methodID,
                                              js_tblName, jPartialRowSize,
                                              jNumCols, jRowCount);
  jboolean isCopy;
  jlong* arrayElems = jenv_->GetLongArrayElements(jRowCount, &isCopy);
  rowCount = *arrayElems;
  if (isCopy == JNI_TRUE)
    jenv_->ReleaseLongArrayElements(jRowCount, arrayElems, JNI_ABORT);

  jenv_->DeleteLocalRef(js_tblName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::estimateRowCount()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_ROWCOUNT_EST_EXCEPTION;
  }

  if (jresult == false)
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::estimateRowCount()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_ROWCOUNT_EST_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;  // Table exists.
}

MC_RetCode MonarchClient_JNI::getLatestSnapshot(const char * tblName, char *& snapshotName, NAHeap * heap)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::getLatestSnapshot(%s) called.", tblName);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  jstring js_tblName = jenv_->NewStringUTF(tblName);
  if (js_tblName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_GET_LATEST_SNP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GET_LATEST_SNP_PARAM;
  }
  tsRecentJMFromJNI = JavaMethods_[JM_GET_LATEST_SNP].jm_full_name;
  jstring jresult = (jstring)jenv_->CallObjectMethod(javaObj_, JavaMethods_[JM_GET_LATEST_SNP].methodID,js_tblName);
  if (js_tblName != NULL)
    jenv_->DeleteLocalRef(js_tblName);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::getLatestSnapshot()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GET_LATEST_SNP_EXCEPTION;
  }

  if (jresult == NULL)
    snapshotName = NULL;
  else
  {
    char * tmp = (char*)jenv_->GetStringUTFChars(jresult, NULL);
    snapshotName = new (heap) char[strlen(tmp)+1];
    strcpy(snapshotName, tmp);
    jenv_->ReleaseStringUTFChars(jresult, tmp);
    jenv_->DeleteLocalRef(jresult);
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;  
}
MC_RetCode MonarchClient_JNI::cleanSnpTmpLocation(const char * path)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::cleanSnpTmpLocation(%s) called.", path);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  jstring js_path = jenv_->NewStringUTF(path);
  if (js_path == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CLEAN_SNP_TMP_LOC_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CLEAN_SNP_TMP_LOC_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_CLEAN_SNP_TMP_LOC].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_CLEAN_SNP_TMP_LOC].methodID,js_path);

  if (js_path != NULL)
    jenv_->DeleteLocalRef(js_path);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::cleanSnpTmpLocation()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CLEAN_SNP_TMP_LOC_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}

MC_RetCode MonarchClient_JNI::setArchivePermissions(const char * tbl)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::setArchivePermissions(%s) called.", tbl);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  jstring js_tbl = jenv_->NewStringUTF(tbl);
  if (js_tbl == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_SET_ARC_PERMS_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_SET_ARC_PERMS_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_SET_ARC_PERMS].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_SET_ARC_PERMS].methodID,js_tbl);

  if (js_tbl != NULL)
    jenv_->DeleteLocalRef(js_tbl);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::setArchivePermission()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_SET_ARC_PERMS_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}

MC_RetCode MonarchClient_JNI::getBlockCacheFraction(float& frac)
{
   QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, 
                 "MonarchClient_JNI::getBlockCacheFraction() called.");
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  tsRecentJMFromJNI = JavaMethods_[JM_GET_CAC_FRC].jm_full_name;
  jfloat jresult = jenv_->CallFloatMethod(javaObj_, 
                                          JavaMethods_[JM_GET_CAC_FRC].methodID);
  
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::getBlockCacheFraction()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GET_CACHE_FRAC_EXCEPTION;
  }
  frac = jresult;
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}

MC_RetCode MonarchClient_JNI::revoke(const Text& user, const Text& tblName, const TextVec& actions)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::revoke(%s, %s, %s) called.", user.data(), tblName.data(), actions.data());
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  int len = user.size();
  jbyteArray jba_user = jenv_->NewByteArray(len);
  if (jba_user == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_REVOKE_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_REVOKE_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_user, 0, len, (const jbyte*)user.data());

  len = tblName.size();
  jbyteArray jba_tblName = jenv_->NewByteArray(len);
  if (jba_tblName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_REVOKE_PARAM));
    jenv_->DeleteLocalRef(jba_user);  
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_REVOKE_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_tblName, 0, len, (const jbyte*)tblName.data());

  jobjectArray j_actionCodes = NULL;
  if (!actions.empty())
  {
    QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  Adding %d actions.", actions.size());
    j_actionCodes = convertToStringObjectArray(actions);
    if (j_actionCodes == NULL)
    {
       getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::revoke()");
       jenv_->PopLocalFrame(NULL);
       return MC_ERROR_REVOKE_PARAM;
    }
  }
  tsRecentJMFromJNI = JavaMethods_[JM_REVOKE].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
       JavaMethods_[JM_REVOKE].methodID, jba_user, jba_tblName, j_actionCodes);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::revoke()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_REVOKE_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::revoke()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_REVOKE_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}
*/

////////////////////////////////////////////////////////////////////
void MonarchClient_JNI::logIt(const char* str)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, str);
}

MTableClient_JNI *MonarchClient_JNI::startGet(NAHeap *heap,
					    const char* tableName,
					    bool useTRex,
					    NABoolean replSync,
					    ExHbaseAccessStats *hbs, 
					    Int64 transID,
					    const HbaseStr& rowID, 
					    const LIST(HbaseStr) & cols, 
                                            Int64 timestamp,
                                            const char * hbaseAuths)
{
  if (javaObj_ == NULL || (!isInitialized())) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_GET_MTC_EXCEPTION));
    return NULL;
  }
  if (initJNIEnv() != JOI_OK)
     return NULL;

  MTableClient_JNI *mtc = new (heap) MTableClient_JNI(heap, (jobject)-1);
  if (mtc->init() != MTC_OK) {
     NADELETE(mtc, MTableClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return NULL;
  }
  mtc->setTableName(tableName);
  mtc->setHbaseStats(hbs);
  mtc->setFetchMode(MTableClient_JNI::GET_ROW);

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_STARTGET_EXCEPTION));
    NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
  int len = rowID.len;
  jbyteArray jba_rowID = jenv_->NewByteArray(len);
  if (jba_rowID == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_STARTGET_EXCEPTION));
     NADELETE(mtc, MTableClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return NULL;;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, len, (const jbyte*)rowID.val);
  jobjectArray j_cols = NULL;
  if (!cols.isEmpty()) {
     j_cols = convertToByteArrayObjectArray(cols);
     if (j_cols == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::startGet()");
        NADELETE(mtc, MTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return NULL;
     }  
     mtc->setNumColsInScan(cols.entries());
  }
  else
     mtc->setNumColsInScan(0);
  mtc->setNumReqRows(1);
  jlong j_tid = transID;  
  jlong j_ts = timestamp;
  
  jstring js_hbaseAuths = NULL;
  if (hbaseAuths)
    {
      js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
      if (js_hbaseAuths == NULL)
        {
          GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_STARTGET_EXCEPTION));
          NADELETE(mtc, MTableClient_JNI, heap);
          jenv_->PopLocalFrame(NULL);
          return NULL;
        }
    }
  
  if (hbs)
    hbs->getHbaseTimer().start();

  tsRecentJMFromJNI = JavaMethods_[JM_START_GET].jm_full_name;
  jint jresult = jenv_->CallIntMethod(javaObj_, 
				      JavaMethods_[JM_START_GET].methodID, 
				      (jlong)mtc,
				      js_tblName,
				      (jboolean)useTRex, 
				      (jboolean) replSync,
				      j_tid,
				      jba_rowID,
				      j_cols, j_ts,
                                      js_hbaseAuths);
  if (hbs) {
      hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
      hbs->incHbaseCalls();
  }

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::startGet()");
    jenv_->PopLocalFrame(NULL);
    releaseMTableClient(mtc);
    return NULL;
  }

  if (jresult == 0) 
     mtc->setNumRowsReturned(-1);
  else
     mtc->setNumRowsReturned(1);
  jenv_->PopLocalFrame(NULL);
  return mtc;
}

MTableClient_JNI *MonarchClient_JNI::startGets(NAHeap *heap,
					     const char* tableName,
					     bool useTRex,
					     NABoolean replSync, 
					     ExHbaseAccessStats *hbs,
					     Int64 transID,
					     const LIST(HbaseStr) *rowIDs, 
					     short rowIDLen,
					     const HbaseStr *rowIDsInDB,
					     const LIST(HbaseStr) & cols,
					     Int64 timestamp,
                                             const char * hbaseAuths)
{
  if (javaObj_ == NULL || (!isInitialized())) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_GET_MTC_EXCEPTION));
    return NULL;
  }

  if (initJNIEnv() != JOI_OK)
     return NULL;

  MTableClient_JNI *mtc = new (heap) MTableClient_JNI(heap, (jobject)-1);
  if (mtc->init() != MTC_OK) {
     NADELETE(mtc, MTableClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return NULL;
  }
  mtc->setTableName(tableName);
  mtc->setHbaseStats(hbs);
  mtc->setFetchMode(MTableClient_JNI::BATCH_GET);

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_STARTGET_EXCEPTION));
    NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
  jobjectArray j_cols = NULL;
  if (!cols.isEmpty()) {
     j_cols = convertToByteArrayObjectArray(cols);
     if (j_cols == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::startGets()");
        NADELETE(mtc, MTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return NULL;
     }  
     mtc->setNumColsInScan(cols.entries());
  }
  else
     mtc->setNumColsInScan(0);
  jobjectArray	j_rows = NULL;
  jobject       jRowIDs = NULL;

  if (rowIDs != NULL) {
     j_rows = convertToByteArrayObjectArray(*rowIDs);
     if (j_rows == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::startGets()");
        NADELETE(mtc, MTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return NULL;
     }  
     mtc->setNumReqRows(rowIDs->entries());
  } else {
     jRowIDs = jenv_->NewDirectByteBuffer(rowIDsInDB->val, rowIDsInDB->len);
     if (jRowIDs == NULL) {
        GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_STARTGET_EXCEPTION));
        NADELETE(mtc, MTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return NULL;
     }
     // Need to swap the bytes 
     short numReqRows = *(short *)rowIDsInDB->val;
     mtc->setNumReqRows(bswap_16(numReqRows));
  }

  jlong j_tid = transID;  
  jlong j_ts = timestamp;
  jshort jRowIDLen = rowIDLen; 

  jstring js_hbaseAuths = NULL;
  if (hbaseAuths)
    {
      js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
      if (js_hbaseAuths == NULL)
        {
          GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_STARTGETS_EXCEPTION));
          NADELETE(mtc, MTableClient_JNI, heap);
          jenv_->PopLocalFrame(NULL);
          return NULL;
        }
    }

  if (hbs)
    hbs->getHbaseTimer().start();

  jint jresult;
  if (rowIDs != NULL) {
     tsRecentJMFromJNI = JavaMethods_[JM_START_GETS].jm_full_name;
     jresult = jenv_->CallIntMethod(javaObj_, 
            JavaMethods_[JM_START_GETS].methodID, 
	(jlong)mtc, js_tblName, (jboolean)useTRex, (jboolean) replSync, j_tid, j_rows,
                                    j_cols, j_ts, js_hbaseAuths);
  } else {
    tsRecentJMFromJNI = JavaMethods_[JM_START_DIRECT_GETS].jm_full_name;
    jresult = jenv_->CallIntMethod(javaObj_, 
                                   JavaMethods_[JM_START_DIRECT_GETS].methodID, 
                                   (jlong)mtc, js_tblName, (jboolean)useTRex, (jboolean) replSync, j_tid, jRowIDLen, jRowIDs,
                                   j_cols, js_hbaseAuths);
  }
  if (hbs) {
      hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
      hbs->incHbaseCalls();
  }

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::startGets()");
    jenv_->PopLocalFrame(NULL);
    releaseMTableClient(mtc);
    return NULL;
  }

  if (jresult == 0) 
     mtc->setNumRowsReturned(-1);
  else
     mtc->setNumRowsReturned(jresult);
  jenv_->PopLocalFrame(NULL);
  return mtc;
}

MC_RetCode MonarchClient_JNI::getRegionsNodeName(const char* tblName,
                                                Int32 partns,
                                                ARRAY(const char *)& nodeNames)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::getRegionsNodeName(%s) called.", tblName);
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  jstring js_tblName = jenv_->NewStringUTF(tblName);
  if (js_tblName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_GET_HBTI_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GET_HBTI_PARAM;
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
    jenv_->DeleteLocalRef(strObj);
  }

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::getRegionsNodeName()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GET_HBTI_EXCEPTION;
  }

  if (jresult == false)
  {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::getRegionsNodeName()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GET_HBTI_EXCEPTION;
  }
  jenv_->PopLocalFrame(NULL);
  return MC_OK;  // Table exists.
}
/*
//////////////////////////////////////////////////////////////////////////////
// Get Hbase Table information. Currently the following info is requested:
// 1. index levels : This info is obtained from trailer block of Hfiles of randomly chosen region
// 2. block size : This info is obtained for HColumnDescriptor
////////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::getHbaseTableInfo(const char* tblName,
                                              Int32& indexLevels,
                                              Int32& blockSize)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::getHbaseTableInfo(%s) called.", tblName);

  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;
  jstring js_tblName = jenv_->NewStringUTF(tblName);
  if (js_tblName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_GET_HBTI_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GET_HBTI_PARAM;
  }

  jintArray jHtabInfo = jenv_->NewIntArray(2);
  tsRecentJMFromJNI = JavaMethods_[JM_GET_HBTI].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_GET_HBTI].methodID,
                                              js_tblName, jHtabInfo);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::getHbaseTableInfo()");
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_GET_HBTI_EXCEPTION;
  }
  jboolean isCopy;
  jint* arrayElems = jenv_->GetIntArrayElements(jHtabInfo, &isCopy);
  indexLevels = arrayElems[0];
  blockSize = arrayElems[1];
  if (isCopy == JNI_TRUE)
     jenv_->ReleaseIntArrayElements(jHtabInfo, arrayElems, JNI_ABORT);
  jenv_->PopLocalFrame(NULL);
  return MC_OK;  // Table exists.
}
*/
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::insertRow(NAHeap *heap,
				       const char *tableName,
				       ExHbaseAccessStats *hbs,
				       bool useTRex,
				       NABoolean replSync, 
				       Int64 transID,
				       HbaseStr rowID,
				       HbaseStr row,
				       Int64 timestamp,
				       bool checkAndPut,
				       bool asyncOperation,
				       bool useRegionTx,
				       MTableClient_JNI **outHtc)
{
  
  MTableClient_JNI *mtc = NULL;

  if (asyncOperation) {
     mtc = new (heap) MTableClient_JNI(heap, (jobject)-1);
     if (mtc->init() != MTC_OK) {
         NADELETE(mtc, MTableClient_JNI, heap);
         return MC_ERROR_INSERTROW_EXCEPTION;
     }
     mtc->setTableName(tableName);
     mtc->setHbaseStats(hbs);
  }
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INSERTROW_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_INSERTROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_INSERTROW_PARAM;
  }
  jbyteArray jba_rowID = jenv_->NewByteArray(rowID.len);
  if (jba_rowID == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_INSERTROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_INSERTROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowID.len, (const jbyte*)rowID.val);
  jobject jRow = jenv_->NewDirectByteBuffer(row.val, row.len);
  if (jRow == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_INSERTROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_INSERTROW_PARAM;
  }
  jlong j_mtc = (long)mtc;
  jboolean j_useTRex = useTRex;
  jlong j_tid = transID;  
  jlong j_ts = timestamp;
  jboolean j_checkAndPut = checkAndPut;
  jboolean j_asyncOperation = asyncOperation;
 
  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_MC_DIRECT_INSERT_ROW].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_,
					      JavaMethods_[JM_MC_DIRECT_INSERT_ROW].methodID,
					      j_mtc,
					      js_tblName,
					      j_useTRex,
					      (jboolean) replSync,
					      j_tid,
					      jba_rowID,
					      jRow,
					      j_ts,
					      j_checkAndPut,
					      j_asyncOperation); 
  if (hbs) {
      hbs->incHbaseCalls();
      if (!asyncOperation)
         hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::insertRow()");
    jenv_->PopLocalFrame(NULL);
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    return MC_ERROR_INSERTROW_EXCEPTION;
  }
  if (hbs)
    hbs->incBytesRead(rowID.len + row.len);
  if (jresult == false) {
     jenv_->PopLocalFrame(NULL);
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
     return MC_ERROR_INSERTROW_DUP_ROWID;
  }
  *outHtc = mtc;
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::insertRows(NAHeap *heap, const char *tableName,
					ExHbaseAccessStats *hbs,
					bool useTRex,
					NABoolean replSync,
					Int64 transID,
					short rowIDLen,
					HbaseStr rowIDs,
					HbaseStr rows,
					Int64 timestamp,
					bool asyncOperation,
					MTableClient_JNI **outHtc)
{
  
  MTableClient_JNI *mtc = NULL;

  if (asyncOperation) {
     mtc = new (heap) MTableClient_JNI(heap, (jobject)-1);
     if (mtc->init() != MTC_OK) {
         NADELETE(mtc, MTableClient_JNI, heap);
         return MC_ERROR_INSERTROWS_EXCEPTION;
     }
     mtc->setTableName(tableName);
     mtc->setHbaseStats(hbs);
  }
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_INSERTROWS_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_INSERTROWS_PARAM;
  }
  jobject jRowIDs = jenv_->NewDirectByteBuffer(rowIDs.val, rowIDs.len);
  if (jRowIDs == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_INSERTROWS_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_INSERTROWS_PARAM;
  }
  
  jobject jRows = jenv_->NewDirectByteBuffer(rows.val, rows.len);
  if (jRows == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_INSERTROWS_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_INSERTROWS_PARAM;
  }
  jboolean j_useTRex = useTRex;
  jlong j_tid = transID;  
  jlong j_ts = timestamp;
  jlong j_mtc = (long)mtc;
  jshort j_rowIDLen = rowIDLen;
  jboolean j_asyncOperation = asyncOperation;
 
  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_MC_DIRECT_INSERT_ROWS].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_,
					      JavaMethods_[JM_MC_DIRECT_INSERT_ROWS].methodID, 
					      j_mtc,
					      js_tblName,
					      j_useTRex,
					      (jboolean) replSync,
					      j_tid,
					      j_rowIDLen,
					      jRowIDs,
					      jRows,
					      j_ts,
					      j_asyncOperation);
  if (hbs) {
      hbs->incHbaseCalls();
      if (!asyncOperation)
         hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::insertRows()");
    jenv_->PopLocalFrame(NULL);
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    return MC_ERROR_INSERTROWS_EXCEPTION;
  }
  if (jresult == false) {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::insertRows()", getLastError());
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_INSERTROWS_EXCEPTION;
  }
  if (hbs)
    hbs->incBytesRead(rowIDs.len + rows.len);
  *outHtc = mtc;
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}
/*
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::updateVisibility(NAHeap *heap, const char *tableName,
                                        ExHbaseAccessStats *hbs, 
                                        bool useTRex, Int64 transID, 
                                        HbaseStr rowID,
                                        HbaseStr tagsRow, 
                                        MTableClient_JNI **outHtc)
{
  
  MTableClient_JNI *mtc = NULL;

  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_UPDATEVISIBILITY_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_UPDATEVISIBILITY_PARAM;
  }
  jbyteArray jba_rowID = jenv_->NewByteArray(rowID.len);
  if (jba_rowID == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_INSERTROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_UPDATEVISIBILITY_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowID.len, (const jbyte*)rowID.val);

  jobject jTagsRow = jenv_->NewDirectByteBuffer(tagsRow.val, tagsRow.len);
  if (jTagsRow == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_UPDATEVISIBILITY_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_UPDATEVISIBILITY_PARAM;
  }
  jboolean j_useTRex = useTRex;
  jlong j_tid = transID;  
  jlong j_mtc = (long)mtc;
 
  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_MC_DIRECT_UPDATE_TAGS].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_MC_DIRECT_UPDATE_TAGS].methodID, 
               	j_mtc, js_tblName, j_useTRex, j_tid, jba_rowID, jTagsRow);
  if (hbs) {
      hbs->incHbaseCalls();
      hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::updateVisibility()");
    jenv_->PopLocalFrame(NULL);
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    return MC_ERROR_UPDATEVISIBILITY_EXCEPTION;
  }
  if (jresult == false) {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::updateVisibility()", getLastError());
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_UPDATEVISIBILITY_EXCEPTION;
  }
  //  if (hbs)
  //    hbs->incBytesRead(rowIDs.len + rows.len);
  *outHtc = mtc;
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}
*/
//
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::checkAndUpdateRow(NAHeap *heap, const char *tableName,
					       ExHbaseAccessStats *hbs,
					       bool useTRex,
					       NABoolean replSync,
					       Int64 transID,
					       HbaseStr rowID,
					       HbaseStr row,
					       HbaseStr columnToCheck,
					       HbaseStr columnValToCheck,
					       Int64 timestamp,
					       bool asyncOperation,
				               bool useRegionTx,
					       MTableClient_JNI **outHtc)
{
  
  MTableClient_JNI *mtc = NULL;

  if (asyncOperation) {
     mtc = new (heap) MTableClient_JNI(heap, (jobject)-1);
     if (mtc->init() != MTC_OK) {
         NADELETE(mtc, MTableClient_JNI, heap);
         return MC_ERROR_CHECKANDUPDATEROW_EXCEPTION;
     }
     mtc->setTableName(tableName);
     mtc->setHbaseStats(hbs);
  }
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CHECKANDUPDATEROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CHECKANDUPDATEROW_PARAM;
  }
  jbyteArray jba_rowID = jenv_->NewByteArray(rowID.len);
  if (jba_rowID == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CHECKANDUPDATEROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CHECKANDUPDATEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowID.len, (const jbyte*)rowID.val);
  
  jobject jRow = jenv_->NewDirectByteBuffer(row.val, row.len);
  if (jRow == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CHECKANDUPDATEROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CHECKANDUPDATEROW_PARAM;
  }

  jbyteArray jba_columnToCheck = jenv_->NewByteArray(columnToCheck.len);
  if (jba_columnToCheck == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CHECKANDUPDATEROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CHECKANDUPDATEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_columnToCheck, 0, columnToCheck.len, (const jbyte*)columnToCheck.val);

  jbyteArray jba_columnValToCheck = jenv_->NewByteArray(columnValToCheck.len);
  if (jba_columnValToCheck == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CHECKANDUPDATEROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CHECKANDUPDATEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_columnValToCheck, 0, columnValToCheck.len, (const jbyte*)columnValToCheck.val);
  jlong j_mtc = (long)mtc;
  jboolean j_useTRex = useTRex;
  jlong j_tid = transID;  
  jlong j_ts = timestamp;
  jboolean j_asyncOperation = asyncOperation;
 
  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_MC_DIRECT_CHECKANDUPDATE_ROW].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_,
					      JavaMethods_[JM_MC_DIRECT_CHECKANDUPDATE_ROW].methodID, 
					      j_mtc,
					      js_tblName,
					      j_useTRex,
					      (jboolean) replSync,
					      j_tid,
					      jba_rowID,
					      jRow,
					      jba_columnToCheck,
					      jba_columnValToCheck,
					      j_ts, j_asyncOperation);
  if (hbs) {
      hbs->incHbaseCalls();
      if (!asyncOperation)
         hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::checkAndUpdateRow()");
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CHECKANDUPDATEROW_EXCEPTION;
  }
  if (hbs)
    hbs->incBytesRead(rowID.len + row.len);
  if (jresult == false) {
     GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CHECKANDUPDATEROW_NOTFOUND));
     jenv_->PopLocalFrame(NULL);
     if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
     return MC_ERROR_CHECKANDUPDATEROW_NOTFOUND;
  }
  *outHtc = mtc;
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::deleteRow(NAHeap *heap, const char *tableName,
				       ExHbaseAccessStats *hbs,
				       bool useTRex,
				       NABoolean replSync,
				       Int64 transID,
				       HbaseStr rowID,
				       const LIST(HbaseStr) *cols, 
				       Int64 timestamp,
				       bool asyncOperation,
				       bool useRegionTx,
                                       const char * hbaseAuths,
				       MTableClient_JNI **outHtc)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::deleteRow(%ld, %s) called.", transID, rowID.val);

  MTableClient_JNI *mtc = NULL;

  if (asyncOperation) {
     mtc = new (heap) MTableClient_JNI(heap, (jobject)-1);
     if (mtc->init() != MTC_OK) {
         NADELETE(mtc, MTableClient_JNI, heap);
         return MC_ERROR_DELETEROW_EXCEPTION;
     }
     mtc->setTableName(tableName);
     mtc->setHbaseStats(hbs);
  }
  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DELETEROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DELETEROW_PARAM;
  }
  jbyteArray jba_rowID = jenv_->NewByteArray(rowID.len);
  if (jba_rowID == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DELETEROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return MC_ERROR_DELETEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowID.len, (const jbyte*)rowID.val);
  jobjectArray j_cols = NULL;
  if (cols != NULL && !cols->isEmpty()) {
     j_cols = convertToByteArrayObjectArray(*cols);
     if (j_cols == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::deleteRow()");
        if (mtc != NULL)
           NADELETE(mtc, MTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return MC_ERROR_DELETEROW_PARAM;
     }
  }  
  jlong j_mtc = (jlong)mtc;
  jboolean j_useTRex = useTRex;
  jlong j_tid = transID;  
  jlong j_ts = timestamp;
  jboolean j_asyncOperation = asyncOperation;

  jstring js_hbaseAuths = NULL;
  if (hbaseAuths)
    {
      js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
      if (js_hbaseAuths == NULL)
        {
          GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DELETEROW_PARAM));
          NADELETE(mtc, MTableClient_JNI, heap);
          jenv_->PopLocalFrame(NULL);
          return MC_ERROR_DELETEROW_PARAM;
        }
    }
 
  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_MC_DELETE_ROW].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
          JavaMethods_[JM_MC_DELETE_ROW].methodID,
					      j_mtc,
					      js_tblName,
					      j_useTRex,
					      (jboolean) replSync,
					      j_tid,
					      jba_rowID,
					      j_cols,
					      j_ts,
					      j_asyncOperation,
                                              js_hbaseAuths);
  if (hbs) {
      hbs->incHbaseCalls();
      if (!asyncOperation) 
         hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::deleteRow()");
    jenv_->PopLocalFrame(NULL);
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    return MC_ERROR_DELETEROW_EXCEPTION;
  }

  if (jresult == false) {
     logError(CAT_SQL_HBASE, "MonarchClient_JNI::deleteRow()", getLastError());
     jenv_->PopLocalFrame(NULL);
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
     return MC_ERROR_DELETEROW_EXCEPTION;
  }

  if (hbs)
    hbs->incBytesRead(rowID.len);
  *outHtc = mtc;
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::deleteRows(NAHeap *heap,
					const char *tableName,
					ExHbaseAccessStats *hbs,
					bool useTRex,
					NABoolean replSync,
					Int64 transID,
					short rowIDLen,
					HbaseStr rowIDs, 
                                        const LIST(HbaseStr) *cols, 
					Int64 timestamp,
					bool asyncOperation,
                                        const char * hbaseAuths,
					MTableClient_JNI **outHtc)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::deleteRows(%ld, %s) called.", transID, rowIDs.val);

  MTableClient_JNI *mtc = NULL;

  if (asyncOperation) {
      mtc = new (heap) MTableClient_JNI(heap, (jobject)-1);
     if (mtc->init() != MTC_OK) {
         NADELETE(mtc, MTableClient_JNI, heap);
         return MC_ERROR_DELETEROWS_EXCEPTION;
     }
     mtc->setTableName(tableName);
     mtc->setHbaseStats(hbs);
  }
  if (initJNIEnv() != JOI_OK)
    return MC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DELETEROWS_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DELETEROWS_PARAM;
  }

  jobject jRowIDs = jenv_->NewDirectByteBuffer(rowIDs.val, rowIDs.len);
  if (jRowIDs == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DELETEROWS_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_DELETEROWS_PARAM;
  }

  jobjectArray j_cols = NULL;
  if (cols != NULL && !cols->isEmpty()) {
     j_cols = convertToByteArrayObjectArray(*cols);
     if (j_cols == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::deleteRows()");
        if (mtc != NULL)
           NADELETE(mtc, MTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return MC_ERROR_DELETEROW_PARAM;
     }
  }  

  jlong j_mtc = (jlong)mtc;
  jboolean j_useTRex = useTRex;
  jlong j_tid = transID;  
  jshort j_rowIDLen = rowIDLen;
  jlong j_ts = timestamp;
  jboolean j_asyncOperation = asyncOperation;

  jstring js_hbaseAuths = NULL;
  if (hbaseAuths)
    {
      js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
      if (js_hbaseAuths == NULL)
        {
          GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_DELETEROWS_PARAM));
          NADELETE(mtc, MTableClient_JNI, heap);
          jenv_->PopLocalFrame(NULL);
          return MC_ERROR_DELETEROWS_PARAM;
        }
    }

  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_MC_DIRECT_DELETE_ROWS].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
					      JavaMethods_[JM_MC_DIRECT_DELETE_ROWS].methodID,
					      j_mtc,
					      js_tblName,
					      j_useTRex,
					      (jboolean) replSync,
					      j_tid,
					      j_rowIDLen,
					      jRowIDs,
                                             // j_cols,
					      j_ts,
					      j_asyncOperation,
                                              js_hbaseAuths);
  if (hbs) {
      hbs->incHbaseCalls();
      if (!asyncOperation) 
         hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::deleteRows()");
    jenv_->PopLocalFrame(NULL);
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    return MC_ERROR_DELETEROWS_EXCEPTION;
  }
  if (jresult == false) {
    logError(CAT_SQL_HBASE, "MonarchClient_JNI::deleteRows()", getLastError());
    jenv_->PopLocalFrame(NULL);
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    return MC_ERROR_DELETEROWS_EXCEPTION;
  }
  if (hbs)
    hbs->incBytesRead(rowIDs.len);
  *outHtc = mtc;
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}
//
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MC_RetCode MonarchClient_JNI::checkAndDeleteRow(NAHeap *heap,
					       const char *tableName,
					       ExHbaseAccessStats *hbs,
					       bool useTRex,
					       NABoolean replSync,
					       Int64 transID,
					       HbaseStr rowID, 
                                               const LIST(HbaseStr) *cols, 
					       HbaseStr columnToCheck,
					       HbaseStr columnValToCheck,
					       Int64 timestamp,
					       bool asyncOperation,
				               bool useRegionTx,
                                               const char * hbaseAuths,
					       MTableClient_JNI **outHtc)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MonarchClient_JNI::checkAndDeleteRow(%ld, %s) called.", transID, rowID.val);

  MTableClient_JNI *mtc = NULL;

  if (asyncOperation) {
     mtc = new (heap) MTableClient_JNI(heap, (jobject)-1);
     if (mtc->init() != MTC_OK) {
         NADELETE(mtc, MTableClient_JNI, heap);
         return MC_ERROR_CHECKANDDELETEROW_EXCEPTION;
     }
     mtc->setTableName(tableName);
     mtc->setHbaseStats(hbs);
  }
  if (initJNIEnv() != JOI_OK)
    return MC_ERROR_INIT_PARAM;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CHECKANDDELETEROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    jenv_->PopLocalFrame(NULL);
    return MC_ERROR_CHECKANDDELETEROW_PARAM;
  }
  jbyteArray jba_rowID = jenv_->NewByteArray(rowID.len);
  if (jba_rowID == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CHECKANDDELETEROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return MC_ERROR_CHECKANDDELETEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowID.len, (const jbyte*)rowID.val);
  jobjectArray j_cols = NULL;
  if (cols != NULL && !cols->isEmpty()) {
     j_cols = convertToByteArrayObjectArray(*cols);
     if (j_cols == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::checkAndDeleteRow()");
        if (mtc != NULL)
           NADELETE(mtc, MTableClient_JNI, heap);
        jenv_->PopLocalFrame(NULL);
        return MC_ERROR_DELETEROW_PARAM;
     }
  }  
  jbyteArray jba_columnValToCheck = jenv_->NewByteArray(columnValToCheck.len);
  if (jba_columnValToCheck == NULL) {
     GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CHECKANDDELETEROW_PARAM));
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
     jenv_->PopLocalFrame(NULL);
     return MC_ERROR_CHECKANDDELETEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_columnValToCheck, 0, columnValToCheck.len, (const jbyte*)columnValToCheck.val);
  jlong j_mtc = (jlong)mtc;
  jboolean j_useTRex = useTRex;
  jlong j_tid = transID;  
  jlong j_ts = timestamp;
  jboolean j_asyncOperation = asyncOperation;

  jstring js_hbaseAuths = NULL;
  if (hbaseAuths)
    {
      js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
      if (js_hbaseAuths == NULL)
        {
          GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_CHECKANDDELETEROW_PARAM));
          if (mtc != NULL)
            NADELETE(mtc, MTableClient_JNI, heap);
          jenv_->PopLocalFrame(NULL);
          return MC_ERROR_CHECKANDDELETEROW_PARAM;
        }
    }

  if (hbs)
    hbs->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_MC_CHECKANDDELETE_ROW].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
					      JavaMethods_[JM_MC_CHECKANDDELETE_ROW].methodID,
					      j_mtc,
					      js_tblName,
					      j_useTRex,
					      (jboolean) replSync, 
					      j_tid,
					      jba_rowID, 
                                              j_cols,
					      jba_columnValToCheck,
					      j_ts,
					      j_asyncOperation,
                                              js_hbaseAuths);
  if (hbs) {
      hbs->incHbaseCalls();
      if (!asyncOperation) 
         hbs->incMaxHbaseIOTime(hbs->getHbaseTimer().stop());
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::checkAndDeleteRow()");
    jenv_->PopLocalFrame(NULL);
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
    return MC_ERROR_CHECKANDDELETEROW_EXCEPTION;
  }

  if (jresult == false) {
     logError(CAT_SQL_HBASE, "MonarchClient_JNI::checkAndDeleteRow()", getLastError());
     jenv_->PopLocalFrame(NULL);
    if (mtc != NULL)
        NADELETE(mtc, MTableClient_JNI, heap);
     return MC_ERROR_CHECKANDDELETEROW_NOTFOUND;
  }

  if (hbs)
    hbs->incBytesRead(rowID.len);
  *outHtc = mtc;
  jenv_->PopLocalFrame(NULL);
  return MC_OK;
}

// ===========================================================================
// ===== Class MTableClient
// ===========================================================================

JavaMethodInit* MTableClient_JNI::JavaMethods_ = NULL;
jclass MTableClient_JNI::javaClass_ = 0;
bool MTableClient_JNI::javaMethodsInitialized_ = false;
pthread_mutex_t MTableClient_JNI::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

static const char* const mtcErrorEnumStr[] = 
{
  "Preparing parameters for initConnection()."
 ,"Java exception in initConnection()."
 ,"Java exception in setTransactionID()."
 ,"Java exception in cleanup()."
 ,"Java exception in close()."
 ,"Preparing parameters for scanOpen()."
 ,"Java exception in scanOpen()."
 ,"Java exception in fetchRows()."
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
 ,"Java exception in getMTableName()."
 ,"Java exception in getColName()."
 ,"Java exception in getColValue()."
 ,"Java exception in getRowID()."
 ,"Java exception in nextCell()."
 ,"Java exception in completeAsyncOperation()."
 ,"Async Hbase Operation not yet complete."
 ,"Java exception in setWriteToWal()."
 ,"Java exception in setWriteBufferSize()."
};

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
char* MTableClient_JNI::getErrorText(MTC_RetCode errEnum)
{
  if (errEnum < (MTC_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)errEnum);
  else    
    return (char*)mtcErrorEnumStr[errEnum-MTC_FIRST-1];
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MTableClient_JNI::~MTableClient_JNI()
{
  //QRLogger::log(CAT_JNI_TOP, LL_DEBUG, "MTableClient destructor called.");
  cleanupResultInfo();
  if (tableName_ != NULL)
  {
     NADELETEBASIC(tableName_, heap_);
  }
  if (colNameAllocLen_ != 0)
     NADELETEBASIC(colName_, heap_);
  if (numCellsAllocated_ > 0)
  {
      NADELETEBASIC(p_cellsValLen_, heap_);
      NADELETEBASIC(p_cellsValOffset_, heap_);
      NADELETEBASIC(p_timestamp_, heap_);
      numCellsAllocated_ = 0;
  }
}
 
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MTC_RetCode MTableClient_JNI::init()
{
  static char className[]="org/trafodion/sql/MTableClient";
  MTC_RetCode rc;
  
  if (isInitialized())
    return MTC_OK;
  
  if (javaMethodsInitialized_)
    return (MTC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
  else
  {
    pthread_mutex_lock(&javaMethodsInitMutex_);
    if (javaMethodsInitialized_)
    {
      pthread_mutex_unlock(&javaMethodsInitMutex_);
      return (MTC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    }
    JavaMethods_ = new JavaMethodInit[JM_LAST];
    
    JavaMethods_[JM_CTOR       ].jm_name      = "<init>";
    JavaMethods_[JM_CTOR       ].jm_signature = "()V";
    JavaMethods_[JM_SCAN_OPEN  ].jm_name      = "startScan";
    JavaMethods_[JM_SCAN_OPEN  ].jm_signature = "(J[B[B[Ljava/lang/Object;JZZI[Ljava/lang/Object;[Ljava/lang/Object;[Ljava/lang/Object;FZZILjava/lang/String;Ljava/lang/String;IIJJLjava/lang/String;)Z";
    JavaMethods_[JM_DELETE     ].jm_name      = "deleteRow";
    JavaMethods_[JM_DELETE     ].jm_signature = "(J[B[Ljava/lang/Object;JZLjava/lang/String;)Z";
    JavaMethods_[JM_COPROC_AGGR     ].jm_name      = "coProcAggr";
    JavaMethods_[JM_COPROC_AGGR     ].jm_signature = "(JI[B[B[B[BZI)[B";
    JavaMethods_[JM_GET_NAME   ].jm_name      = "getTableName";
    JavaMethods_[JM_GET_NAME   ].jm_signature = "()Ljava/lang/String;";
    JavaMethods_[JM_GET_HTNAME ].jm_name      = "getMTableName";
    JavaMethods_[JM_GET_HTNAME ].jm_signature = "()Ljava/lang/String;";
/*
    JavaMethods_[JM_SET_WB_SIZE ].jm_name      = "setWriteBufferSize";
    JavaMethods_[JM_SET_WB_SIZE ].jm_signature = "(J)Z";
    JavaMethods_[JM_SET_WRITE_TO_WAL ].jm_name      = "setWriteToWAL";
    JavaMethods_[JM_SET_WRITE _TO_WAL ].jm_signature = "(Z)Z";
*/
    JavaMethods_[JM_FETCH_ROWS ].jm_name      = "fetchRows";
    JavaMethods_[JM_FETCH_ROWS ].jm_signature = "()I";
    JavaMethods_[JM_COMPLETE_PUT ].jm_name      = "completeAsyncOperation";
    JavaMethods_[JM_COMPLETE_PUT ].jm_signature = "(I[Z)Z";
    rc = (MTC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    if (rc == MTC_OK)
       javaMethodsInitialized_ = TRUE;
    pthread_mutex_unlock(&javaMethodsInitMutex_);
  }
  return rc;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MTC_RetCode MTableClient_JNI::startScan(Int64 transID, const Text& startRowID, 
   const Text& stopRowID, const LIST(HbaseStr) & cols, Int64 timestamp, 
   bool cacheBlocks, bool smallScanner, Lng32 numCacheRows, NABoolean preFetch,
					const LIST(NAString) *inColNamesToFilter, 
					const LIST(NAString) *inCompareOpList,
					const LIST(NAString) *inColValuesToCompare,
					Float32 samplePercent,
					NABoolean useSnapshotScan,
					Lng32 snapTimeout,
					char * snapName ,
					char * tmpLoc,
					Lng32 espNum,
                                        Lng32 versions,
                                        Int64 minTS,
                                        Int64 maxTS,
                                        const char * hbaseAuths)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MTableClient_JNI::startScan() called.");

  if (initJNIEnv() != JOI_OK)
     return MTC_ERROR_INIT_PARAM;

  int len = startRowID.size();
  jbyteArray jba_startRowID = jenv_->NewByteArray(len);
  if (jba_startRowID == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_SCANOPEN_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_SCANOPEN_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_startRowID, 0, len, (const jbyte*)startRowID.data());

  len = stopRowID.size();
  jbyteArray jba_stopRowID = jenv_->NewByteArray(len);
  if (jba_stopRowID == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_SCANOPEN_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_SCANOPEN_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_stopRowID, 0, len, (const jbyte*)stopRowID.data());

  jobjectArray j_cols = NULL;
  if (!cols.isEmpty())
  {
    j_cols = convertToByteArrayObjectArray(cols);
    if (j_cols == NULL)
    {
       getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::startScan()");
       jenv_->PopLocalFrame(NULL);
       return MTC_ERROR_SCANOPEN_PARAM;
    }
    numColsInScan_ = cols.entries();
  }
  else
     numColsInScan_ = 0;
  jlong j_tid = transID;  
  jlong j_ts = timestamp;

  jboolean j_cb = cacheBlocks;
  jboolean j_smallScanner = smallScanner;
  jboolean j_preFetch = preFetch;
  jint j_ncr = numCacheRows;
  numReqRows_ = numCacheRows;
  currentRowNum_ = -1;
  currentRowCellNum_ = -1;

  jobjectArray j_colnamestofilter = NULL;
  if ((inColNamesToFilter) && (!inColNamesToFilter->isEmpty()))
  {
    j_colnamestofilter = convertToByteArrayObjectArray(*inColNamesToFilter);
    if (j_colnamestofilter == NULL)
    {
       getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::startScan()");
       jenv_->PopLocalFrame(NULL);
       return MTC_ERROR_SCANOPEN_PARAM;
    }
  }

  jobjectArray j_compareoplist = NULL;
  if ((inCompareOpList) && (! inCompareOpList->isEmpty()))
  {
     j_compareoplist = convertToByteArrayObjectArray(*inCompareOpList);
     if (j_compareoplist == NULL)
     {
        getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::startScan()");
        jenv_->PopLocalFrame(NULL);
        return MTC_ERROR_SCANOPEN_PARAM;
     }
  }

  jobjectArray j_colvaluestocompare = NULL;
  if ((inColValuesToCompare) && (!inColValuesToCompare->isEmpty()))
  {
     j_colvaluestocompare = convertToByteArrayObjectArray(*inColValuesToCompare);
     if (j_colvaluestocompare == NULL)
     {
        getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::startScan()");
        jenv_->PopLocalFrame(NULL);
        return MTC_ERROR_SCANOPEN_PARAM;
     }
  }
  jfloat j_smplPct = samplePercent;
  jboolean j_useSnapshotScan = useSnapshotScan;
  jint j_snapTimeout = snapTimeout;
  jint j_espNum = espNum;
  jint j_versions = versions;

  jlong j_minTS = minTS;  
  jlong j_maxTS = maxTS;  

  jstring js_snapName = jenv_->NewStringUTF(snapName != NULL ? snapName : "Dummy");
   if (js_snapName == NULL)
   {
     GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_SCANOPEN_PARAM));
     jenv_->PopLocalFrame(NULL);
     return MTC_ERROR_SCANOPEN_PARAM;
   }
  jstring js_tmp_loc = jenv_->NewStringUTF(tmpLoc != NULL ? tmpLoc : "Dummy");
   if (js_tmp_loc == NULL)
   {
     GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_SCANOPEN_PARAM));
     //delete the previous string in case of error
     jenv_->PopLocalFrame(NULL);
     return MTC_ERROR_SCANOPEN_PARAM;
   }

   jstring js_hbaseAuths = NULL;
   if (hbaseAuths)
     {
       js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
       if (js_hbaseAuths == NULL)
         {
           GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_SCANOPEN_PARAM));
           jenv_->PopLocalFrame(NULL);
           return MTC_ERROR_SCANOPEN_PARAM;
         }
     }

  if (hbs_)
      hbs_->getHbaseTimer().start();

  tsRecentJMFromJNI = JavaMethods_[JM_SCAN_OPEN].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(
                                              javaObj_, 
                                              JavaMethods_[JM_SCAN_OPEN].methodID, 
                                              j_tid, jba_startRowID, jba_stopRowID, j_cols, j_ts, j_cb, j_smallScanner, j_ncr,
                                              j_colnamestofilter, j_compareoplist, j_colvaluestocompare, 
                                              j_smplPct, j_preFetch, j_useSnapshotScan,
                                              j_snapTimeout, js_snapName, js_tmp_loc, j_espNum,
                                              j_versions, j_minTS, j_maxTS,
                                              js_hbaseAuths);

  if (hbs_)
  {
    hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
    hbs_->incHbaseCalls();
  }
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::startScan()");
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_SCANOPEN_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "MTableClient_JNI::startScan()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_SCANOPEN_EXCEPTION;
  }
  fetchMode_ = SCAN_FETCH;
  jenv_->PopLocalFrame(NULL);
  return MTC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
MTC_RetCode MTableClient_JNI::deleteRow(Int64 transID, HbaseStr &rowID, const LIST(HbaseStr) *cols, Int64 timestamp, const char *hbaseAuths)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MTableClient_JNI::deleteRow(%ld, %s) called.", transID, rowID.val);

  if (initJNIEnv() != JOI_OK)
     return MTC_ERROR_INIT_PARAM;

  jbyteArray jba_rowID = jenv_->NewByteArray(rowID.len);
  if (jba_rowID == NULL) 
  {
     GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_DELETEROW_PARAM));
     jenv_->PopLocalFrame(NULL);
     return MTC_ERROR_DELETEROW_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_rowID, 0, rowID.len, (const jbyte*)rowID.val);
  jobjectArray j_cols = NULL;
  if (cols != NULL && !cols->isEmpty())
  {
     j_cols = convertToByteArrayObjectArray(*cols);
     if (j_cols == NULL)
     {
        getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::deleteRow()");
        jenv_->PopLocalFrame(NULL);
        return MTC_ERROR_DELETEROW_PARAM;
     }
  }  
  jstring js_hbaseAuths = NULL;
  if (hbaseAuths) {
     js_hbaseAuths = jenv_->NewStringUTF(hbaseAuths);
     if (js_hbaseAuths == NULL) {
         GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_DELETEROW_PARAM));
         jenv_->PopLocalFrame(NULL);
         return MTC_ERROR_DELETEROW_PARAM;
     }
  }
  jlong j_tid = transID;  
  jlong j_ts = timestamp;
  if (hbs_)
    hbs_->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_DELETE].jm_full_name;
  jboolean j_asyncOperation = FALSE;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
          JavaMethods_[JM_DELETE].methodID, j_tid, jba_rowID, j_cols, j_ts, j_asyncOperation, js_hbaseAuths);
  if (hbs_)
    {
      hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
      hbs_->incHbaseCalls();
    }

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::deleteRow()");
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_DELETEROW_EXCEPTION;
  }

  if (jresult == false) 
  {
    logError(CAT_SQL_HBASE, "MTableClient_JNI::deleteRow()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_DELETEROW_EXCEPTION;
  }

  if (hbs_)
    hbs_->incBytesRead(rowID.len);
  jenv_->PopLocalFrame(NULL);
  return MTC_OK;
}
/*
//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
MTC_RetCode MTableClient_JNI::setWriteBufferSize(Int64 size)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MTableClient_JNI::setWriteBufferSize() called.");

  if (initJNIEnv() != JOI_OK)
    return MC_ERROR_INIT_PARAM;

  jlong j_size = size;

  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_SET_WB_SIZE].methodID, j_size);


  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::setWriteBufferSize()");
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_WRITEBUFFERSIZE_EXCEPTION;
  }

  if (jresult == false)
  {
    logError(CAT_SQL_HBASE, "MTableClient_JNI::setWriteBufferSize()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_WRITEBUFFERSIZE_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return MTC_OK;
}

MTC_RetCode MTableClient_JNI::setWriteToWAL(bool WAL)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MTableClient_JNI::setWriteToWAL() called.");

  jboolean j_WAL = WAL;

  if (initJNIEnv() != JOI_OK)
     return MC_ERROR_INIT_PARAM;

  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_SET_WRITE_TO_WAL].methodID, j_WAL);


  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::setWriteToWAL()");
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_WRITETOWAL_EXCEPTION;
  }

  if (jresult == false)
  {
    logError(CAT_SQL_HBASE, "MTableClient_JNI::setWriteToWAL()", getLastError());
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_WRITETOWAL_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return MTC_OK;
}
*/
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
const char *MTableClient_JNI::getTableName()
{
  return tableName_;
}
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
std::string* MTableClient_JNI::getMTableName()
{
  if (initJNIEnv() != JOI_OK)
     return NULL;

  jstring js_name = (jstring)jenv_->CallObjectMethod(javaObj_, JavaMethods_[JM_GET_HTNAME].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::getMTaleName()");
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
 
  if (js_name == NULL) {
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
    
  const char* char_result = jenv_->GetStringUTFChars(js_name, 0);
  std::string* tableName = new (heap_) std::string(char_result);
  jenv_->ReleaseStringUTFChars(js_name, char_result);
  jenv_->PopLocalFrame(NULL);
  return tableName;
}

MTC_RetCode MTableClient_JNI::coProcAggr(Int64 transID, 
					 int aggrType, // 0:count, 1:min, 2:max, 3:sum, 4:avg
					 const Text& startRowID, 
					 const Text& stopRowID, 
					 const Text &colFamily,
					 const Text &colName,
					 const NABoolean cacheBlocks,
					 const Lng32 numCacheRows,
					 Text &aggrVal) // returned value
{

  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MTableClient_JNI::coProcAggr called.");

  int len = 0;

  if (initJNIEnv() != JOI_OK)
     return MTC_ERROR_INIT_PARAM;

  len = startRowID.size();
  jbyteArray jba_startrowid = jenv_->NewByteArray(len);
  if (jba_startrowid == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_COPROC_AGGR_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_COPROC_AGGR_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_startrowid, 0, len, 
			    (const jbyte*)startRowID.data());

  len = stopRowID.size();
  jbyteArray jba_stoprowid = jenv_->NewByteArray(len);
  if (jba_stoprowid == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_COPROC_AGGR_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_COPROC_AGGR_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_stoprowid, 0, len, 
			    (const jbyte*)stopRowID.data());
 
  len = colFamily.size();
  jbyteArray jba_colfamily = jenv_->NewByteArray(len);
  if (jba_colfamily == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_COPROC_AGGR_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_COPROC_AGGR_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_colfamily, 0, len, 
			    (const jbyte*)colFamily.data());
 
  len = colName.size();
  jbyteArray jba_colname = jenv_->NewByteArray(len);
  if (jba_colname == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_COPROC_AGGR_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_COPROC_AGGR_PARAM;
  }
  jenv_->SetByteArrayRegion(jba_colname, 0, len, 
			    (const jbyte*)colName.data());

  jlong j_tid = transID;  
  jint j_aggrtype = aggrType;

  jboolean j_cb = cacheBlocks;
  jint j_ncr = numCacheRows;

  if (hbs_)
    hbs_->getHbaseTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_COPROC_AGGR].jm_full_name;
  jarray jresult = (jarray)jenv_->CallObjectMethod(javaObj_, 
              JavaMethods_[JM_COPROC_AGGR].methodID, j_tid, 
              j_aggrtype, jba_startrowid, jba_stoprowid, jba_colfamily, 
              jba_colname, j_cb, j_ncr);
  if (hbs_)
    {
      hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
      hbs_->incHbaseCalls();
    }

  jenv_->DeleteLocalRef(jba_startrowid);  
  jenv_->DeleteLocalRef(jba_stoprowid);  
  jenv_->DeleteLocalRef(jba_colfamily);
  jenv_->DeleteLocalRef(jba_colname);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::copProcAggr()");
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_COPROC_AGGR_EXCEPTION;
  }

  Text *val = NULL;
  if (jresult != NULL)
  {
     jbyte *result = jenv_->GetByteArrayElements((jbyteArray)jresult, NULL);
     int len = jenv_->GetArrayLength(jresult);
     val = new (heap_) Text((char *)result, len);
     jenv_->ReleaseByteArrayElements((jbyteArray)jresult, result, JNI_ABORT);
     jenv_->DeleteLocalRef(jresult);

  }
  if (val == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(MTC_ERROR_COPROC_AGGR_PARAM));
    jenv_->PopLocalFrame(NULL);
    return MTC_ERROR_COPROC_AGGR_PARAM;
  }  
  aggrVal = *val;

  jenv_->PopLocalFrame(NULL);
  return MTC_OK;
}

NAArray<HbaseStr>* MonarchClient_JNI::getStartKeys(NAHeap *heap, const char *tableName, bool useTRex)
{
   return MonarchClient_JNI::getKeys(JM_MC_GETSTARTKEYS, heap, tableName, useTRex);
}

NAArray<HbaseStr>* MonarchClient_JNI::getEndKeys(NAHeap *heap, const char *tableName, bool useTRex)
{
   return MonarchClient_JNI::getKeys(JM_MC_GETENDKEYS, heap, tableName, useTRex);
}

NAArray<HbaseStr>* MonarchClient_JNI::getKeys(Int32 funcIndex, NAHeap *heap, const char *tableName, bool useTRex)
{
  if (initJNIEnv() != JOI_OK)
     return NULL;

  jstring js_tblName = jenv_->NewStringUTF(tableName);
  if (js_tblName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(MC_ERROR_GETKEYS));
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
  jboolean j_useTRex = useTRex;

  tsRecentJMFromJNI = JavaMethods_[funcIndex].jm_full_name;
  jarray j_keyArray = 
     (jarray) jenv_->CallObjectMethod(javaObj_, JavaMethods_[funcIndex].methodID, js_tblName, j_useTRex);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "MonarchClient_JNI::getKeys()");
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }
  NAArray<HbaseStr> *keyArray;
  jint retcode = convertByteArrayObjectArrayToNAArray(heap, j_keyArray, &keyArray);
  jenv_->PopLocalFrame(NULL);
  if (retcode == 0)
     return NULL;
  else
     return keyArray; 
}
#ifdef __cplusplus
extern "C" {
#endif

JNIEXPORT jint JNICALL Java_org_trafodion_sql_MTableClient_setResultInfo
  (JNIEnv *jenv, jobject jobj, jlong jniObject, 
   jobjectArray jCellsName, jobjectArray jCellsValBuffer,
   jintArray jCellsValOffset, jintArray jCellsValLen,
   jlongArray jTimestamp, jobjectArray jRowIDs,
   jintArray jCellsPerRow, jint numCellsReturned, jint numRowsReturned)
{
   MTableClient_JNI *mtc = (MTableClient_JNI *)jniObject;
   if (mtc->getFetchMode() == MTableClient_JNI::GET_ROW ||
          mtc->getFetchMode() == MTableClient_JNI::BATCH_GET)
      mtc->setJavaObject(jobj);
   mtc->setResultInfo(jCellsName, jCellsValBuffer, jCellsValOffset, jCellsValLen, jTimestamp,
                      jRowIDs, jCellsPerRow, 
                      numCellsReturned, numRowsReturned);  
   return 0;
}

JNIEXPORT jint JNICALL Java_org_trafodion_sql_MTableClient_setJavaObject
  (JNIEnv *jenv, jobject jobj, jlong jniObject)
{
   MTableClient_JNI *mtc = (MTableClient_JNI *)jniObject;
   mtc->setJavaObject(jobj);
   return 0;
}

JNIEXPORT void JNICALL Java_org_trafodion_sql_MTableClient_cleanup
  (JNIEnv *jenv, jobject jobj, jlong jniObject)
{
   MTableClient_JNI *mtc = (MTableClient_JNI *)jniObject;
   NADELETE(mtc, MTableClient_JNI, mtc->getHeap()); 
}

#ifdef __cplusplus
}
#endif

void MTableClient_JNI::setResultInfo( jobjectArray jCellsName, jobjectArray jCellsValBuffer, 
                                      jintArray jCellsValOffset, jintArray jCellsValLen,
                                      jlongArray jTimestamp,  jobjectArray jRowIDs,
                                      jintArray jCellsPerRow, jint numCellsReturned, jint numRowsReturned)
{
   if (numRowsReturned_ > 0)
      cleanupResultInfo();
   NABoolean exceptionFound = FALSE;
   if (numCellsReturned != 0) {
      jCellsName_ = (jobjectArray)jenv_->NewGlobalRef(jCellsName);
      if (jenv_->ExceptionCheck())
          exceptionFound = TRUE;
      if (! exceptionFound) {
         jCellsValBuffer_ = (jobjectArray)jenv_->NewGlobalRef(jCellsValBuffer);
         if (jenv_->ExceptionCheck())
            exceptionFound = TRUE;
      }
      if (! exceptionFound) {
         jCellsValOffset_ = (jintArray)jenv_->NewGlobalRef(jCellsValOffset);
         if (jenv_->ExceptionCheck())
            exceptionFound = TRUE;
      }
      if (! exceptionFound) {
         jCellsValLen_ = (jintArray)jenv_->NewGlobalRef(jCellsValLen);
         if (jenv_->ExceptionCheck())
            exceptionFound = TRUE;
      }
      if (! exceptionFound) {
         jTimestamp_ = (jlongArray)jenv_->NewGlobalRef(jTimestamp);
         if (jenv_->ExceptionCheck())
            exceptionFound = TRUE;
      }
   }
   if (! exceptionFound) {
      jRowIDs_ = (jobjectArray)jenv_->NewGlobalRef(jRowIDs);
      if (jenv_->ExceptionCheck())
         exceptionFound = TRUE;
   }
   if (! exceptionFound) {
      jCellsPerRow_ = (jintArray)jenv_->NewGlobalRef(jCellsPerRow);
      if (jenv_->ExceptionCheck())
         exceptionFound = TRUE;
   }
   numCellsReturned_ = numCellsReturned;
   numRowsReturned_ = numRowsReturned;
   prevRowCellNum_ = 0;
   currentRowNum_ = -1;
   cleanupDone_ = FALSE;
   ex_assert(! exceptionFound, "Exception in MTableClient_JNI::setResultInfo");
   return;
} 

void MTableClient_JNI::cleanupResultInfo()
{
   if (cleanupDone_)
      return;
   if (jCellsName_ != NULL) {
      jenv_->DeleteGlobalRef(jCellsName_);
      jCellsName_ = NULL;
   }
   if (jCellsValBuffer_ != NULL) {
      jenv_->DeleteGlobalRef(jCellsValBuffer_);
      jCellsValBuffer_ = NULL;
   }
   if (jCellsValLen_ != NULL) {
      jenv_->DeleteGlobalRef(jCellsValLen_);
      jCellsValLen_ = NULL;
   }
   if (jCellsValOffset_ != NULL) {
      jenv_->DeleteGlobalRef(jCellsValOffset_);
      jCellsValOffset_ = NULL;
   }
   if (jTimestamp_ != NULL) {
      jenv_->DeleteGlobalRef(jTimestamp_);
      jTimestamp_ = NULL;
   }
   if (jRowIDs_ != NULL) {
      jenv_->DeleteGlobalRef(jRowIDs_);
      jRowIDs_ = NULL;
   }
   if (jba_cellValBuffer_ != NULL) {
      jenv_->DeleteGlobalRef(jba_cellValBuffer_);
      jba_cellValBuffer_ = NULL;
   }
   if (p_rowID_ != NULL) {
      jenv_->ReleaseByteArrayElements(jba_rowID_, p_rowID_, JNI_ABORT);
      p_rowID_ = NULL;
      jenv_->DeleteGlobalRef(jba_rowID_);
      jba_rowID_ = NULL;
   }
   if (p_cellsPerRow_ != NULL) {
      jenv_->ReleaseIntArrayElements(jCellsPerRow_, p_cellsPerRow_, JNI_ABORT);
      p_cellsPerRow_ = NULL;
      jenv_->DeleteGlobalRef(jCellsPerRow_);
      jCellsPerRow_ = NULL;
   }
   cleanupDone_ = TRUE;
   return;
}

MTC_RetCode MTableClient_JNI::nextRow()
{
    MTC_RetCode retCode;

    ex_assert(fetchMode_ != UNKNOWN, "invalid fetchMode_");
    switch (fetchMode_) {
       case GET_ROW:
          if (numRowsReturned_ == -1)
             return MTC_DONE;
          if (currentRowNum_ == -1)
          {
             getResultInfo();
             return MTC_OK;
	  }
          else
          {
             cleanupResultInfo();   
             return MTC_DONE;
          }
          break;
       case BATCH_GET:
          if (numRowsReturned_ == -1)
             return MTC_DONE_RESULT;
          if (currentRowNum_ == -1)
          {
             getResultInfo();
             return MTC_OK;
          }
          else
          if ((currentRowNum_+1) >= numRowsReturned_)
          {
             cleanupResultInfo();   
             return MTC_DONE_RESULT;
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
            return MTC_DONE;
        }   
        retCode = fetchRows();
        if (retCode != MTC_OK)
        {
           cleanupResultInfo();
           return retCode;
        }
        getResultInfo();
    }
    else
    {
        // Add the number of previous cells returned
        jint cellsPerRow = p_cellsPerRow_[currentRowNum_];
        prevRowCellNum_ += cellsPerRow;  
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
    return MTC_OK;
}

void MTableClient_JNI::getResultInfo()
{
   // Allocate Buffer and copy the cell info
   int numCellsNeeded;
   if (numCellsReturned_ == 0)
   {
      p_cellsPerRow_ = jenv_->GetIntArrayElements(jCellsPerRow_, NULL);
      currentRowNum_ = 0;
      currentRowCellNum_ = 0;
      prevRowCellNum_ = 0;
      return;
   }
   if (numCellsAllocated_ == 0 || 
		numCellsAllocated_ < numCellsReturned_) {
      NAHeap *heap = getHeap();
      if (numCellsAllocated_ > 0) {
         NADELETEBASIC(p_cellsValLen_, heap_);
         NADELETEBASIC(p_cellsValOffset_, heap_);
         NADELETEBASIC(p_timestamp_, heap);
         numCellsNeeded = numCellsReturned_;
      }
      else {  
         if (numColsInScan_ == 0)
            numCellsNeeded = numCellsReturned_;
         else    
            numCellsNeeded = 2 * numReqRows_ * numColsInScan_;
      }
      p_cellsValLen_ = new (heap) jint[numCellsNeeded];
      p_cellsValOffset_ = new (heap) jint[numCellsNeeded];
      p_timestamp_ = new (heap) jlong[numCellsNeeded];
      numCellsAllocated_ = numCellsNeeded;
   }
   jenv_->GetIntArrayRegion(jCellsValLen_, 0, numCellsReturned_, p_cellsValLen_);
   jenv_->GetIntArrayRegion(jCellsValOffset_, 0, numCellsReturned_, p_cellsValOffset_);
   p_cellsPerRow_ = jenv_->GetIntArrayElements(jCellsPerRow_, NULL);
   currentRowNum_ = 0;
   currentRowCellNum_ = 0;
   prevRowCellNum_ = 0;
}

MTC_RetCode MTableClient_JNI::getColName(int colNo,
              char **outColName, 
              short &colNameLen,
              Int64 &timestamp)
{
    jsize cellNameLen;

    jint cellsPerRow = p_cellsPerRow_[currentRowNum_];
    if (cellsPerRow == 0 || colNo >= cellsPerRow)
    {
       *outColName == NULL;
       timestamp = 0;
       return MTC_OK;
    }
    int idx = prevRowCellNum_ + colNo;
    ex_assert((idx < numCellsReturned_), "Buffer overflow");
    if (p_cellName_ != NULL) {
       jenv_->ReleaseByteArrayElements(jba_cellName_, p_cellName_, JNI_ABORT);
       jenv_->DeleteGlobalRef(jba_cellName_);
       p_cellName_ = NULL;
       jba_cellName_ = NULL;
    }
    jobject cellNameObj;
    cellNameObj = jenv_->GetObjectArrayElement(jCellsName_, idx);
    if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::getColName()");
      return MTC_GET_COLNAME_EXCEPTION;
    }
    jba_cellName_ = (jbyteArray)jenv_->NewGlobalRef(cellNameObj);
    if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::getColName()");
      return MTC_GET_COLNAME_EXCEPTION;
    }
    jenv_->DeleteLocalRef(cellNameObj);
    p_cellName_ = jenv_->GetByteArrayElements(jba_cellName_, NULL);
    timestamp = p_timestamp_[idx];
    *outColName = (char *)p_cellName_;
    cellNameLen = jenv_->GetArrayLength(jba_cellName_); 
    colNameLen = cellNameLen;
    if (hbs_)
      hbs_->incBytesRead(sizeof(timestamp) + colNameLen);
    return MTC_OK; 
}

MTC_RetCode MTableClient_JNI::getColVal(int colNo, BYTE *colVal, 
                                        Lng32 &colValLen, 
                                        NABoolean nullable, BYTE &nullVal,
                                        BYTE *tag, Lng32 &tagLen)
{
    Lng32 copyLen;
    jbyte nullByte;

    jint cellsPerRow = p_cellsPerRow_[currentRowNum_];
    if (cellsPerRow == 0 || colNo >= cellsPerRow)
       return MTC_GET_COLVAL_EXCEPTION;
    int idx = prevRowCellNum_ + colNo;
    ex_assert((idx < numCellsReturned_), "Buffer overflow");
    jint cellValLen = p_cellsValLen_[idx];
    jint cellValOffset = p_cellsValOffset_[idx];
    // clean the jba_cellValBuffer_ of the previous column
    // And get the jba_cellValBuffer_ for the current column
    if (jba_cellValBuffer_ != NULL) {
       jenv_->DeleteGlobalRef(jba_cellValBuffer_);
       jba_cellValBuffer_ = NULL;
    }
    jobject cellValBufferObj;
    cellValBufferObj = jenv_->GetObjectArrayElement(jCellsValBuffer_, idx);
    if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::getColVal()");
      return MTC_GET_COLVAL_EXCEPTION;
    }
    jba_cellValBuffer_ = (jbyteArray)jenv_->NewGlobalRef(cellValBufferObj);
    if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::getColVal()");
      return MTC_GET_COLVAL_EXCEPTION;
    }
    jenv_->DeleteLocalRef(cellValBufferObj);
    // If the column is nullable, get the first byte
    // The first byte determines if the column is null(0xff) or not (0)
    if (nullable) {
       copyLen = MINOF(cellValLen-1, colValLen);
       jenv_->GetByteArrayRegion(jba_cellValBuffer_, cellValOffset, 1, &nullByte);
       jenv_->GetByteArrayRegion(jba_cellValBuffer_, cellValOffset+1, copyLen,
               (jbyte *)colVal);
    } else {
       copyLen = MINOF(cellValLen, colValLen);
       nullByte = 0;
       jenv_->GetByteArrayRegion(jba_cellValBuffer_, cellValOffset, copyLen, (jbyte *)colVal ); 
    }
    nullVal = nullByte;
    colValLen = copyLen;
    if (hbs_)
      hbs_->incBytesRead(colValLen);
    return MTC_OK;
}

MTC_RetCode MTableClient_JNI::getColVal(NAHeap *heap, int colNo, BYTE **colVal, 
          Lng32 &colValLen)
{
    jint cellsPerRow = p_cellsPerRow_[currentRowNum_];
    if (cellsPerRow == 0 || colNo >= cellsPerRow)
       return MTC_GET_COLVAL_EXCEPTION;
    int idx = prevRowCellNum_ + colNo;
    ex_assert((idx < numCellsReturned_), "Buffer overflow");
    if (jba_cellValBuffer_ != NULL) {
       jenv_->DeleteGlobalRef(jba_cellValBuffer_);
       jba_cellValBuffer_ = NULL;
    }
    jint cellValLen = p_cellsValLen_[idx];
    jint cellValOffset = p_cellsValOffset_[idx];
    // clean the jba_cellValBuffer_ of the previous column
    // And get the jba_cellValBuffer_ for the current column
    if (jba_cellValBuffer_ != NULL) {
       jenv_->DeleteGlobalRef(jba_cellValBuffer_);
       jba_cellValBuffer_ = NULL;
    }
    jobject cellValBufferObj;
    cellValBufferObj = jenv_->GetObjectArrayElement(jCellsValBuffer_, idx);
    if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::getColVal()");
      return MTC_GET_COLVAL_EXCEPTION;
    }
    jba_cellValBuffer_ = (jbyteArray)jenv_->NewGlobalRef(cellValBufferObj);
    if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::getColVal()");
      return MTC_GET_COLVAL_EXCEPTION;
    }
    jenv_->DeleteLocalRef(cellValBufferObj);
   
    BYTE *colValTmp;
    int colValLenTmp;
    if (heap == NULL)
    {
       colValTmp = *colVal; 
       colValLenTmp = colValLen;
       if (colValLenTmp > cellValLen)
          colValLenTmp = cellValLen;
    }
    else
    {
       colValTmp = new (heap) BYTE[cellValLen];
       colValLenTmp = cellValLen;
    }
    jenv_->GetByteArrayRegion(jba_cellValBuffer_, cellValOffset, colValLenTmp,
             (jbyte *)colValTmp); 
    *colVal = colValTmp;
    colValLen = colValLenTmp;
    if (hbs_)
      hbs_->incBytesRead(colValLen);
    return MTC_OK;
}

MTC_RetCode MTableClient_JNI::getNumCellsPerRow(int &numCells)
{
    jint cellsPerRow = p_cellsPerRow_[currentRowNum_];
    numCells = cellsPerRow;
    if (numCells == 0)
       return MTC_DONE_DATA;
    else
       return MTC_OK;
}  


MTC_RetCode MTableClient_JNI::getRowID(HbaseStr &rowID)
{
    jint cellsPerRow = p_cellsPerRow_[currentRowNum_];
    if (p_rowID_ != NULL)
    {
      jenv_->ReleaseByteArrayElements(jba_rowID_, p_rowID_, JNI_ABORT);
      p_rowID_ = NULL;
      jenv_->DeleteGlobalRef(jba_rowID_);
    }

    if (cellsPerRow == 0) 
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
          getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::getRowID()");
          return MTC_GET_ROWID_EXCEPTION;
       }
       jenv_->DeleteLocalRef(rowIDObj);
       p_rowID_ = jenv_->GetByteArrayElements(jba_rowID_, NULL);
       rowIDLen_ = jenv_->GetArrayLength(jba_rowID_); 
       rowID.len = rowIDLen_;
       rowID.val = (char *)p_rowID_;
    }
    return MTC_OK;
}

MTC_RetCode MTableClient_JNI::fetchRows()
{
   QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "MTableClient_JNI::fetchRows() called.");
   if (initJNIEnv() != JOI_OK)
      return MTC_ERROR_INIT_PARAM;

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
      getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::fetchRows()");
      jenv_->PopLocalFrame(NULL);
      return MTC_ERROR_FETCHROWS_EXCEPTION;
   }

   numRowsReturned_ = jRowsReturned;
   if (numRowsReturned_ == -1)
   {
      logError(CAT_SQL_HBASE, "MTableClient_JNI::fetchRows()", getLastError());
      jenv_->PopLocalFrame(NULL);
      return MTC_ERROR_FETCHROWS_EXCEPTION;
   }
   else
   if (numRowsReturned_ == 0) {
      jenv_->PopLocalFrame(NULL);
      return MTC_DONE;
   }
   if (hbs_)
      hbs_->incAccessedRows(numRowsReturned_);
   jenv_->PopLocalFrame(NULL);
   return MTC_OK; 
}

MTC_RetCode MTableClient_JNI::nextCell(
        	 HbaseStr &rowId,
                 HbaseStr &colFamName,
                 HbaseStr &colQualName,
                 HbaseStr &colVal,
                 Int64 &timestamp)
{
   MTC_RetCode retcode;
   jint cellsPerRow = p_cellsPerRow_[currentRowNum_];
   if (currentRowCellNum_ >= cellsPerRow)
   {
      currentRowCellNum_ = -1;
      return MTC_DONE;
   }
   if (p_rowID_ != NULL)
   {
      rowId.val = (char *)p_rowID_;
      rowId.len = rowIDLen_;
   }
   else
   {
      retcode = getRowID(rowId);
      if (retcode != MTC_OK)
         return retcode;
   }
   int idx = prevRowCellNum_ + currentRowCellNum_;
   ex_assert((idx < numCellsReturned_), "Buffer overflow");
   if (p_cellName_ != NULL) {
       jenv_->ReleaseByteArrayElements(jba_cellName_, p_cellName_, JNI_ABORT);
       jenv_->DeleteGlobalRef(jba_cellName_);
       p_cellName_ = NULL;
       jba_cellName_ = NULL;
   }
   jobject cellNameObj;
   cellNameObj = jenv_->GetObjectArrayElement(jCellsName_, idx);
   if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::nextCell()");
      return MTC_GET_COLNAME_EXCEPTION;
   }
   jba_cellName_ = (jbyteArray)jenv_->NewGlobalRef(cellNameObj);
   if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::nextCell()");
      return MTC_GET_COLNAME_EXCEPTION;
   }
   jenv_->DeleteLocalRef(cellNameObj);
   p_cellName_ = jenv_->GetByteArrayElements(jba_cellName_, NULL);
   jsize cellNameLen = jenv_->GetArrayLength(jba_cellName_);
   colFamName.val = (char *) p_cellName_;
   char *temp  = strchr((char *)p_cellName_, ':');
   colFamName.len = temp - colFamName.val; 
   colQualName.val = temp+1;
   colQualName.len = cellNameLen - colFamName.len - 1; // 1  for ':'
   timestamp = -1; //p_timestamp_[idx];
   retcode = getColVal(NULL, currentRowCellNum_, (BYTE **)&colVal.val,
                         colVal.len);
   if (retcode != MTC_OK)
      return retcode;
   currentRowCellNum_++;
   return MTC_OK;
}

MTC_RetCode MTableClient_JNI::completeAsyncOperation(Int32 timeout, NABoolean *resultArray, Int16 resultArrayLen)
{
  MTC_RetCode retcode;

  if (initJNIEnv() != JOI_OK) {
     if (hbs_)
       hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
     return MTC_ERROR_COMPLETEASYNCOPERATION_EXCEPTION;
  }
  jint jtimeout = timeout;
  jbooleanArray jresultArray =  jenv_->NewBooleanArray(resultArrayLen);
  if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::completeAsyncOperation()");
      jenv_->PopLocalFrame(NULL);
      if (hbs_)
         hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
      return MTC_ERROR_COMPLETEASYNCOPERATION_EXCEPTION;
   }
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_COMPLETE_PUT].methodID,
                               jtimeout, jresultArray);
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "MTableClient_JNI::completeAsyncOperation()");
    jenv_->PopLocalFrame(NULL);
    if (hbs_)
       hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
    return MTC_ERROR_COMPLETEASYNCOPERATION_EXCEPTION;
  }
  if (jresult == false) {
     jenv_->PopLocalFrame(NULL);
     return MTC_ERROR_ASYNC_OPERATION_NOT_COMPLETE;
  }
  if (hbs_)
     hbs_->incMaxHbaseIOTime(hbs_->getHbaseTimer().stop());
  jboolean *returnArray = jenv_->GetBooleanArrayElements(jresultArray, NULL);
  for (int i = 0; i < resultArrayLen; i++) 
      resultArray[i] = returnArray[i]; 
  jenv_->ReleaseBooleanArrayElements(jresultArray, returnArray, JNI_ABORT);
  jenv_->DeleteLocalRef(jresultArray);
  jenv_->PopLocalFrame(NULL);
  return MTC_OK;
}

