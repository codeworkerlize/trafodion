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

#include "QRLogger.h"
#include "Globals.h"
#include "Context.h"

#include "HiveClient_JNI.h"
#include "org_trafodion_sql_HiveClient.h"


// ===========================================================================
// ===== Class HiveClient_JNI
// ===========================================================================

JavaMethodInit* HiveClient_JNI::JavaMethods_ = NULL;
jclass HiveClient_JNI::javaClass_ = 0;
bool HiveClient_JNI::javaMethodsInitialized_ = false;
pthread_mutex_t HiveClient_JNI::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

static const char* const hvcErrorEnumStr[] = 
{
  "Preparing parameters for HiveClient."
 ,"Java exception in init()."
 ,"Java exception in close()."
 ,"Preparing parameters for exists()."
 ,"Java exception in exists()."
 ,"Preparing parameters for getRedefTime()."
 ,"Java exception in getRedefTime()."
 ,"Java exception in getAllSchemas()."
 ,"Preparing parameters for getAllTables()."
 ,"Java exception in getAllTables()."
 ,"Preparing parameters for getAllTablesMeta()."
 ,"Java exception in getAllTablesMeta()."
 ,"Preparing parameters for getAllViews()."
 ,"Java exception in getAllViews()."
 ,"Preparing parameters for executeHiveSQL()."
 ,"Java exception in executeHiveSQL()."
 ,"Preparing parameters for createHiveTablePartition()."
 ,"java exception in createHiveTablePartition()."
 ,"Preparing parameters for getSentryPrivileges()."
 ,"java exception in getSentryPrivileges()."
 ,"Preparing parameters for getHiveTableInfo()."
 ,"Java exception in getHiveTableInfo()."
 ,"Error in getHiveTableInfoDetails()."
 ,"Error during populdate SDs."
 ,"Preparing parameters for getStripeOrRowgroupInfo()."
 ,"Java exception in getStripeOrRowgroupInfo()."
};



//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
char* HiveClient_JNI::getErrorText(HVC_RetCode errEnum)
{
  if (errEnum < (HVC_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)errEnum);
  else    
    return (char*)hvcErrorEnumStr[errEnum-HVC_FIRST-1];
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HiveClient_JNI* HiveClient_JNI::newInstance(NAHeap *heap, HVC_RetCode &retCode)
{
   QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "HiveClient_JNI::newInstance() called.");

   if (initJNIEnv() != JOI_OK)
     return NULL;
   retCode = HVC_OK;
   HiveClient_JNI *hiveClient_JNI = new (heap) HiveClient_JNI(heap);
   if (hiveClient_JNI != NULL) {
       retCode = hiveClient_JNI->init();
       if (retCode != HVC_OK) {
          NADELETE(hiveClient_JNI, HiveClient_JNI, heap);
          hiveClient_JNI = NULL;
       }
   } 
   return hiveClient_JNI;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HiveClient_JNI* HiveClient_JNI::getInstance()
{
   HVC_RetCode hvcRetcode = HVC_OK;

   ContextCli *currContext = GetCliGlobals()->currContext();
   HiveClient_JNI *hiveClient_JNI = currContext->getHiveClient();
   if (hiveClient_JNI == NULL)
   { 
       NAHeap *heap = currContext->exHeap();
       hiveClient_JNI = new (heap) HiveClient_JNI(heap);
       if ((hvcRetcode = hiveClient_JNI->init()) == HVC_OK)
          currContext->setHiveClient(hiveClient_JNI);
       else {
          NADELETE(hiveClient_JNI, HiveClient_JNI, heap);
          hiveClient_JNI = NULL;
       }
   }
   return hiveClient_JNI;
}

void HiveClient_JNI::deleteInstance()
{
  ContextCli *currContext = GetCliGlobals()->currContext();
  HiveClient_JNI *hiveClient_JNI = currContext->getHiveClient();
  if (hiveClient_JNI != NULL)
  {
     NAHeap *heap = currContext->exHeap();
     NADELETE(hiveClient_JNI, HiveClient_JNI, heap);
     currContext->setHiveClient(NULL);
  }
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HiveClient_JNI::~HiveClient_JNI()
{
   cleanupTableInfo();
   if (isConnected_)	
      close(); // error handling?
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HVC_RetCode HiveClient_JNI::init()
{
  static char className[]="org/trafodion/sql/HiveClient";
  HVC_RetCode rc;

  if (isInitialized())
    return HVC_OK;
  
  if (javaMethodsInitialized_)
    return (HVC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
  else
  {
    pthread_mutex_lock(&javaMethodsInitMutex_);
    if (javaMethodsInitialized_)
    {
      pthread_mutex_unlock(&javaMethodsInitMutex_);
      return (HVC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    }
    JavaMethods_ = new JavaMethodInit[JM_LAST];
    
    JavaMethods_[JM_CTOR       ].jm_name      = "<init>";
    JavaMethods_[JM_CTOR       ].jm_signature = "()V";
    JavaMethods_[JM_CLOSE      ].jm_name      = "close";
    JavaMethods_[JM_CLOSE      ].jm_signature = "()Z";
    JavaMethods_[JM_EXISTS     ].jm_name      = "exists";
    JavaMethods_[JM_EXISTS     ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)Z";
    JavaMethods_[JM_GET_RDT    ].jm_name      = "getRedefTime";
    JavaMethods_[JM_GET_RDT    ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;Z)J";
    JavaMethods_[JM_GET_ASH     ].jm_name      = "getAllSchemas";
    JavaMethods_[JM_GET_ASH     ].jm_signature = "()[Ljava/lang/Object;";
    JavaMethods_[JM_GET_ATL    ].jm_name      = "getAllTables";
    JavaMethods_[JM_GET_ATL    ].jm_signature = "(Ljava/lang/String;)[Ljava/lang/Object;";
    JavaMethods_[JM_GET_ATLM   ].jm_name      = "getAllTablesMeta";
    JavaMethods_[JM_GET_ATLM   ].jm_signature = "(Ljava/lang/String;)[Ljava/lang/Object;";
    JavaMethods_[JM_GET_AVW    ].jm_name      = "getAllViews";
    JavaMethods_[JM_GET_AVW    ].jm_signature = "(Ljava/lang/String;)[Ljava/lang/Object;";
    JavaMethods_[JM_EXEC_HIVE_SQL].jm_name = "executeHiveSQL";
    JavaMethods_[JM_EXEC_HIVE_SQL].jm_signature = "(Ljava/lang/String;)V";
    JavaMethods_[JM_CREATE_HIVE_PART ].jm_name      = "createHiveTablePartition";
    JavaMethods_[JM_CREATE_HIVE_PART ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Z)V";
    JavaMethods_[JM_GET_SENTRY_PRIVS ].jm_name      = "getSentryPrivileges";
    JavaMethods_[JM_GET_SENTRY_PRIVS ].jm_signature = "([Ljava/lang/Object;[Ljava/lang/Object;Ljava/lang/String;)[I";
    JavaMethods_[JM_GET_SENTRY_PRIVS_U ].jm_name      = "getSentryPrivilegesByUserName";
    JavaMethods_[JM_GET_SENTRY_PRIVS_U ].jm_signature = "(Ljava/lang/String;[Ljava/lang/Object;Ljava/lang/String;)[I";
    JavaMethods_[JM_GET_HVT_INFO].jm_name      = "getHiveTableInfo";
    JavaMethods_[JM_GET_HVT_INFO].jm_signature = "(JLjava/lang/String;Ljava/lang/String;Z)Z";
    JavaMethods_[JM_GET_STRIPE_OR_ROWGROUP_INFO].jm_name      = "getStripeOrRowgroupInfo";
    JavaMethods_[JM_GET_STRIPE_OR_ROWGROUP_INFO].jm_signature = "(JILjava/lang/String;IIZ)J";

    rc = (HVC_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    if (rc == HVC_OK)
       javaMethodsInitialized_ = TRUE;
    pthread_mutex_unlock(&javaMethodsInitMutex_);
  }
  return rc;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HVC_RetCode HiveClient_JNI::exists(const char* schName, const char* tabName)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "HiveClient_JNI::exists(%s, %s) called.", schName, tabName);
  if (initJNIEnv() != JOI_OK)
     return HVC_ERROR_INIT_PARAM;
  if (getInstance() == NULL)
     return HVC_ERROR_INIT_PARAM;
  jstring js_schName = jenv_->NewStringUTF(schName);
  if (js_schName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_EXISTS_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_EXISTS_PARAM;
  }
  jstring js_tabName = jenv_->NewStringUTF(tabName);
  if (js_tabName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_EXISTS_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_EXISTS_PARAM;
  }

  // boolean exists(java.lang.String, java.lang.String);
  tsRecentJMFromJNI = JavaMethods_[JM_EXISTS].jm_full_name;
  jboolean jresult = jenv_->CallStaticBooleanMethod(javaClass_, JavaMethods_[JM_EXISTS].methodID, js_schName, js_tabName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::exists()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_EXISTS_EXCEPTION;
  }

  if (jresult == false) {
     jenv_->PopLocalFrame(NULL);
     return HVC_DONE;  // Table does not exist
  }

  jenv_->PopLocalFrame(NULL);
  return HVC_OK;  // Table exists.
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////  
HVC_RetCode HiveClient_JNI::getRedefTime(const char* schName, 
                                         const char* tabName, 
					 NABoolean allowSubdir,
                                         Int64& redefTime)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "Enter HiveClient_JNI::getRedefTime(%s, %s, %d, %lld).", 
		schName, tabName, allowSubdir, redefTime);
  if (initJNIEnv() != JOI_OK)
     return HVC_ERROR_INIT_PARAM;
  if (getInstance() == NULL)
     return HVC_ERROR_INIT_PARAM;

  jstring js_schName = jenv_->NewStringUTF(schName);
  if (js_schName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_GET_REDEFTIME_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_REDEFTIME_PARAM;
  }
  jstring js_tabName = jenv_->NewStringUTF(tabName);
  if (js_tabName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_GET_REDEFTIME_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_REDEFTIME_PARAM;
  }
  jboolean j_allowSubdir = allowSubdir;

  //  jlong getRedefTime(java.lang.String, java.lang.String, boolean);
  tsRecentJMFromJNI = JavaMethods_[JM_GET_RDT].jm_full_name;
  jlong jresult = jenv_->CallStaticLongMethod(javaClass_, 
					      JavaMethods_[JM_GET_RDT].methodID, 
					      js_schName, js_tabName, j_allowSubdir);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getRedefTime()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_REDEFTIME_EXCEPTION;
  }

  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "Exit HiveClient_JNI::getRedefTime(%s, %s, %d, %lld).", 
		schName, tabName, allowSubdir, redefTime);

  if (jresult <= 0) {
    jenv_->PopLocalFrame(NULL);
    return HVC_DONE; // Table does not exist
  }

  redefTime = jresult ;
  jenv_->PopLocalFrame(NULL);
  return HVC_OK;  // Table exists.
}

//////////////////////////////////////////////////////////////////////////////
// 
////////////////////////////////////////////////////////////////////////////// 
HVC_RetCode HiveClient_JNI::getAllSchemas(NAHeap *heap, LIST(Text *)& schNames)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "Enter HiveClient_JNI::getAllSchemas(%p) called.", (void *) &schNames);
  if (initJNIEnv() != JOI_OK)
     return HVC_ERROR_INIT_PARAM;
  if (getInstance() == NULL)
     return HVC_ERROR_INIT_PARAM;

  tsRecentJMFromJNI = JavaMethods_[JM_GET_ASH].jm_full_name;
  jarray j_schNames= 
     (jarray)jenv_->CallStaticObjectMethod(javaClass_, JavaMethods_[JM_GET_ASH].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getAllSchemas()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_ALLSCH_EXCEPTION;
  }

  int numSchemas = convertStringObjectArrayToList(heap, j_schNames,
                   schNames);           
  if (numSchemas == 0) {
     jenv_->PopLocalFrame(NULL);
     return HVC_DONE;
  }
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, 
       "Exit HiveClient_JNI::getAllSchemas(%p) called.", (void *) &schNames);
  jenv_->PopLocalFrame(NULL);
  return HVC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
////////////////////////////////////////////////////////////////////////////// 
HVC_RetCode HiveClient_JNI::executeHiveSQL(const char* hiveSQL)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "Enter HiveClient_JNI::executeHiveSQL(%s) called.", hiveSQL);
  if (initJNIEnv() != JOI_OK)
     return HVC_ERROR_INIT_PARAM;
  if (getInstance() == NULL)
     return HVC_ERROR_INIT_PARAM;

  jstring js_hiveSQL = jenv_->NewStringUTF(hiveSQL);
  if (js_hiveSQL == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_GET_ALLTBL_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_EXECUTE_HIVE_SQL_PARAM;
  }
  
  tsRecentJMFromJNI = JavaMethods_[JM_EXEC_HIVE_SQL].jm_full_name;
  jenv_->CallStaticVoidMethod(javaClass_, JavaMethods_[JM_EXEC_HIVE_SQL].methodID, js_hiveSQL);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::executeHiveSQL()",
                        NULL, TRUE /*dont return stack details*/);
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_EXECUTE_HIVE_SQL_EXCEPTION;
  }

  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, 
       "Exit HiveClient_JNI::executeHiveSQL(%s) called.", hiveSQL);
  jenv_->PopLocalFrame(NULL);
  return HVC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HVC_RetCode HiveClient_JNI::getAllTables(NAHeap *heap, const char* schName, 
                                         LIST(Text *)& tblNames)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "Enter HiveClient_JNI::getAllTables(%s, %p) called.", schName, (void *) &tblNames);
  if (initJNIEnv() != JOI_OK)
     return HVC_ERROR_INIT_PARAM;
  if (getInstance() == NULL)
     return HVC_ERROR_INIT_PARAM;

  jstring js_schName = jenv_->NewStringUTF(schName);
  if (js_schName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_GET_ALLTBL_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_ALLTBL_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_GET_ATL].jm_full_name;
  jarray j_tblNames = 
    (jarray)jenv_->CallStaticObjectMethod(javaClass_, JavaMethods_[JM_GET_ATL].methodID, 
                            js_schName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getAllTables()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_ALLTBL_EXCEPTION;
  }

  if (j_tblNames == NULL) 	
  {	
     GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_EXISTS_EXCEPTION));
     jenv_->PopLocalFrame(NULL);	
     return HVC_ERROR_EXISTS_EXCEPTION;	
  }

  int numTables = convertStringObjectArrayToList(heap, j_tblNames,
                   tblNames);           
  if (numTables == 0) {
     jenv_->PopLocalFrame(NULL);
     return HVC_DONE;
  }
  jenv_->PopLocalFrame(NULL);
  return HVC_OK;
}

//////////////////////////////////////////////////////////////////////////////	
// 	
//////////////////////////////////////////////////////////////////////////////
HVC_RetCode HiveClient_JNI::getAllTablesMeta(NAHeap *heap, const char* schName,
                                             LIST(Text *)& tblNamesMeta)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "Enter HiveClient_JNI::getAllTablesMeta(%s, %p) called.",
                schName, (void *) &tblNamesMeta);
  if (initJNIEnv() != JOI_OK)
     return HVC_ERROR_INIT_PARAM;

  jstring js_schName = jenv_->NewStringUTF(schName);
  if (js_schName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_GET_ALLTBLMETA_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_ALLTBLMETA_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_GET_ATLM].jm_full_name;
  jarray j_tblNamesMeta =
    (jarray)jenv_->CallStaticObjectMethod(javaClass_, JavaMethods_[JM_GET_ATLM].methodID,
                                    js_schName);

  if (j_tblNamesMeta == NULL)
    {
      GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_GET_ALLTBLMETA_EXCEPTION));
      jenv_->PopLocalFrame(NULL);
      return HVC_ERROR_GET_ALLTBLMETA_EXCEPTION;
    }

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getAllTablesMeta()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_ALLTBLMETA_EXCEPTION;
  }

  int numTables = convertStringObjectArrayToList(heap, j_tblNamesMeta,
                   tblNamesMeta);
  if (numTables == 0) {
     jenv_->PopLocalFrame(NULL);
     return HVC_DONE;
  }
  jenv_->PopLocalFrame(NULL);
  return HVC_OK;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
HVC_RetCode HiveClient_JNI::getAllViews(NAHeap *heap, const char* schName,
                                        LIST(Text *)& viewNames)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "Enter HiveClient_JNI::getAllViews(%s, %p) called.", schName, (void *) &viewNames);
  if (initJNIEnv() != JOI_OK)
     return HVC_ERROR_INIT_PARAM;

  jstring js_schName = jenv_->NewStringUTF(schName);
  if (js_schName == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_GET_ALLVIEW_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_ALLVIEW_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_GET_AVW].jm_full_name;
  jarray j_viewNames =
    (jarray)jenv_->CallStaticObjectMethod(javaClass_, JavaMethods_[JM_GET_AVW].methodID,
                            js_schName);

  if (j_viewNames == NULL)
    {
      GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_GET_ALLVIEW_EXCEPTION));
      jenv_->PopLocalFrame(NULL);
      return HVC_ERROR_GET_ALLVIEW_EXCEPTION;
    }

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getAllViews()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_ALLVIEW_EXCEPTION;
  }

  int numViews = convertStringObjectArrayToList(heap, j_viewNames,
                   viewNames);
  if (numViews == 0) {
     jenv_->PopLocalFrame(NULL);
     return HVC_DONE;
  }
  jenv_->PopLocalFrame(NULL);
  return HVC_OK;
}


//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////	
HVC_RetCode HiveClient_JNI::close()	
{	
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "HiveClient_JNI::close() called.");	
	
  if (initJNIEnv() != JOI_OK)	
     return HVC_ERROR_INIT_PARAM;	
	
  // boolean close();	
  tsRecentJMFromJNI = JavaMethods_[JM_CLOSE].jm_full_name;	
  jboolean jresult = jenv_->CallStaticBooleanMethod(javaClass_, JavaMethods_[JM_CLOSE].methodID);	
  if (jenv_->ExceptionCheck())	
  {	
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::close()");	
    jenv_->PopLocalFrame(NULL);	
    return HVC_ERROR_CLOSE_EXCEPTION;	
  }	
  	
  if (jresult == false) 	
  {	
    logError(CAT_SQL_HDFS, "HiveClient_JNI::close()", getLastError());	
    jenv_->PopLocalFrame(NULL);	
    return HVC_ERROR_CLOSE_EXCEPTION;	
  }	
	
  jenv_->PopLocalFrame(NULL);	
  return HVC_OK;	
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////  
HVC_RetCode HiveClient_JNI::createHiveTablePartition(const char *schemaName,
                                                     const char *tableName,
                                                     const char *partitionString,
                                                     bool useDistributedLock)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG,
                "Enter HiveClient_JNI::createHiveTablePartition(\"%s\",\"%s\",\"%s\") called.",
                schemaName, tableName, partitionString);
  if (initJNIEnv() != JOI_OK)
     return HVC_ERROR_INIT_PARAM;
  if (getInstance() == NULL)
     return HVC_ERROR_INIT_PARAM;

  jstring js_schemaName = jenv_->NewStringUTF(schemaName);
  jstring js_tableName = jenv_->NewStringUTF(tableName);
  jstring js_partitionString = jenv_->NewStringUTF(partitionString);
  if (js_schemaName == NULL ||
      js_tableName == NULL ||
      js_partitionString == NULL)
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_CREATEHIVEPART_EXCEPTION));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_CREATEHIVEPART_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_CREATE_HIVE_PART].jm_full_name;
  jenv_->CallVoidMethod(javaObj_,
                        JavaMethods_[JM_CREATE_HIVE_PART].methodID,
                        js_schemaName,
                        js_tableName,
                        js_partitionString,
                        (jboolean) useDistributedLock);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::createHiveTablePartition()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_CREATEHIVEPART_EXCEPTION;
  }
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, 
       "Exit HiveClient_JNI::createHiveTablePartition() called.");
  jenv_->PopLocalFrame(NULL);
  return HVC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////  
HVC_RetCode HiveClient_JNI::getSentryPrivileges(set<string> & groupNames,
                                                const char * tableName,                                 
                                                map<Lng32,char *> & columnNumberToNameMap,
                                                vector<Lng32> & bitMaps /* out */,
                                                vector<Lng32> & positionToColumnNumberVector /* out */)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "Enter HiveClient_JNI::getSentryPrivileges(%s) called.",tableName);

  if (initJNIEnv() != JOI_OK)
     return HVC_ERROR_INIT_PARAM;
  if (getInstance() == NULL)
     return HVC_ERROR_INIT_PARAM;

  jobjectArray js_groupNames = convertToStringObjectArray(groupNames);
  if (js_groupNames == NULL)
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getSentryPrivileges()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_SENTRY_GET_PARAM;
  }

  // The columnNumberToNameMap maps the column number to a column name. When converting
  // to a string array for passing across the JNI, we lose the column number information.
  // That is, the first column name we encounter in the map will go into the 0th position
  // in the string array, and so on. Our caller will get an array of bitmaps with the
  // same position, but won't have the array of names in order to recompute the original
  // column number. So, we have to supply the caller with a vector of column numbers
  // in order to recover that information. That is, the 0th (actually 2nd) returned
  // bit map is for column number positionToColumnNumberVector[0]. Now, if we had a
  // columnNameToNumber map (instead of columnNumberToName) we would not need these
  // gymnastics.
  jobjectArray js_desiredColumns = convertMapToStringObjectArray(columnNumberToNameMap,
                                                positionToColumnNumberVector /* out */);
  if (js_desiredColumns == NULL)
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getSentryPrivileges()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_SENTRY_GET_EXCEPTION;
  }

  jstring js_tableName = jenv_->NewStringUTF(tableName);
  if (js_tableName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_SENTRY_GET_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_SENTRY_GET_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_GET_SENTRY_PRIVS].jm_full_name;
  jintArray j_bitmaps = 
    (jintArray)jenv_->CallStaticObjectMethod(javaClass_, JavaMethods_[JM_GET_SENTRY_PRIVS].methodID, 
                           js_groupNames, js_desiredColumns, js_tableName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getSentryPrivileges()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_SENTRY_GET_EXCEPTION;
  }

  int numBitmaps = convertIntObjectArrayToVector(j_bitmaps, bitMaps /* out */);  
  jenv_->DeleteLocalRef(j_bitmaps);
  if (numBitmaps == 0)
  {
    jenv_->PopLocalFrame(NULL);
    return HVC_DONE;
  }
  jenv_->PopLocalFrame(NULL);
  return HVC_OK;
}


//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////  
HVC_RetCode HiveClient_JNI::getSentryPrivileges(const char * userName,
                                                const char * tableName,                                 
                                                map<Lng32,char *> & columnNumberToNameMap,
                                                vector<Lng32> & bitMaps /* out */,
                                                vector<Lng32> & positionToColumnNumberVector /* out */)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "Enter HiveClient_JNI::getSentryPrivileges(%s,%s) called.",userName,tableName);

  if (initJNIEnv() != JOI_OK)
     return HVC_ERROR_INIT_PARAM;
  if (getInstance() == NULL)
     return HVC_ERROR_INIT_PARAM;

  jstring js_userName = jenv_->NewStringUTF(userName);
  if (js_userName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_SENTRY_GET_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_SENTRY_GET_PARAM;
  }

  // The columnNumberToNameMap maps the column number to a column name. When converting
  // to a string array for passing across the JNI, we lose the column number information.
  // That is, the first column name we encounter in the map will go into the 0th position
  // in the string array, and so on. Our caller will get an array of bitmaps with the
  // same position, but won't have the array of names in order to recompute the original
  // column number. So, we have to supply the caller with a vector of column numbers
  // in order to recover that information. That is, the 0th (actually 2nd) returned
  // bit map is for column number positionToColumnNumberVector[0]. Now, if we had a
  // columnNameToNumber map (instead of columnNumberToName) we would not need these
  // gymnastics.
  jobjectArray js_desiredColumns = convertMapToStringObjectArray(columnNumberToNameMap,
                                                positionToColumnNumberVector /* out */);
  if (js_desiredColumns == NULL)
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getSentryPrivileges()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_SENTRY_GET_EXCEPTION;
  }

  jstring js_tableName = jenv_->NewStringUTF(tableName);
  if (js_tableName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_SENTRY_GET_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_SENTRY_GET_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_GET_SENTRY_PRIVS_U].jm_full_name;
  jintArray j_bitmaps = 
    (jintArray)jenv_->CallStaticObjectMethod(javaClass_, JavaMethods_[JM_GET_SENTRY_PRIVS_U].methodID, 
                           js_userName, js_desiredColumns, js_tableName);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getSentryPrivileges()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_SENTRY_GET_EXCEPTION;
  }

  int numBitmaps = convertIntObjectArrayToVector(j_bitmaps, bitMaps /* out */);  
  if (numBitmaps == 0)
  {
    jenv_->PopLocalFrame(NULL);
    return HVC_DONE;
  }
  jenv_->PopLocalFrame(NULL);
  return HVC_OK;
}
//
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
HVC_RetCode HiveClient_JNI::getHiveTableInfo(const char* schName, 
                                            const char* tabName,
                                            NABoolean readPartnInfo)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "Enter HiveClient_JNI::getHiveTableInfo(%s, %s)", schName, tabName);
  if (initJNIEnv() != JOI_OK)
     return HVC_ERROR_INIT_PARAM;
  jstring js_schName = jenv_->NewStringUTF(schName);
  if (js_schName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_GET_HVT_INFO_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_HVT_INFO_PARAM;
  }
  jstring js_tabName = jenv_->NewStringUTF(tabName);
  if (js_tabName == NULL) 
  {
    GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_GET_HVT_INFO_PARAM));
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_HVT_INFO_PARAM;
  }
  jboolean jReadPartn = readPartnInfo;
  jlong jniObject = (jlong)this;
  tsRecentJMFromJNI = JavaMethods_[JM_GET_HVT_INFO].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, 
                                            JavaMethods_[JM_GET_HVT_INFO].methodID, 
                                            jniObject,
                                            js_schName, js_tabName, jReadPartn);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getHiveTableStr()");
    jenv_->PopLocalFrame(NULL);
    return HVC_ERROR_GET_HVT_INFO_EXCEPTION;
  }
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "Exit HiveClient_JNI::getHiveTableInfo(%s, %s).", schName, tabName);
  jenv_->PopLocalFrame(NULL);
  if (jresult)
     return HVC_OK;
  else
     return HVC_DONE;
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_trafodion_sql_HiveClient
 * Method:    setTableInfo
 * Signature: (J[Ljava/lang/String;[[Ljava/lang/String;[[Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;[I[Ljava/lang/String;[Ljava/lang/String;[Ljava/lang/String;[[Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_trafodion_sql_HiveClient_setTableInfo
  (JNIEnv *jniEnv, jobject jobj, jlong jniObject, jobjectArray tableInfo, jobjectArray colInfo, jobjectArray partKeyInfo, 
                jobjectArray bucketCols, jobjectArray sortCols, jintArray sortColsOrder, jobjectArray paramsKeys, jobjectArray paramsValues,
                jobjectArray partNames, jobjectArray partKeyValues)
{
   HiveClient_JNI *hiveClient = (HiveClient_JNI *)jniObject; 
   hiveClient->setTableInfo(tableInfo, colInfo, partKeyInfo, bucketCols, sortCols, sortColsOrder, paramsKeys, paramsValues, 
                                partNames, partKeyValues);
}
#ifdef __cplusplus
}
#endif

void HiveClient_JNI::setTableInfo(jobjectArray tableInfo, jobjectArray colInfo, jobjectArray partKeyInfo, 
                jobjectArray bucketCols, jobjectArray sortCols, jintArray sortColsOrder,jobjectArray paramsKeys, jobjectArray paramsValues,
                jobjectArray partNames, jobjectArray partKeyValues)
{
   NABoolean exceptionFound = FALSE;
   tableInfo_ = (jobjectArray)jenv_->NewGlobalRef(tableInfo);
   if (jenv_->ExceptionCheck())
      exceptionFound = TRUE; 
   if (! exceptionFound) {
      colInfo_ = (jobjectArray)jenv_->NewGlobalRef(colInfo);
      if (jenv_->ExceptionCheck())
          exceptionFound = TRUE; 
   }
   if (! exceptionFound) {
      if (partKeyInfo != NULL) {
         partKeyInfo_ = (jobjectArray)jenv_->NewGlobalRef(partKeyInfo);
         if (jenv_->ExceptionCheck())
             exceptionFound = TRUE; 
      }
   }
   if (! exceptionFound) {
      if (bucketCols != NULL) {
         bucketCols_ = (jobjectArray)jenv_->NewGlobalRef(bucketCols);
         if (jenv_->ExceptionCheck())
             exceptionFound = TRUE; 
      }
   }
   if (! exceptionFound) {
      if (sortCols != NULL) {
         sortCols_ = (jobjectArray)jenv_->NewGlobalRef(sortCols);
         if (jenv_->ExceptionCheck())
             exceptionFound = TRUE; 
      }
   }
   if (! exceptionFound) {
      if (sortColsOrder != NULL) {
         sortColsOrder_ = (jintArray)jenv_->NewGlobalRef(sortColsOrder);
         if (jenv_->ExceptionCheck())
             exceptionFound = TRUE; 
      }
   }
   if (! exceptionFound) {
      if (paramsKeys != NULL) {
         paramsKeys_ = (jobjectArray)jenv_->NewGlobalRef(paramsKeys);
         if (jenv_->ExceptionCheck())
             exceptionFound = TRUE; 
      }
   }
   if (! exceptionFound) {
      if (paramsValues != NULL) {
         paramsValues_ = (jobjectArray)jenv_->NewGlobalRef(paramsValues);
         if (jenv_->ExceptionCheck())
             exceptionFound = TRUE; 
      }
   }
   if (! exceptionFound) {
      if (partNames != NULL) {
         partNames_ = (jobjectArray)jenv_->NewGlobalRef(partNames);
         if (jenv_->ExceptionCheck())
             exceptionFound = TRUE; 
      }
   }
   if (! exceptionFound) {
      if (partKeyValues != NULL) {
         partKeyValues_ = (jobjectArray)jenv_->NewGlobalRef(partKeyValues);
         if (jenv_->ExceptionCheck())
             exceptionFound = TRUE; 
      }
   }
   ex_assert(! exceptionFound, "Exception in HiveClient_JNI::setTableInfo");
}

void HiveClient_JNI::cleanupTableInfo() 
{
   if (tableInfo_ != NULL) {
      jenv_->DeleteGlobalRef(tableInfo_);
      tableInfo_ = NULL;
   }
   if (colInfo_ != NULL) {
      jenv_->DeleteGlobalRef(colInfo_);
      colInfo_ = NULL;
   }
   if (partKeyInfo_ != NULL) {
      jenv_->DeleteGlobalRef(partKeyInfo_);
      partKeyInfo_ = NULL;
   }
   if (bucketCols_ != NULL) {
      jenv_->DeleteGlobalRef(bucketCols_);
      bucketCols_ = NULL;
   }
   if (sortCols_ != NULL) {
      jenv_->DeleteGlobalRef(sortCols_);
      sortCols_ = NULL;
   }
   if (sortColsOrder_ != NULL) {
      jenv_->DeleteGlobalRef(sortColsOrder_);
      sortColsOrder_ = NULL;
   }
   if (paramsKeys_ != NULL) {
      jenv_->DeleteGlobalRef(paramsKeys_);
      paramsKeys_ = NULL;
   }
   if (paramsValues_ != NULL) {
      jenv_->DeleteGlobalRef(paramsValues_);
      paramsValues_ = NULL;
   }
   if (partNames_ != NULL) {
      jenv_->DeleteGlobalRef(partNames_);
      partNames_ = NULL;
   }
   if (partKeyValues_ != NULL) {
      jenv_->DeleteGlobalRef(partKeyValues_);
      partKeyValues_ = NULL;
   }
}

HVC_RetCode HiveClient_JNI::getHiveTableDesc(NAHeap *heap, hive_tbl_desc *&hiveTableDesc)
{
   HVC_RetCode hvcRetcode;
   jstring jTableInfo[7];
   const char *pTableInfo[7];
   Int64 creationTs;

   jTableInfo[0] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_TABLE_NAME);
   pTableInfo[0] = jenv_->GetStringUTFChars(jTableInfo[0], NULL);

   jTableInfo[1] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_DB_NAME);
   pTableInfo[1] = jenv_->GetStringUTFChars(jTableInfo[1], NULL);

   jTableInfo[2] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_OWNER);
   pTableInfo[2] = jenv_->GetStringUTFChars(jTableInfo[2], NULL);

   jTableInfo[3] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_TABLE_TYPE);
   pTableInfo[3] = jenv_->GetStringUTFChars(jTableInfo[3], NULL);

   jTableInfo[4] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_VIEW_ORIGINAL_TEXT);
   if (jTableInfo[4] != NULL)
      pTableInfo[4] = jenv_->GetStringUTFChars(jTableInfo[4], NULL);
   else
      pTableInfo[4] = NULL;
    
   jTableInfo[5] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_VIEW_EXPANDED_TEXT);
   if (jTableInfo[5] != NULL)
       pTableInfo[5] = jenv_->GetStringUTFChars(jTableInfo[5], NULL);
   else
      pTableInfo[5] = NULL;

   jTableInfo[6] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_CREATE_TIME);
   pTableInfo[6] = jenv_->GetStringUTFChars(jTableInfo[6], NULL);
   creationTs = atol(pTableInfo[6]);
   hive_sd_desc *hiveSdDesc;
   if ((hvcRetcode = populateSD(heap, creationTs, hiveSdDesc)) != HVC_OK)
      return hvcRetcode;

   struct hive_pkey_desc* partKeyDesc; 
   if ((hvcRetcode = populatePartKeyColumns(heap, partKeyDesc)) != HVC_OK)
       return hvcRetcode;
   
   struct hive_tblparams_desc *tblParamsDesc;
   if ((hvcRetcode = populateTableParams(heap, hiveSdDesc, tblParamsDesc)) != HVC_OK)
       return hvcRetcode;

   hiveTableDesc =  new (heap)
      struct hive_tbl_desc(heap, 0, // no tblID with JNI 
                          pTableInfo[0], // Table Name
                          pTableInfo[1], // schema Name
                          pTableInfo[2], // owner
                          pTableInfo[3], // table type
                          creationTs,
                          pTableInfo[4], // view original str
                          pTableInfo[5], // view expanded  str
                          hiveSdDesc, partKeyDesc, tblParamsDesc); 

   for (int i = 0; i < 7 ; i++) {
      if (jTableInfo[i] != NULL) {
         jenv_->ReleaseStringUTFChars(jTableInfo[i], pTableInfo[i]);
         jenv_->DeleteLocalRef(jTableInfo[i]);
      }
   }
   return HVC_OK; 
} 

HVC_RetCode HiveClient_JNI::populateSD(NAHeap *heap, Int64 creationTs, hive_sd_desc* &sdDesc)
{
   HVC_RetCode hvcRetcode;

   if (tableInfo_ == NULL)
      return HVC_ERROR_POPULATE_SDS_ERROR;

   struct hive_sd_desc* lastSd = NULL;
   char fieldTerminator  = '\001';  // this the Hive default ^A or ascii code 1
   char recordTerminator = '\n';    // this is the Hive default


   jstring jSdEntries[9]; 
   const char *pSdEntries[9];

   NABoolean isCompressed = FALSE;
   Int32 numBuckets = 0;

   jSdEntries[0] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_SD_LOCATION);
   if (jSdEntries[0] != NULL)
      pSdEntries[0] = jenv_->GetStringUTFChars(jSdEntries[0], NULL);
   else
      pSdEntries[0] = NULL;

   jSdEntries[1] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_SD_INPUT_FORMAT);
   pSdEntries[1] = jenv_->GetStringUTFChars(jSdEntries[1], NULL);

   jSdEntries[2] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_SD_OUTPUT_FORMAT);
   pSdEntries[2] = jenv_->GetStringUTFChars(jSdEntries[2], NULL);

   jSdEntries[3] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_SD_COMPRESSED);
   pSdEntries[3] = jenv_->GetStringUTFChars(jSdEntries[3], NULL);
   if (strcmp(pSdEntries[3], "true") == 0)
      isCompressed = TRUE;

   jSdEntries[4] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_SD_NUM_BUCKETS);
   pSdEntries[4] = jenv_->GetStringUTFChars(jSdEntries[4], NULL);
   numBuckets = atoi(pSdEntries[4]); 

   jSdEntries[5] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_NULL_FORMAT);
   if (jSdEntries[5] != NULL) 
      pSdEntries[5] = jenv_->GetStringUTFChars(jSdEntries[5], NULL);
   else
      pSdEntries[5] = NULL;
   
   jSdEntries[6] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_FIELD_DELIM);
   if (jSdEntries[6] != NULL)  {
      pSdEntries[6] = jenv_->GetStringUTFChars(jSdEntries[6], NULL);
      fieldTerminator = *pSdEntries[6];
   }
   else
      pSdEntries[6] = NULL;

   jSdEntries[7] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_LINE_DELIM);
   if (jSdEntries[7] != NULL) {
      pSdEntries[7] = jenv_->GetStringUTFChars(jSdEntries[7], NULL);
      recordTerminator = *pSdEntries[7];
   }
   else
      pSdEntries[7] = NULL;

   jSdEntries[8] = (jstring)jenv_->GetObjectArrayElement(tableInfo_, org_trafodion_sql_HiveClient_Table_SERIALIZE_LIB);
   if (jSdEntries[8] != NULL) 
      pSdEntries[8] = jenv_->GetStringUTFChars(jSdEntries[8], NULL);
   else
      pSdEntries[8] = NULL;
  
   struct hive_column_desc* colsDesc; 
   if ((hvcRetcode = populateColumns(heap, colsDesc)) != HVC_OK)
       return hvcRetcode;

   struct hive_skey_desc *sortColsDesc;
   if ((hvcRetcode = populateSortColumns(heap, sortColsDesc)) != HVC_OK)
       return hvcRetcode;

   struct hive_bkey_desc *bucketColsDesc;
   if ((hvcRetcode = populateBucketColumns(heap, bucketColsDesc)) != HVC_OK)
       return hvcRetcode;

   jint numPartns = 0;
    
   struct hive_sd_desc* tableSdDesc = new (heap)
        struct hive_sd_desc(heap, 0, //SdID
                            pSdEntries[0], // Location
                            creationTs,
                            numBuckets, // numBuckets
                            pSdEntries[1], // input format
                            pSdEntries[2], // output format
                            pSdEntries[5],  // null format
                            pSdEntries[8],  // serialization lib
                            hive_sd_desc::TABLE_SD,
                            colsDesc,
                            sortColsDesc,
                            bucketColsDesc,
                            fieldTerminator,
                            recordTerminator,
                            isCompressed,
                            NULL);
   lastSd = tableSdDesc;
   jobjectArray partValuesArray;
   if (partKeyValues_ != NULL) {
      numPartns = jenv_->GetArrayLength(partKeyValues_);     
      for (int partNum = 0 ; partNum < numPartns ; partNum++) {
          jstring jPartName = (jstring)jenv_->GetObjectArrayElement(partNames_, partNum);
          const char *pPartName = jenv_->GetStringUTFChars(jPartName, NULL);
          jobjectArray jPartKeyValues = (jobjectArray)jenv_->GetObjectArrayElement(partKeyValues_, partNum);
          int numPartKeyValues = jenv_->GetArrayLength(jPartKeyValues);       
          NAString partKeyValue(heap);
          for (int partKeyValueIdx = 0 ; partKeyValueIdx < numPartKeyValues; partKeyValueIdx++) {
             jstring jPartKeyValue = (jstring)jenv_->GetObjectArrayElement(jPartKeyValues, partKeyValueIdx);
             const char *pPartKeyValue = jenv_->GetStringUTFChars(jPartKeyValue, NULL);
             if (partKeyValueIdx != 0)
                partKeyValue += ", ";
             partKeyValue += pPartKeyValue;
             jenv_->ReleaseStringUTFChars(jPartKeyValue, pPartKeyValue);
             jenv_->DeleteLocalRef(jPartKeyValue);
          }
          NAString location(pSdEntries[0], heap);
          location += "/";
          location += pPartName;
          struct hive_sd_desc* partSdDesc = new (heap)
                struct hive_sd_desc(heap, 0, //SdID
                            location.data(),
                            creationTs,
                            numBuckets, // numBuckets
                            pSdEntries[1], // input format
                            pSdEntries[2], // output format
                            pSdEntries[5],  // null format
                            pSdEntries[8],  // serialization lib
                            hive_sd_desc::PARTN_SD,
                            colsDesc,
                            sortColsDesc,
                            bucketColsDesc,
                            fieldTerminator,
                            recordTerminator,
                            isCompressed,
                            partKeyValue.data());
          lastSd->next_ = partSdDesc;
          lastSd = partSdDesc;                  
          jenv_->DeleteLocalRef(jPartKeyValues);
          jenv_->ReleaseStringUTFChars(jPartName, pPartName);
          jenv_->DeleteLocalRef(jPartName);
      }
   }    
   
   for (int i = 0; i < 8 ; i++) {
      if (jSdEntries[i] != NULL) {
         jenv_->ReleaseStringUTFChars(jSdEntries[i], pSdEntries[i]);
         jenv_->DeleteLocalRef(jSdEntries[i]);
      }
   }
   sdDesc = tableSdDesc;  
   return HVC_OK; 
}

HVC_RetCode HiveClient_JNI::populateColumns(NAHeap *heap, hive_column_desc* &columns)
{
   HVC_RetCode hvcRetcode;

   if (colInfo_ == NULL) {
      columns = NULL;
      return HVC_OK;
   }
   struct hive_column_desc* result = NULL;
   struct hive_column_desc* last = result;

   jint numCols = jenv_->GetArrayLength(colInfo_);
   jstring jColDetails[2];
   const char *pColDetails[2];
   for (int colIdx = 0 ; colIdx < numCols ; colIdx++) {
      jobjectArray jCol = (jobjectArray)jenv_->GetObjectArrayElement(colInfo_, colIdx);
      jColDetails[0] = (jstring)jenv_->GetObjectArrayElement(jCol, org_trafodion_sql_HiveClient_Col_NAME);
      jColDetails[1] = (jstring)jenv_->GetObjectArrayElement(jCol, org_trafodion_sql_HiveClient_Col_TYPE);
      pColDetails[0] = jenv_->GetStringUTFChars(jColDetails[0], NULL); 
      pColDetails[1] = jenv_->GetStringUTFChars(jColDetails[1], NULL);
      struct hive_column_desc* newCol = new (heap)
           struct hive_column_desc(heap, 0,
                                pColDetails[0],
                                pColDetails[1],
                                colIdx);
       if ( result == NULL ) {
        last = result = newCol;
      } else {
        last->next_ = newCol;
        last = newCol;
      }
      for (int i = 0; i < 2 ; i++) {
         jenv_->ReleaseStringUTFChars(jColDetails[i], pColDetails[i]);
         jenv_->DeleteLocalRef(jColDetails[i]);
      }
      jenv_->DeleteLocalRef(jCol);
   } 
   columns = result;
   return HVC_OK;
}

HVC_RetCode HiveClient_JNI::populatePartKeyColumns(NAHeap *heap, hive_pkey_desc* &columns)
{
   HVC_RetCode hvcRetcode;
   
   if (partKeyInfo_ == NULL) {
      columns = NULL;
      return HVC_OK;
   }
   struct hive_pkey_desc* result = NULL;
   struct hive_pkey_desc* last = result;

   jint numCols = jenv_->GetArrayLength(partKeyInfo_);
   jstring jColDetails[2];
   const char *pColDetails[2];
   for (int colIdx = 0 ; colIdx < numCols ; colIdx++) {
      jobjectArray jCol = (jobjectArray)jenv_->GetObjectArrayElement(partKeyInfo_, colIdx);
      jColDetails[0] = (jstring)jenv_->GetObjectArrayElement(jCol, org_trafodion_sql_HiveClient_Col_NAME);
      jColDetails[1] = (jstring)jenv_->GetObjectArrayElement(jCol, org_trafodion_sql_HiveClient_Col_TYPE);
      pColDetails[0] = jenv_->GetStringUTFChars(jColDetails[0], NULL); 
      pColDetails[1] = jenv_->GetStringUTFChars(jColDetails[1], NULL);
      struct hive_pkey_desc* newCol = new (heap)
           struct hive_pkey_desc(heap, pColDetails[0],
                                pColDetails[1],
                                colIdx);
       if ( result == NULL ) {
        last = result = newCol;
      } else {
        last->next_ = newCol;
        last = newCol;
      }
      for (int i = 0; i < 2 ; i++) {
         jenv_->ReleaseStringUTFChars(jColDetails[i], pColDetails[i]);
         jenv_->DeleteLocalRef(jColDetails[i]);
      }
      jenv_->DeleteLocalRef(jCol);
   } 
   columns = result;
   return HVC_OK;
}

HVC_RetCode HiveClient_JNI::populateSortColumns(NAHeap *heap, hive_skey_desc* &sortColsDesc)
{
   HVC_RetCode hvcRetcode;
   
   if (sortCols_ == NULL) {
      sortColsDesc = NULL;
      return HVC_OK;
   }
   struct hive_skey_desc* result = NULL;
   struct hive_skey_desc* last = result;

   jint numCols = jenv_->GetArrayLength(sortCols_);
   jstring jColName;
   const char *pColName;
   jint *pSortColsOrder = jenv_->GetIntArrayElements(sortColsOrder_, NULL);
   for (int colIdx = 0 ; colIdx < numCols ; colIdx++) {
      jColName = (jstring)jenv_->GetObjectArrayElement(sortCols_, colIdx);
      pColName = jenv_->GetStringUTFChars(jColName, NULL); 
      struct hive_skey_desc* newCol = new (heap)
           struct hive_skey_desc(heap, pColName,
                                pSortColsOrder[colIdx],
                                colIdx);
       if ( result == NULL ) {
        last = result = newCol;
      } else {
        last->next_ = newCol;
        last = newCol;
      }
      jenv_->ReleaseStringUTFChars(jColName, pColName);
      jenv_->DeleteLocalRef(jColName);
   } 
   jenv_->ReleaseIntArrayElements(sortColsOrder_, pSortColsOrder, JNI_ABORT);
   sortColsDesc = result;
   return HVC_OK;
}

HVC_RetCode HiveClient_JNI::populateBucketColumns(NAHeap *heap, hive_bkey_desc* &bucketColsDesc)
{
   HVC_RetCode hvcRetcode;
   
   if (bucketCols_ == NULL) {
      bucketColsDesc = NULL;
      return HVC_OK;
   }
   struct hive_bkey_desc* result = NULL;
   struct hive_bkey_desc* last = result;

   jint numCols = jenv_->GetArrayLength(bucketCols_);
   jstring jColName;
   const char *pColName;
   for (int colIdx = 0 ; colIdx < numCols ; colIdx++) {
      jColName = (jstring)jenv_->GetObjectArrayElement(bucketCols_, colIdx);
      pColName = jenv_->GetStringUTFChars(jColName, NULL); 
      struct hive_bkey_desc* newCol = new (heap)
           struct hive_bkey_desc(heap, pColName,
                                colIdx);
       if ( result == NULL ) {
        last = result = newCol;
      } else {
        last->next_ = newCol;
        last = newCol;
      }
      jenv_->ReleaseStringUTFChars(jColName, pColName);
      jenv_->DeleteLocalRef(jColName);
   } 
   bucketColsDesc = result;
   return HVC_OK;
}

HVC_RetCode HiveClient_JNI::populateTableParams(NAHeap *heap, hive_sd_desc *sd, hive_tblparams_desc* &tblParamsDesc)
{
   if (paramsKeys_ == NULL) {
      tblParamsDesc = NULL;
      return HVC_OK;
   }
  
   const char **paramsKey;
   const char *orcParamsKey[] = {"orc.block.padding", "orc.stripe.size", "orc.compress", 
                            "orc.row.index.stride", "orc.bloom.filter.columns", "orc.bloom.filter.fpp",
                            "orc.create.index", NULL}; 
   const char *parquetParamsKey[] = {"parquet.block.size", "parquet.page.size", "parquet.compression",  
                            "parquet.enable.dictionary", "parquet.dictionary.page.size", "parquet.writer.max-padding", 
                            NULL};

   const char *textParamsKey[] = {"skip.header.line.count", NULL};

   if (sd->isOrcFile())
      paramsKey = orcParamsKey;
   else if (sd->isParquetFile())
      paramsKey = parquetParamsKey;
   else if (sd->isTextFile())
      paramsKey = textParamsKey;
   else {
      tblParamsDesc = NULL;
      return HVC_OK;
   }
   Int32 orcBlockPadding = 1;
   Int64 orcStripeSize = ORC_DEFAULT_STRIPE_SIZE;
   Int32 orcRowIndexStride = ORC_DEFAULT_ROW_INDEX_STRIDE;
   const char *orcCompression = ORC_DEFAULT_COMPRESSION;
   const char *orcBloomFilterColumns = NULL;
   double orcBloomFilterFPP = ORC_DEFAULT_BLOOM_FILTER_FPP;
   NABoolean orcCreateIndex = TRUE;

   NABoolean parquetEnableDictionary = FALSE;
   Int64 parquetBlockSize = PARQUET_DEFAULT_BLOCK_SIZE;
   Int32 parquetPageSize = PARQUET_DEFAULT_PAGE_SIZE;
   const char *parquetCompression = PARQUET_DEFAULT_COMPRESSION;
   Int64 parquetDictionaryPageSize = PARQUET_DEFAULT_DICTIONARY_PAGE_SIZE;
   Int32 parquetWriterMaxPadding = 0;

   Int32 textSkipHeaderCount = 0;

   NAString tblParamsStr(heap);
   const char *pParamsValue[7]; 
   jstring jParamsValue[7];
   for (int i = 0 ; i < 7 ; i++) {
      pParamsValue[i] = NULL;
      jParamsValue[i] = NULL;
   } 
   int numParams = jenv_->GetArrayLength(paramsKeys_);
   for (int paramNo = 0 ; paramNo < numParams; paramNo++) {
      jstring jTmpParamsKey = (jstring)jenv_->GetObjectArrayElement(paramsKeys_, paramNo);
      jstring jTmpParamsValue = (jstring)jenv_->GetObjectArrayElement(paramsValues_, paramNo);
      const char *pTmpParamsKey = jenv_->GetStringUTFChars(jTmpParamsKey, NULL); 
      const char *pTmpParamsValue = jenv_->GetStringUTFChars(jTmpParamsValue, NULL); 
      bool paramFound = false;
      int  paramFoundKeyIdx;
      for (int paramsKeyIdx = 0; paramsKey[paramsKeyIdx] != NULL ; paramsKeyIdx++) {
         if (strcmp(paramsKey[paramsKeyIdx], pTmpParamsKey) == 0) {
            paramFound = true;
            paramFoundKeyIdx = paramsKeyIdx;
            break;
         }
      } 
      if (paramFound) {
         jParamsValue[paramFoundKeyIdx] = jTmpParamsValue; 
         pParamsValue[paramFoundKeyIdx] = pTmpParamsValue; 
         tblParamsStr += pTmpParamsKey;
         tblParamsStr += "=";  
         tblParamsStr += pTmpParamsValue;
         tblParamsStr += "|";  
      } else {
         jenv_->ReleaseStringUTFChars(jTmpParamsValue, pTmpParamsValue);
         jenv_->DeleteLocalRef(jTmpParamsValue);
      }      
      jenv_->ReleaseStringUTFChars(jTmpParamsKey, pTmpParamsKey);
      jenv_->DeleteLocalRef(jTmpParamsKey);
   }
  
   if (sd->isOrcFile()) {
      if (pParamsValue[0] != NULL) {
         if (strcmp(pParamsValue[0], "true") == 0)
            orcBlockPadding = TRUE;
         else
            orcBlockPadding = FALSE;
      }
      if (pParamsValue[1] != NULL) 
         orcStripeSize = atol(pParamsValue[1]);
      if (pParamsValue[2] != NULL) 
         orcRowIndexStride = atoi(pParamsValue[2]);
      if (pParamsValue[3] != NULL) 
         orcCompression = pParamsValue[3];
      orcBloomFilterColumns = pParamsValue[4];
      if (pParamsValue[5] != NULL) 
         orcBloomFilterFPP = atof(pParamsValue[5]);
      if (pParamsValue[6] != NULL) {
         if (strcmp(pParamsValue[6], "true") == 0) 
            orcCreateIndex = TRUE;
         else
            orcCreateIndex = FALSE;
      }
      tblParamsDesc  = new (heap)
         struct hive_tblparams_desc(heap, 
         tblParamsStr.data(),
         orcBlockPadding,
         orcStripeSize, 
         orcRowIndexStride,
         orcCompression,
         orcBloomFilterColumns,
         orcBloomFilterFPP,
         orcCreateIndex);

   }
   else
   if (sd->isParquetFile()) {
      if (pParamsValue[0] != NULL) 
         parquetBlockSize = atol(pParamsValue[0]);
      if (pParamsValue[1] != NULL) 
         parquetPageSize = atoi(pParamsValue[1]);
      if (pParamsValue[2] != NULL) 
         parquetCompression = pParamsValue[2];
      if (pParamsValue[3] != NULL) {
         if (strcmp(pParamsValue[3], "true") == 0) 
            parquetEnableDictionary = TRUE;
         else
            parquetEnableDictionary = FALSE;
      }
      if (pParamsValue[4] != NULL) 
         parquetDictionaryPageSize = atol(pParamsValue[4]);
      if (pParamsValue[5] != NULL) 
         parquetWriterMaxPadding = atol(pParamsValue[5]);
      tblParamsDesc  = new (heap)
           struct hive_tblparams_desc(heap, 
         tblParamsStr,
         parquetWriterMaxPadding,
         parquetBlockSize,
         parquetPageSize,
         parquetCompression,
         NULL,
         parquetDictionaryPageSize,
         parquetEnableDictionary);
   } 
   else if (sd->isTextFile() && (pParamsValue[0] != NULL)) // only one text parameter for now
   {
     textSkipHeaderCount = atoi(pParamsValue[0]);
     tblParamsDesc  = new (heap)
       struct hive_tblparams_desc(heap, tblParamsStr, textSkipHeaderCount,
                                  0,0,NULL,NULL,0,FALSE);
   }
   else
      tblParamsDesc = NULL;

   for (int i = 0; i < 7 ; i++) {
      if (jParamsValue[i] != NULL) {
         jenv_->ReleaseStringUTFChars(jParamsValue[i], pParamsValue[i]);
         jenv_->DeleteLocalRef(jParamsValue[i]);
      }
   }
   return HVC_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
////////////////////////////////////////////////////////////////////////////  
void HiveClient_JNI::logIt(const char* str)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, str);
}

HVC_RetCode HiveClient_JNI::getStripeOrRowgroupInfo(HiveFileType fileType, const char *dir, int numPartKeys, int numFilesToSample, 
	NABoolean readSubdirs, Int64 &modificationTs, HHDFSTableStats *hhdfsTableStats)
{
   if (initJNIEnv() != JOI_OK)
      return HVC_ERROR_INIT_PARAM;
   jstring js_dir = jenv_->NewStringUTF(dir);
   if (js_dir == NULL) {
      GetCliGlobals()->setJniErrorStr(getErrorText(HVC_ERROR_EXISTS_PARAM));
      jenv_->PopLocalFrame(NULL);
      return HVC_ERROR_GET_STRIPE_OR_ROWGROUP_INFO_PARAM;
   }
   jint j_fileType = (jint)fileType;
   jlong j_jniObj = (jlong)this;
   jint j_numPartKeys = numPartKeys;
   jint j_numFilesToSample = numFilesToSample;
   jboolean j_readSubdirs = readSubdirs;
   hhdfsTableStats_ = hhdfsTableStats;
   numFilesStatsRead_ = 0;

   tsRecentJMFromJNI = JavaMethods_[JM_GET_STRIPE_OR_ROWGROUP_INFO].jm_full_name;
   jlong j_modificationTs  = jenv_->CallLongMethod(javaObj_, JavaMethods_[JM_GET_STRIPE_OR_ROWGROUP_INFO].methodID, j_jniObj, 
        j_fileType, js_dir, j_numPartKeys, j_numFilesToSample, j_readSubdirs);
   if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "HiveClient_JNI::getStripeOrRowgroupInfo()");
      jenv_->PopLocalFrame(NULL);
      return HVC_ERROR_GET_STRIPE_OR_ROWGROUP_INFO_EXCEPTION;
   }
   modificationTs = j_modificationTs;
   jenv_->PopLocalFrame(NULL);
   return HVC_OK; 
}

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Class:     org_trafodion_sql_HiveClient
 * Method:    setStripeOrRowgroupInfo
 * Signature: (JILjava/lang/String;Ljava/lang/String;Ljava/lang/String;ZJJSJJI[J[J[JI[Ljava/lang/String;)V
 */
JNIEXPORT void JNICALL Java_org_trafodion_sql_HiveClient_setStripeOrRowgroupInfo
  (JNIEnv *jniEnv, jobject jobj, jlong jniObj, jint partNum, 
   jstring fileName, jstring partDir, jstring partKeyValues, jboolean isFile, jlong modificationTs, jlong fileSize, jshort replicationFactor, 
   jlong blockSize, jlong accessTs, 
   jint numStripesOrRowgroups, jlongArray offsets, jlongArray lengths, jlongArray numRows, jint numBlocks, jobjectArray blockHosts)
{   
   HiveClient_JNI *hiveClient = (HiveClient_JNI *)jniObj; 
   hiveClient->setStripeOrRowgroupInfo(partNum, fileName, partDir, partKeyValues, isFile, modificationTs, fileSize, replicationFactor, blockSize, accessTs, 
              numStripesOrRowgroups, offsets, lengths, numRows, numBlocks, blockHosts);
}

#ifdef __cplusplus
}
#endif

void HiveClient_JNI::setStripeOrRowgroupInfo(jint j_partNum, jstring j_fileName, jstring j_partDir, jstring j_partKeyValues,  
    jboolean j_isFile, jlong j_modificationTs, jlong j_fileSize, jshort j_replicationFactor, jlong j_blockSize, jlong j_accessTs, 
    jint j_numStripesOrRowgroups, jlongArray j_offsets, jlongArray j_lengths, jlongArray j_numRows, jint j_numBlocks, jobjectArray j_blockHosts)
{
   const char *pFileName  = NULL;
   const char *pPartDir = NULL;
   const char *pPartKeyValues = NULL;

   pFileName = jenv_->GetStringUTFChars(j_fileName, NULL);
   pPartDir = jenv_->GetStringUTFChars(j_partDir, NULL);
   if (j_partKeyValues != NULL)
      pPartKeyValues = jenv_->GetStringUTFChars(j_partKeyValues, NULL);
   hdfsFileInfo fileInfo;
   if (j_isFile)
      fileInfo.mKind = kObjectKindFile;
   else
      fileInfo.mKind = kObjectKindDirectory;
   fileInfo.mName = (char *)pFileName;
   fileInfo.mLastMod = j_modificationTs;
   fileInfo.mSize = j_fileSize;
   fileInfo.mReplication = j_replicationFactor;
   fileInfo.mBlockSize = j_blockSize;
   fileInfo.mOwner = NULL;
   fileInfo.mGroup = NULL;
   fileInfo.mPermissions = 0;
   fileInfo.mLastAccess = j_accessTs;
   int partNum = j_partNum;
   int numStripesOrRowgroups = j_numStripesOrRowgroups;
   int numBlocks = j_numBlocks;
   jlong *offsets = NULL;
   jlong *lengths = NULL;
   jlong *numRows = NULL;
   if (numStripesOrRowgroups != 0) {
      offsets = jenv_->GetLongArrayElements(j_offsets, NULL);
      lengths = jenv_->GetLongArrayElements(j_lengths, NULL);
      numRows = jenv_->GetLongArrayElements(j_numRows, NULL);
      numFilesStatsRead_++;
   }
   hhdfsTableStats_->setStripeOrRowgroupInfo(partNum, pPartDir, pPartKeyValues,&fileInfo, numStripesOrRowgroups, offsets, lengths, numRows, numBlocks, j_blockHosts);
   if (numStripesOrRowgroups != 0) {
      jenv_->ReleaseLongArrayElements(j_offsets, offsets, JNI_ABORT);
      jenv_->ReleaseLongArrayElements(j_lengths, lengths, JNI_ABORT);
      jenv_->ReleaseLongArrayElements(j_numRows, numRows, JNI_ABORT);
   }
   jenv_->ReleaseStringUTFChars(j_partDir, pPartDir);
   jenv_->ReleaseStringUTFChars(j_fileName, pFileName);
   if (j_partKeyValues != NULL)
      jenv_->ReleaseStringUTFChars(j_partKeyValues, pPartKeyValues);
}

int HiveClient_JNI::getHostIds(int numBlocks, short rf, jobjectArray j_blockHosts, HostId *blockHosts) 
{
   int numBlockHosts = numBlocks * rf;
   jstring j_hostName;
   const char *hostName;
   int blocksInThisCluster = 0;
   int localBlocks = 0;
   HostId hostId;

   for (int hostNum = 0, blockNum = 0; (blockNum < numBlocks || hostNum < numBlockHosts); blockNum++) {
      blocksInThisCluster = 0;
      for (int blockHostNum = 0 ; blockHostNum < rf ; blockHostNum++) {
          j_hostName = (jstring)jenv_->GetObjectArrayElement(j_blockHosts, hostNum++);
          hostName = jenv_->GetStringUTFChars(j_hostName, NULL);
          if (hostName != NULL) {
             // NAClusterInfo stores only the unqualified
             // first part of the host name, so do the same
             // with the host name we got from HDFS
             const char *pos = strchr(hostName,'.');
             if (pos != NULL)
                *(char *)pos = '\0';
             Lng32 nodeNum = CURRCONTEXT_CLUSTERINFO->mapNodeNameToNodeNum(hostName);
             if (nodeNum >= 0) {
                hostId = nodeNum;
                blocksInThisCluster = 1;
             } else
                hostId = HHDFSFileStats::InvalidHostId;
          }
          else
             hostId = HHDFSFileStats::InvalidHostId;
          blockHosts[(blockHostNum*numBlocks)+blockNum] = hostId;
          jenv_->ReleaseStringUTFChars(j_hostName, hostName);
      }
      if (blocksInThisCluster == 1)
         localBlocks++; 
   } 
   return localBlocks;
}
