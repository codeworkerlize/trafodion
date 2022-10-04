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
#include <signal.h>
#include "executor/TenantHelper_JNI.h"
#include "common/NAWNodeSet.h"
#include "qmscommon/QRLogger.h"
#include "pthread.h"

//
// ===========================================================================
// ===== Class OversubscriptionInfo
// ===========================================================================

void OversubscriptionInfo::init() {
  int numMeasures = getNumMeasures();

  for (int i = 0; i < numMeasures; i++) measures_[i] = -1;
}

void OversubscriptionInfo::setFromJavaArray(JNIEnv *jenv, jintArray &javaArray) {
  jboolean isCopy;
  jint *arrayElems = jenv->GetIntArrayElements(javaArray, &isCopy);
  int numMeasures = getNumMeasures();

  for (int i = 0; i < numMeasures; i++) measures_[i] = arrayElems[i];

  if (isCopy == JNI_TRUE) jenv->ReleaseIntArrayElements(javaArray, arrayElems, JNI_ABORT);
}

bool OversubscriptionInfo::addDiagnostics(ComDiagsArea *diags, const char *tenantName, float warningThreshold,
                                          float errorThreshold) {
  if (!isOversubscribed()) return false;

  NABoolean generateError = FALSE;
  NABoolean generateWarning = FALSE;
  float ratio = nodeOversubscriptionRatio();

  if (ratio >= errorThreshold)
    generateError = TRUE;
  else if (ratio >= warningThreshold)
    generateWarning = TRUE;

  if (generateError || generateWarning) {
    char buf0[20];
    char buf1[20];

    snprintf(buf0, sizeof(buf0), "%d", measures_[5]);
    snprintf(buf1, sizeof(buf1), "%d", measures_[6]);

    *diags << DgSqlCode(generateError ? -1093 : 1092) << DgString0(tenantName) << DgInt0(measures_[0])
           << DgInt1(measures_[1]) << DgInt2(measures_[2]) << DgInt3(measures_[3]) << DgInt4(measures_[4])
           << DgString1(buf0) << DgString2(buf1);
  }
  return generateError;
}

//
// ===========================================================================
// ===== Class TenantHelper_JNI
// ===========================================================================

JavaMethodInit *TenantHelper_JNI::JavaMethods_ = NULL;
jclass TenantHelper_JNI::javaClass_ = 0;
bool TenantHelper_JNI::javaMethodsInitialized_ = false;
pthread_mutex_t TenantHelper_JNI::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

// Keep in sync with TH_RetCode enum in TenantHelper_JNI.h.
static const char *const thErrorEnumStr[] = {"Preparing parameters for registerTenant().",
                                             "Java exception in registerTenant().",
                                             "No or bad result from registerTenant().",
                                             "Preparing parameters for alterTenant().",
                                             "Java exception in alterTenant().",
                                             "No or bad result from alterTenant().",
                                             "Preparing parameters for unregisterTenant().",
                                             "Java exception in unregisterTenant().",
                                             "Preparing parameters for createLocalCGroup().",
                                             "Java exception in createLocalCGroup()."};

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
char *TenantHelper_JNI::getErrorText(TH_RetCode errEnum) {
  if (errEnum < (TH_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)errEnum);
  else
    return (char *)thErrorEnumStr[errEnum - TH_FIRST - 1];
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
TenantHelper_JNI *TenantHelper_JNI::getInstance() {
  ContextCli *currContext = GetCliGlobals()->currContext();
  NAHeap *heap = currContext->exHeap();
  TenantHelper_JNI *tenantHelper_JNI = currContext->getTenantHelper();
  if (tenantHelper_JNI == NULL) {
    tenantHelper_JNI = new (heap) TenantHelper_JNI(heap);
    TH_RetCode rc = tenantHelper_JNI->init();
    ex_assert(rc == TH_OK, "Error in TenantHelper_JNI::init()");
    currContext->setTenantHelper(tenantHelper_JNI);
  }
  return tenantHelper_JNI;
}

void TenantHelper_JNI::deleteInstance() {
  ContextCli *currContext = GetCliGlobals()->currContext();
  TenantHelper_JNI *tenantHelper_JNI = currContext->getTenantHelper();
  if (tenantHelper_JNI != NULL) {
    NAHeap *heap = currContext->exHeap();
    NADELETE(tenantHelper_JNI, TenantHelper_JNI, heap);
    currContext->setTenantHelper(NULL);
  }
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
TenantHelper_JNI::~TenantHelper_JNI() {
  // Nothing to do
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
TH_RetCode TenantHelper_JNI::init() {
  static char className[] = "com/esgyn/common/TenantHelper";
  TH_RetCode rc = TH_OK;

  if (isInitialized()) return rc;

  if (javaMethodsInitialized_)
    return (TH_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST,
                                                 javaMethodsInitialized_);
  else {
    pthread_mutex_lock(&javaMethodsInitMutex_);
    if (javaMethodsInitialized_) {
      pthread_mutex_unlock(&javaMethodsInitMutex_);
      return (TH_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST,
                                                   javaMethodsInitialized_);
    }
    JavaMethods_ = new JavaMethodInit[JM_LAST];

    JavaMethods_[JM_CTOR].jm_name = "<init>";
    JavaMethods_[JM_CTOR].jm_signature = "()V";
    JavaMethods_[JM_REGISTER_TENANT].jm_name = "registerTenant";
    JavaMethods_[JM_REGISTER_TENANT].jm_signature =
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Z[I)Ljava/lang/String;";
    JavaMethods_[JM_REGISTER_TENANT].isStatic = true;
    JavaMethods_[JM_ALTER_TENANT].jm_name = "alterTenant";
    JavaMethods_[JM_ALTER_TENANT].jm_signature =
        "(Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Ljava/lang/String;Z[I)Ljava/lang/"
        "String;";
    JavaMethods_[JM_ALTER_TENANT].isStatic = true;
    JavaMethods_[JM_UNREGISTER_TENANT].jm_name = "unRegisterTenant";
    JavaMethods_[JM_UNREGISTER_TENANT].jm_signature = "(Ljava/lang/String;)V";
    JavaMethods_[JM_UNREGISTER_TENANT].isStatic = true;
    JavaMethods_[JM_CREATE_LOCAL_CGROUP].jm_name = "createLocalCGroup";
    JavaMethods_[JM_CREATE_LOCAL_CGROUP].jm_signature = "(Ljava/lang/String;)V";
    JavaMethods_[JM_CREATE_LOCAL_CGROUP].isStatic = true;

    rc = (TH_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST,
                                               javaMethodsInitialized_);
    javaMethodsInitialized_ = TRUE;
    pthread_mutex_unlock(&javaMethodsInitMutex_);
  }
  return rc;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
TH_RetCode TenantHelper_JNI::registerTenant(const NAString &tenantName, const NAWNodeSet &assignedNodes,
                                            const NAString &sessionLimit, const NAString &defaultSchema,
                                            NABoolean overrideWarning, NAWNodeSet *&resultAssignedNodes,
                                            OversubscriptionInfo &osi, NAHeap *outHeap) {
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "TenantHelper_JNI::registerTenant(%s, %s, %s) called.", tenantName.data(),
                sessionLimit.data(), defaultSchema.data());

  resultAssignedNodes = NULL;
  osi.init();

  if (initJNIEnv() != JOI_OK) return TH_ERROR_REGISTER_TENANT_PARAM;

  NAText serializedNodes;

  assignedNodes.serialize(serializedNodes);

  jboolean j_overrideWarning = overrideWarning;

  jstring js_tenantName = jenv_->NewStringUTF(tenantName.data());
  jstring js_serializedNodes = jenv_->NewStringUTF(serializedNodes.c_str());
  jstring js_sessionLimit = jenv_->NewStringUTF(sessionLimit.data());
  jstring js_defaultSchema = jenv_->NewStringUTF(defaultSchema.data());
  jintArray jia_oversub = jenv_->NewIntArray(OversubscriptionInfo::getNumMeasures());

  if (js_tenantName == NULL || js_serializedNodes == NULL || js_sessionLimit == NULL || js_defaultSchema == NULL ||
      jia_oversub == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(TH_ERROR_REGISTER_TENANT_PARAM));
    jenv_->PopLocalFrame(NULL);
    return TH_ERROR_REGISTER_TENANT_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_REGISTER_TENANT].jm_full_name;
  jstring jresult = (jstring)jenv_->CallStaticObjectMethod(javaClass_, JavaMethods_[JM_REGISTER_TENANT].methodID,
                                                           js_tenantName, js_serializedNodes, js_sessionLimit,
                                                           js_defaultSchema, j_overrideWarning, jia_oversub);

  osi.setFromJavaArray(jenv_, jia_oversub);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "TenantHelper_JNI::registerTenant");
    jenv_->PopLocalFrame(NULL);
    return TH_ERROR_REGISTER_TENANT_EXCEPTION;
  }

  if (jresult != NULL) {
    const char *char_result = jenv_->GetStringUTFChars(jresult, NULL);

    resultAssignedNodes = NAWNodeSet::deserialize(char_result, outHeap);
    jenv_->ReleaseStringUTFChars(jresult, char_result);
  }

  if (resultAssignedNodes == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(TH_ERROR_REGISTER_TENANT_NO_RESULT));
    jenv_->PopLocalFrame(NULL);
    return TH_ERROR_REGISTER_TENANT_NO_RESULT;
  }

  jenv_->PopLocalFrame(NULL);
  return TH_OK;
}

TH_RetCode TenantHelper_JNI::alterTenant(const NAString &tenantName, const NAWNodeSet &oldAssignedNodes,
                                         const NAWNodeSet &newAssignedNodes, const NAString &sessionLimit,
                                         const NAString &defaultSchema, NABoolean overrideWarning,
                                         NAWNodeSet *&resultAssignedNodes, OversubscriptionInfo &osi, NAHeap *outHeap) {
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "TenantHelper_JNI::alterTenant(%s, %s, %s, %s) called.", tenantName.data(),
                sessionLimit.data(), defaultSchema.data(), overrideWarning ? "TRUE" : "FALSE");

  resultAssignedNodes = NULL;
  osi.init();

  if (initJNIEnv() != JOI_OK) return TH_ERROR_ALTER_TENANT_PARAM;

  NAText serializedOldNodes;
  NAText serializedNewNodes;

  oldAssignedNodes.serialize(serializedOldNodes);
  newAssignedNodes.serialize(serializedNewNodes);

  jboolean j_overrideWarning = overrideWarning;

  jstring js_tenantName = jenv_->NewStringUTF(tenantName.data());
  jstring js_oldSerializedNodes = jenv_->NewStringUTF(serializedOldNodes.c_str());
  jstring js_newSerializedNodes = jenv_->NewStringUTF(serializedNewNodes.c_str());
  jstring js_sessionLimit = jenv_->NewStringUTF(sessionLimit.data());
  jstring js_defaultSchema = jenv_->NewStringUTF(defaultSchema.data());
  jintArray jia_oversub = jenv_->NewIntArray(OversubscriptionInfo::getNumMeasures());

  if (js_tenantName == NULL || js_oldSerializedNodes == NULL || js_newSerializedNodes == NULL ||
      js_sessionLimit == NULL || js_defaultSchema == NULL || jia_oversub == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(TH_ERROR_ALTER_TENANT_PARAM));
    jenv_->PopLocalFrame(NULL);
    return TH_ERROR_ALTER_TENANT_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_ALTER_TENANT].jm_full_name;
  jstring jresult = (jstring)jenv_->CallStaticObjectMethod(
      javaClass_, JavaMethods_[JM_ALTER_TENANT].methodID, js_tenantName, js_oldSerializedNodes, js_newSerializedNodes,
      js_sessionLimit, js_defaultSchema, j_overrideWarning, jia_oversub);

  osi.setFromJavaArray(jenv_, jia_oversub);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "TenantHelper_JNI::alterTenant");
    jenv_->PopLocalFrame(NULL);
    return TH_ERROR_ALTER_TENANT_EXCEPTION;
  }

  if (jresult != NULL) {
    const char *char_result = jenv_->GetStringUTFChars(jresult, NULL);

    resultAssignedNodes = NAWNodeSet::deserialize(char_result, outHeap);
    jenv_->ReleaseStringUTFChars(jresult, char_result);
  }

  if (resultAssignedNodes == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(TH_ERROR_ALTER_TENANT_NO_RESULT));
    jenv_->PopLocalFrame(NULL);
    return TH_ERROR_ALTER_TENANT_NO_RESULT;
  }

  jenv_->PopLocalFrame(NULL);
  return TH_OK;
}

TH_RetCode TenantHelper_JNI::unregisterTenant(const NAString &tenantName) {
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "TenantHelper_JNI::unregisterTenant(%s) called.", tenantName.data());

  if (initJNIEnv() != JOI_OK) return TH_ERROR_UNREGISTER_TENANT_PARAM;

  jstring js_tenantName = jenv_->NewStringUTF(tenantName.data());

  if (js_tenantName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(TH_ERROR_UNREGISTER_TENANT_PARAM));
    jenv_->PopLocalFrame(NULL);
    return TH_ERROR_UNREGISTER_TENANT_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_UNREGISTER_TENANT].jm_full_name;
  jenv_->CallStaticVoidMethod(javaClass_, JavaMethods_[JM_UNREGISTER_TENANT].methodID, js_tenantName);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "TenantHelper_JNI::unregisterTenant");
    jenv_->PopLocalFrame(NULL);
    return TH_ERROR_UNREGISTER_TENANT_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return TH_OK;
}

TH_RetCode TenantHelper_JNI::createLocalCGroup(const char *tenantName) {
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "TenantHelper_JNI::createLocalCGroup(%s) called.", tenantName);

  if (initJNIEnv() != JOI_OK) return TH_ERROR_CREATE_LOCAL_CGROUP_PARAM;

  jstring js_tenantName = jenv_->NewStringUTF(tenantName);

  if (js_tenantName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(TH_ERROR_CREATE_LOCAL_CGROUP_PARAM));
    jenv_->PopLocalFrame(NULL);
    return TH_ERROR_CREATE_LOCAL_CGROUP_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_CREATE_LOCAL_CGROUP].jm_full_name;
  jenv_->CallStaticVoidMethod(javaClass_, JavaMethods_[JM_CREATE_LOCAL_CGROUP].methodID, js_tenantName);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "TenantHelper_JNI::createLocalCGroup");
    jenv_->PopLocalFrame(NULL);
    return TH_ERROR_CREATE_LOCAL_CGROUP_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return TH_OK;
}
