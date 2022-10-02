#include "QRLogger.h"
#include "Globals.h"
#include "Context.h"

#include "DistributedLock_JNI.h"

JavaMethodInit* DistributedLock_JNI::JavaMethods_ = NULL;
jclass DistributedLock_JNI::javaClass_ = 0;
bool DistributedLock_JNI::javaMethodsInitialized_ = false;
pthread_mutex_t DistributedLock_JNI::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

static const char* const dlErrorEnumStr[] =
{
  "Doing initialization.",
  "Doing JNI initialization(initJNI).",
  "Doing JNI Env initialization(base class).",
  "Want to lock.",
  "Error on call to longHeldLock.",
  "Want to unlock.",
  "Want to clearlock.",
  "Preparing parameters for DistributedLock_JNI.",
  "Try to close",
  "Try to observe",
  "Try to listNodes"
};

char* DistributedLock_JNI::getErrorText(DL_RetCode errEnum)
{
  if (errEnum < (DL_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)errEnum);
  else
    return (char*)dlErrorEnumStr[errEnum-DL_FIRST - 1];
}

DistributedLock_JNI* DistributedLock_JNI::newInstance(NAHeap *heap, DL_RetCode &retCode)
{
  QRLogger::log(CAT_SQL_HDFS, LL_DEBUG, "DistributedLock_JNI::newInstance() callded.");

  if (initJNIEnv() != JOI_OK)
    return NULL;
	
  retCode = DL_OK;
  DistributedLock_JNI* dl_JNI = new(heap) DistributedLock_JNI(heap);
  if (dl_JNI != NULL) {
    retCode = dl_JNI->init();
    if (retCode != DL_OK) {
      NADELETE(dl_JNI, DistributedLock_JNI, heap);
      dl_JNI = NULL;
    }
  }
  return dl_JNI;
}

DistributedLock_JNI* DistributedLock_JNI::getInstance()
{
  DL_RetCode rc = DL_OK;

  ContextCli *currContext = GetCliGlobals()->currContext();
  DistributedLock_JNI *DLockClient_JNI = currContext->getDLockClient();
  if (DLockClient_JNI == NULL) {
    NAHeap *heap = currContext->exHeap();
    DLockClient_JNI = newInstance(heap, rc);
    currContext->setDLockClient(DLockClient_JNI);
  }
  return DLockClient_JNI;
}

void DistributedLock_JNI::deleteInstance()
{
  ContextCli *currContext = GetCliGlobals()->currContext();
  DistributedLock_JNI *dl_JNI = currContext->getDLockClient();
  if (dl_JNI != NULL) {
    NAHeap *heap = currContext->exHeap();
    NADELETE(dl_JNI, DistributedLock_JNI, heap);
    currContext->setDLockClient(NULL);
  }
}

DL_RetCode DistributedLock_JNI::init()
{
  DL_RetCode rc;
  static char className[] = "org/trafodion/sql/DistributedLockWrapper";

  if (initJNIEnv() != JOI_OK)
    return DL_ERROR_INIT_JNI_ENV;

  if (isInitialized())
    return DL_OK;

  if (javaMethodsInitialized_)
    return (DL_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_,
                                                (Int32)JM_LAST, javaMethodsInitialized_);
  else {
    pthread_mutex_lock(&javaMethodsInitMutex_);
    if (javaMethodsInitialized_) {
      pthread_mutex_unlock(&javaMethodsInitMutex_);
      return (DL_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_,
                                                  (Int32)JM_LAST, javaMethodsInitialized_);
    }
    JavaMethods_ = new JavaMethodInit[JM_LAST];
    JavaMethods_[JM_CTOR].jm_name = "<init>";
    JavaMethods_[JM_CTOR].jm_signature = "()V";
    JavaMethods_[JM_LOCK].jm_name = "lock";
    JavaMethods_[JM_LOCK].jm_signature = "(Ljava/lang/String;J)Z";
    JavaMethods_[JM_LONGHELDLOCK].jm_name = "longHeldLock";
    JavaMethods_[JM_LONGHELDLOCK].jm_signature = "(Ljava/lang/String;Ljava/lang/String;)Ljava/lang/String;";
    JavaMethods_[JM_UNLOCK].jm_name = "unlock";
    JavaMethods_[JM_UNLOCK].jm_signature = "()V";
    JavaMethods_[JM_CLEARLOCK].jm_name = "clearlock";
    JavaMethods_[JM_CLEARLOCK].jm_signature = "()V";
    JavaMethods_[JM_OBSERVE].jm_name = "observe";
    JavaMethods_[JM_OBSERVE].jm_signature = "(Ljava/lang/String;)Z";
    JavaMethods_[JM_LISTNODES].jm_name = "listNodes";
    JavaMethods_[JM_LISTNODES].jm_signature = "(Ljava/lang/String;)V";

    rc = (DL_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_,
                                              (Int32)JM_LAST, javaMethodsInitialized_);
    if (rc == DL_OK)
      javaMethodsInitialized_ = true;	
      pthread_mutex_unlock(&javaMethodsInitMutex_);
  }
  return rc;
}

DL_RetCode DistributedLock_JNI::lock(const char *lockName, long timeout)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG,
                "DistributedLock_JNI::lock(lockName:%s, timeout: %ld)",
                lockName, timeout);
  if (initJNIEnv() != JOI_OK)
    return DL_ERROR_INIT_JNI_ENV;
  jstring js_lockName = jenv_->NewStringUTF(lockName);
  if (js_lockName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(DL_ERROR_PARAM));
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_PARAM;
  }
  jlong js_timeOut = (jlong)timeout;

  tsRecentJMFromJNI = JavaMethods_[JM_LOCK].jm_full_name;
  jboolean jresult = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_LOCK].methodID, js_lockName, js_timeOut);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "DistributedLock_JNI::lock");
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_LOCK;
  }

  if (jresult == false) {
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_LOCK;
  }

  jenv_->PopLocalFrame(NULL);
  return DL_OK;
}

DL_RetCode DistributedLock_JNI::longHeldLock(const char * lockName, const char * data, char * returnedData /* out */, size_t returnedDataMaxLength)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG,
                "DistributedLock_JNI::longHeldLock(lockName: %s, data: %s)",
                lockName, data);
  if (initJNIEnv() != JOI_OK)
    return DL_ERROR_INIT_JNI_ENV;
  jstring js_lockName = jenv_->NewStringUTF(lockName);
  if (js_lockName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(DL_ERROR_PARAM));
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_PARAM;
  }
  jstring js_data = jenv_->NewStringUTF(data);
  if (js_data == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(DL_ERROR_PARAM));
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_LONGHELDLOCK].jm_full_name;
  jstring jresult = (jstring) jenv_->CallObjectMethod(javaObj_, 
    JavaMethods_[JM_LONGHELDLOCK].methodID, js_lockName, js_data);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "DistributedLock_JNI::longHeldLock");
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_LONGHELDLOCK;
  }

  if (jresult == NULL) {
    returnedData[0] = '\0';
  } else {
    const char* char_result = jenv_->GetStringUTFChars(jresult, NULL);
    strncpy(returnedData,char_result,returnedDataMaxLength);
    returnedData[returnedDataMaxLength-1] = '\0';
    jenv_->ReleaseStringUTFChars(jresult, char_result);
  }

  jenv_->PopLocalFrame(NULL);
  return DL_OK;
}

DL_RetCode DistributedLock_JNI::clearlock()
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "DistributedLock_JNI::clearlock()");
  if (initJNIEnv() != JOI_OK)
    return DL_ERROR_INIT_JNI_ENV;

  tsRecentJMFromJNI = JavaMethods_[JM_CLEARLOCK].jm_full_name;
  jenv_->CallVoidMethod(javaObj_, JavaMethods_[JM_CLEARLOCK].methodID);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "DistributedLock_JNI::clearlock");
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_CLEARLOCK;
  }

  jenv_->PopLocalFrame(NULL);
  return DL_OK;
}


DL_RetCode DistributedLock_JNI::unlock()
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "DistributedLock_JNI::unlock()");
  if (initJNIEnv() != JOI_OK)
    return DL_ERROR_INIT_JNI_ENV;

  tsRecentJMFromJNI = JavaMethods_[JM_UNLOCK].jm_full_name;
  jenv_->CallVoidMethod(javaObj_, JavaMethods_[JM_UNLOCK].methodID);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "DistributedLock_JNI::unlock");
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_UNLOCK;
  }

  jenv_->PopLocalFrame(NULL);
    return DL_OK;
}

DL_RetCode DistributedLock_JNI::observe(const char* lockName, bool& locked)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "DistributedLock_JNI::observe()");
  if (initJNIEnv() != JOI_OK)
    return DL_ERROR_INIT_JNI_ENV;

  jstring js_lockName = jenv_->NewStringUTF(lockName);
  if (js_lockName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(DL_ERROR_PARAM));
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_OBSERVE].jm_full_name;
  bool result = jenv_->CallBooleanMethod(javaObj_, JavaMethods_[JM_OBSERVE].methodID, js_lockName);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "DistributedLock_JNI::observe");
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_OBSERVE;
  }

  jenv_->PopLocalFrame(NULL);
  locked = result;

  return DL_OK;
}

DL_RetCode DistributedLock_JNI::listNodes(const char *lockName)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG,
                "DistributedLock_JNI::listNodes(lockName:%s)",
                lockName);
  if (initJNIEnv() != JOI_OK)
    return DL_ERROR_INIT_JNI_ENV;
  jstring js_lockName = jenv_->NewStringUTF(lockName);
  if (js_lockName == NULL) {
    GetCliGlobals()->setJniErrorStr(getErrorText(DL_ERROR_PARAM));
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_PARAM;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_LISTNODES].jm_full_name;
  jenv_->CallVoidMethod(javaObj_, JavaMethods_[JM_LISTNODES].methodID, js_lockName);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "DistributedLock_JNI::listNodes");
    jenv_->PopLocalFrame(NULL);
    return DL_ERROR_LISTNODES;
  }

  jenv_->PopLocalFrame(NULL);
  return DL_OK;
}
