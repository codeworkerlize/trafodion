// **********************************************************************

// **********************************************************************

#include "executor/JavaObjectInterface.h"

#include "LmJavaOptions.h"
#include "cli/Context.h"
#include "cli/Globals.h"
#include "common/ComUser.h"
#include "executor/ex_ex.h"
#include "qmscommon/QRLogger.h"

#define DEFAULT_JVM_MAX_HEAP_SIZE          256
#define ESP_DEFAULT_JVM_MAX_HEAP_SIZE      512
#define DEFAULT_COMPRESSED_CLASSSPACE_SIZE 128
#define DEFAULT_MAX_METASPACE_SIZE         128
#define TRAF_DEFAULT_JNIHANDLE_CAPACITY    32
#define DEFAULT_BR_JVM_MAX_HEAP_SIZE       2048

// ===========================================================================
// ===== Class JavaObjectInterface
// ===========================================================================

JavaVM *JavaObjectInterface::jvm_ = NULL;
jint JavaObjectInterface::jniHandleCapacity_ = 0;
int JavaObjectInterface::debugPort_ = 0;
int JavaObjectInterface::debugTimeout_ = 0;

__thread JNIEnv *jenv_ = NULL;
__thread NAString *tsRecentJMFromJNI = NULL;
__thread NAString *tsSqlJniErrorStr = NULL;
jclass JavaObjectInterface::gThrowableClass = NULL;
jclass JavaObjectInterface::gStackTraceClass = NULL;
jclass JavaObjectInterface::gOOMErrorClass = NULL;
jclass JavaObjectInterface::gLockTimeOutExceptionClass = NULL;
jclass JavaObjectInterface::gDeadLockExceptionClass = NULL;
jclass JavaObjectInterface::gLockCancelOperationExceptionClass = NULL;
jclass JavaObjectInterface::gRegionServerStoppedExceptionClass = NULL;
jclass JavaObjectInterface::gNotServingRegionExceptionClass = NULL;
jclass JavaObjectInterface::gLockNotEnoughResourcsExceptionClass = NULL;
jmethodID JavaObjectInterface::gGetStackTraceMethodID = NULL;
jmethodID JavaObjectInterface::gThrowableToStringMethodID = NULL;
jmethodID JavaObjectInterface::gStackFrameToStringMethodID = NULL;
jmethodID JavaObjectInterface::gGetCauseMethodID = NULL;

void setSqlJniErrorStr(NAString &errorMsg) {
  if (tsSqlJniErrorStr != NULL) delete tsSqlJniErrorStr;
  tsSqlJniErrorStr = new NAString(errorMsg);
}

void setSqlJniErrorStr(const char *errorMsg) {
  if (tsSqlJniErrorStr != NULL) delete tsSqlJniErrorStr;
  tsSqlJniErrorStr = new NAString(errorMsg);
}

const char *getSqlJniErrorStr() {
  if (tsSqlJniErrorStr == NULL)
    return "";
  else
    return tsSqlJniErrorStr->data();
}

static const char *const joiErrorEnumStr[] = {"All is well.",
                                              "Checking for existing JVMs failed",
                                              "Attaching to a JVM of the wrong version",
                                              "Attaching to an existing JVM failed",
                                              "Creating a new JVM failed ",
                                              "JNI FindClass() failed",
                                              "JNI GetMethodID() failed",
                                              "JNI NewObject() failed",
                                              "initJNIEnv() failed",
                                              "Unknown Error"};

void initJavaOptionFromEnv(LmJavaOptions *options) {
  const char *javaOptionStr = getenv("JVM_JAVA_OPTIONS");
  if (javaOptionStr) {
    const char *delimiters = ";";
    options->addOptions(javaOptionStr, delimiters, TRUE);
  }
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
char *JavaObjectInterface::getErrorText(JOI_RetCode errEnum) {
  if (errEnum >= JOI_LAST) {
    abort();
  } else
    return (char *)joiErrorEnumStr[errEnum];
}

#define CHECK_RESULT_AND_RETURN(result, errid)                                                                         \
  do {                                                                                                                 \
    if ((result) != JNI_OK) {                                                                                          \
      char buffer[1024];                                                                                               \
      snprintf(buffer, sizeof(buffer), "%s with error %d %s:%d", getErrorText((errid)), (result), __FILE__, __LINE__); \
      GetCliGlobals()->setJniErrorStr(buffer);                                                                         \
      return (errid);                                                                                                  \
    }                                                                                                                  \
  } while (false)

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
JavaObjectInterface::~JavaObjectInterface() {
  if (jenv_ == NULL) return;
  if ((long)javaObj_ != -1) jenv_->DeleteGlobalRef(javaObj_);
  javaObj_ = NULL;
  isInitialized_ = FALSE;
}

void JavaObjectInterface::setJavaObject(jobject jobj) {
  if ((long)javaObj_ != -1) jenv_->DeleteGlobalRef(javaObj_);
  javaObj_ = jenv_->NewGlobalRef(jobj);
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
char *JavaObjectInterface::buildClassPath() {
  char *classPath = getenv("CLASSPATH");
  char *confPath = getenv("TRAF_CONF");
  int size = 128;
  if (classPath != NULL) {
    size += strlen(classPath);
  }
  if (confPath != NULL) {
    size += strlen(confPath);
  }
  char *classPathBuffer = (char *)malloc(size);

  strcpy(classPathBuffer, "-Djava.class.path=");
  if (confPath != NULL) {
    strcat(classPathBuffer, confPath);
    strcat(classPathBuffer, ":");
  }
  if (classPath != NULL) {
    strcat(classPathBuffer, classPath);
  }
  return classPathBuffer;
}

// helper method for JavaObjectInterface::createJVM() below
static NABoolean isDefinedInOptions(LmJavaOptions *options, const char *prefix) {
  return (options && options->entries() && options->findByPrefix(prefix) != NULL_COLL_INDEX);
}

#define MAX_NO_JVM_OPTIONS 13
//////////////////////////////////////////////////////////////////////////////
// Create a new JVM instance.
//////////////////////////////////////////////////////////////////////////////
#pragma OPTIMIZE OFF
int JavaObjectInterface::createJVM(LmJavaOptions *options) {
  JavaVMInitArgs jvm_args;
  JavaVMOption jvm_options[MAX_NO_JVM_OPTIONS + (options ? options->entries() : 0)];
  int numJVMOptions = 0;

  // variables for option strings used below
  char *classPathArg = NULL;
  char maxHeapOptions[64];
  char compressedClassSpaceSizeOptions[64];
  char maxMetaspaceSizeOptions[64];
  char initHeapOptions[64];
  char debugOptions[100];
  const char *oomOption = "-XX:+HeapDumpOnOutOfMemoryError";
  char *oomDumpDir = NULL;
  CliGlobals *cliGlobals = GetCliGlobals();
  NABoolean espProcess = cliGlobals->isEspProcess();
  NABoolean sqlciProcess = cliGlobals->isSqlciProcess();

  // call the helper method defined above to check for already defined options
  if (!isDefinedInOptions(options, "-Djava.class.path=")) {
    classPathArg = buildClassPath();
    jvm_options[numJVMOptions].optionString = classPathArg;
    jvm_options[numJVMOptions].extraInfo = NULL;
    QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "Using classpath: %s", jvm_options[numJVMOptions].optionString);
    numJVMOptions++;
  }
  if (!isDefinedInOptions(options, "-Dlog4j.configuration=")) {
    const char *logDir = getenv("TRAF_LOG");
    int size;
    if (logDir != NULL) {
      size = strlen(logDir) + 32;
      char *logDirBuffer = (char *)malloc(size);
      strcpy(logDirBuffer, "-DTRAF_LOG=");
      strcat(logDirBuffer, logDir);
      jvm_options[numJVMOptions].optionString = logDirBuffer;
      jvm_options[numJVMOptions].extraInfo = NULL;
      QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "Adding TRAF_LOG to JVM property: %s",
                    jvm_options[numJVMOptions].optionString);
      numJVMOptions++;
    }

    char *hostName = getenv("HOSTNAME");
    if (hostName != NULL) {
      size = strlen(hostName) + 32;
      char *hostNameBuffer = (char *)malloc(size);
      strcpy(hostNameBuffer, "-DHOSTNAME=");
      strcat(hostNameBuffer, hostName);
      jvm_options[numJVMOptions].optionString = hostNameBuffer;
      jvm_options[numJVMOptions].extraInfo = NULL;
      QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "Adding HOSTNAME to JVM property: %s",
                    jvm_options[numJVMOptions].optionString);
      numJVMOptions++;
    }

    size = 64;
    char *logPathBuffer = (char *)malloc(size);
    strcpy(logPathBuffer, "-Dlog4j.configuration=log4j.udr.config");

    jvm_options[numJVMOptions].optionString = logPathBuffer;
    jvm_options[numJVMOptions].extraInfo = NULL;
    QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "Using default log config: %s",
                  jvm_options[numJVMOptions].optionString);
    numJVMOptions++;
  }

  if (!isDefinedInOptions(options, "-Xmx")) {
    int maxHeapEnvvarMB;
    const char *maxHeapEnvvarStr;
    if (!espProcess) {
      maxHeapEnvvarMB = DEFAULT_JVM_MAX_HEAP_SIZE;
      maxHeapEnvvarStr = "JVM_MAX_HEAP_SIZE_MB";
    } else {
      maxHeapEnvvarMB = ESP_DEFAULT_JVM_MAX_HEAP_SIZE;
      maxHeapEnvvarStr = "ESP_JVM_MAX_HEAP_SIZE_MB";
    }
    const char *maxHeapSizeStr = getenv(maxHeapEnvvarStr);
    if (maxHeapSizeStr != NULL) {
      maxHeapEnvvarMB = atoi(maxHeapSizeStr);
      if (maxHeapEnvvarMB <= 0) {
        if (!espProcess)
          maxHeapEnvvarMB = DEFAULT_JVM_MAX_HEAP_SIZE;
        else
          maxHeapEnvvarMB = ESP_DEFAULT_JVM_MAX_HEAP_SIZE;
      }
    }

    if (sqlciProcess) {
      maxHeapEnvvarMB = DEFAULT_BR_JVM_MAX_HEAP_SIZE;
      if (maxHeapEnvvarMB < cliGlobals->getSqlciMaxHeap()) {
        maxHeapEnvvarMB = cliGlobals->getSqlciMaxHeap();
      }
      QRLogger::log(CAT_SQL_EXE, LL_INFO, "Backup Restore JVM Max heap option -Xmx: %d", maxHeapEnvvarMB);
    }

    snprintf(maxHeapOptions, sizeof(maxHeapOptions), "-Xmx%dm", maxHeapEnvvarMB);
    jvm_options[numJVMOptions].optionString = maxHeapOptions;
    jvm_options[numJVMOptions].extraInfo = NULL;
    QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "Max heap option: %s", jvm_options[numJVMOptions].optionString);
    numJVMOptions++;
  }

  const char *pfp = getenv("JVM_PRESERVE_FP");
  if (pfp != NULL) {
    jvm_options[numJVMOptions].optionString = (char *)"-XX:+PreserveFramePointer";
    jvm_options[numJVMOptions].extraInfo = NULL;
    QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "Preserve FramePointer Option: %s",
                  jvm_options[numJVMOptions].optionString);
    numJVMOptions++;
  }

  const char *jvmGC = getenv("JVM_GC_OPTION");
  if (jvmGC != NULL) {
    jvm_options[numJVMOptions].optionString = (char *)jvmGC;
    jvm_options[numJVMOptions].extraInfo = NULL;
    QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "GC Option: %s", jvm_options[numJVMOptions].optionString);
    numJVMOptions++;
  }

  char jvmGC_PTStrOption[64] = "-XX:ParallelGCThreads=2";
  const char *jvmGCPTStr = getenv("JVM_GC_OPTION_PT");
  if (jvmGCPTStr) {
    sprintf(jvmGC_PTStrOption, "-XX:ParallelGCThreads=%d", atoi(jvmGCPTStr));
  }
  jvm_options[numJVMOptions].optionString = jvmGC_PTStrOption;
  jvm_options[numJVMOptions].extraInfo = NULL;
  numJVMOptions++;

  char jvmGC_CPTStrOption[64] = "-XX:CICompilerCount=4";
  const char *jvmGC_CPTStr = getenv("JVM_GC_OPTION_CPT");
  if (jvmGC_CPTStr) {
    sprintf(jvmGC_CPTStrOption, "-XX:CICompilerCount=%d", atoi(jvmGC_CPTStr));
  }
  jvm_options[numJVMOptions].optionString = jvmGC_CPTStrOption;
  jvm_options[numJVMOptions].extraInfo = NULL;
  numJVMOptions++;

  if (!isDefinedInOptions(options, "-XX:CompressedClassSpaceSize=")) {
    int compressedClassSpaceSize = 0;
    const char *compressedClassSpaceSizeStr = getenv("JVM_COMPRESSED_CLASS_SPACE_SIZE");
    if (compressedClassSpaceSizeStr != NULL) compressedClassSpaceSize = atoi(compressedClassSpaceSizeStr);
    if (compressedClassSpaceSize <= 0) compressedClassSpaceSize = DEFAULT_COMPRESSED_CLASSSPACE_SIZE;
    snprintf(compressedClassSpaceSizeOptions, sizeof(compressedClassSpaceSizeOptions),
             "-XX:CompressedClassSpaceSize=%dm", compressedClassSpaceSize);
    jvm_options[numJVMOptions].optionString = compressedClassSpaceSizeOptions;
    jvm_options[numJVMOptions].extraInfo = NULL;
    QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "CompressedClassSpaceSize: %s",
                  jvm_options[numJVMOptions].optionString);
    numJVMOptions++;
  }

  if (!isDefinedInOptions(options, "-XX:MaxMetaspaceSize=")) {
    int maxMetaspaceSize = 0;
    const char *maxMetaspaceSizeStr = getenv("JVM_MAX_METASPACE_SIZE");
    if (maxMetaspaceSizeStr != NULL) maxMetaspaceSize = atoi(maxMetaspaceSizeStr);
    if (maxMetaspaceSize <= 0) maxMetaspaceSize = DEFAULT_MAX_METASPACE_SIZE;
    snprintf(maxMetaspaceSizeOptions, sizeof(maxMetaspaceSizeOptions), "-XX:MaxMetaspaceSize=%dm", maxMetaspaceSize);
    jvm_options[numJVMOptions].optionString = maxMetaspaceSizeOptions;
    jvm_options[numJVMOptions].extraInfo = NULL;
    QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "MaxMetaspaceSize: %s", jvm_options[numJVMOptions].optionString);
    numJVMOptions++;
  }

  if (!isDefinedInOptions(options, "-Xms")) {
    const char *initHeapSizeStr = getenv("JVM_INIT_HEAP_SIZE_MB");
    if (initHeapSizeStr != NULL) {
      const int initHeapEnvvarMB = atoi(initHeapSizeStr);
      if (initHeapEnvvarMB > 0) {
        snprintf(initHeapOptions, sizeof(initHeapOptions), "-Xms%dm", initHeapEnvvarMB);
        jvm_options[numJVMOptions].optionString = initHeapOptions;
        jvm_options[numJVMOptions].extraInfo = NULL;
        QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "Init heap option: %s", jvm_options[numJVMOptions].optionString);
        numJVMOptions++;
      }
    }
  }

  if (!isDefinedInOptions(options, "-agentlib:jdwp=")) {
    int debugPort = 0;
    const char *debugPortStr = getenv("JVM_DEBUG_PORT");
    if (debugPortStr != NULL) debugPort = atoi(debugPortStr);
    if (debugPort > 0) {
      const char *debugTimeoutStr = getenv("JVM_DEBUG_TIMEOUT");
      if (debugTimeoutStr != NULL) debugTimeout_ = atoi(debugTimeoutStr);
      const char *suspendOnDebug = getenv("JVM_SUSPEND_ON_DEBUG");

      debugPort_ = debugPort;
      // to allow debugging multiple processes at the same time,
      // specify a port that is a multiple of 1000 and the code will
      // add pid mod 1000 to the port number to use
      if (debugPort_ % 1000 == 0) debugPort_ += (cliGlobals->myPin() % 1000);
      snprintf(debugOptions, sizeof(debugOptions), "-agentlib:jdwp=transport=dt_socket,address=%d,server=y,timeout=%d",
               debugPort_, debugTimeout_);
      if (suspendOnDebug != NULL)
        strcat(debugOptions, ",suspend=y");
      else
        strcat(debugOptions, ",suspend=n");
      jvm_options[numJVMOptions].optionString = debugOptions;
      jvm_options[numJVMOptions].extraInfo = NULL;
      QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_WARN, "Debugging JVM with options: %s",
                    jvm_options[numJVMOptions].optionString);
      numJVMOptions++;
    }
  }

  if (!isDefinedInOptions(options, oomOption)) {
    jvm_options[numJVMOptions].optionString = (char *)oomOption;
    jvm_options[numJVMOptions].extraInfo = NULL;
    numJVMOptions++;
  }

  if (!isDefinedInOptions(options, "-XX:HeapDumpPath=")) {
    char *mySqLogs = getenv("TRAF_LOG");
    int len;
    if (mySqLogs != NULL) {
      len = strlen(mySqLogs);
      oomDumpDir = new char[len + 50];
      strcpy(oomDumpDir, "-XX:HeapDumpPath=");
      strcat(oomDumpDir, mySqLogs);
      jvm_options[numJVMOptions].optionString = (char *)oomDumpDir;
      jvm_options[numJVMOptions].extraInfo = NULL;
      numJVMOptions++;
    }
  }

  ex_assert((numJVMOptions < MAX_NO_JVM_OPTIONS), "Buffer overflow in JVM options");

  if (options)
    for (CollIndex o = 0; o < options->entries(); o++) {
      jvm_options[numJVMOptions].optionString = (char *)options->getOption(o);
      jvm_options[numJVMOptions].extraInfo = NULL;
      QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "Option passed to JavaObjectInterface::createJVM(): %s",
                    jvm_options[numJVMOptions].optionString);
      numJVMOptions++;
    }

  jvm_args.version = JNI_VERSION_1_6;
  jvm_args.options = jvm_options;
  jvm_args.nOptions = numJVMOptions;
  jvm_args.ignoreUnrecognized = 1;

  for (int i = 0; i < numJVMOptions; i++) {
    QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_INFO, "JNI_CreateJavaVM [%d]: %s", i, jvm_options[i].optionString);
  }

  int ret = JNI_CreateJavaVM(&jvm_, (void **)&jenv_, &jvm_args);
  if (classPathArg) free(classPathArg);
  if (oomDumpDir) delete oomDumpDir;
  return ret;
}
#pragma OPTIMIZE ON

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
JOI_RetCode JavaObjectInterface::initJVM(LmJavaOptions *options) {
  jint result;

  if (jvm_ == NULL) {
    jsize jvm_count = 0;
    // Is there an existing JVM already created?
    result = JNI_GetCreatedJavaVMs(&jvm_, 1, &jvm_count);
    CHECK_RESULT_AND_RETURN(result, JOI_ERROR_CHECK_JVM);
    if (jvm_count == 0) {
      // No - create a new one.
      result = createJVM(options);
      CHECK_RESULT_AND_RETURN(result, JOI_ERROR_CREATE_JVM);
      QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "Created a new JVM.");
    }
    char *jniHandleCapacityStr = getenv("TRAF_JNIHANDLE_CAPACITY");
    if (jniHandleCapacityStr != NULL) jniHandleCapacity_ = atoi(jniHandleCapacityStr);
    if (jniHandleCapacity_ == 0) jniHandleCapacity_ = TRAF_DEFAULT_JNIHANDLE_CAPACITY;
  }
  if (jenv_ == NULL) {
    // We found a JVM, can we use it?
    result = jvm_->GetEnv((void **)&jenv_, JNI_VERSION_1_6);
    switch (result) {
      case JNI_OK:
        QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "Attached to an existing JVM.");
        break;

      case JNI_EDETACHED:
        result = jvm_->AttachCurrentThread((void **)&jenv_, NULL);
        if (result != JNI_OK) {
          QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_ERROR, "Attaching to a JVM with error.");
        }
        CHECK_RESULT_AND_RETURN(result, JOI_ERROR_ATTACH_JVM);
        QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_DEBUG, "Attached to an existing JVM from another thread.");
        break;

      case JNI_EVERSION:
        QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_ERROR, "Attaching to a JVM of the wrong version.");
        CHECK_RESULT_AND_RETURN(result, JOI_ERROR_JVM_VERSION);
        break;

      default:
        QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_ERROR, "Unknown error Attaching to an existing JVM.");
        CHECK_RESULT_AND_RETURN(result, JOI_ERROR_ATTACH_JVM);
        break;
    }
  }
  jclass lJavaClass;
  if (gThrowableClass == NULL) {
    lJavaClass = jenv_->FindClass("java/lang/Throwable");
    if (lJavaClass != NULL) {
      gThrowableClass = (jclass)jenv_->NewGlobalRef(lJavaClass);
      jenv_->DeleteLocalRef(lJavaClass);
      gGetStackTraceMethodID = jenv_->GetMethodID(gThrowableClass, "getStackTrace", "()[Ljava/lang/StackTraceElement;");
      gThrowableToStringMethodID = jenv_->GetMethodID(gThrowableClass, "toString", "()Ljava/lang/String;");
      gGetCauseMethodID = jenv_->GetMethodID(gThrowableClass, "getCause", "()Ljava/lang/Throwable;");
    }
  }
  if (gStackTraceClass == NULL) {
    lJavaClass = (jclass)jenv_->FindClass("java/lang/StackTraceElement");
    if (lJavaClass != NULL) {
      gStackTraceClass = (jclass)jenv_->NewGlobalRef(lJavaClass);
      jenv_->DeleteLocalRef(lJavaClass);
      gStackFrameToStringMethodID = jenv_->GetMethodID(gStackTraceClass, "toString", "()Ljava/lang/String;");
    }
  }
  if (gOOMErrorClass == NULL) {
    lJavaClass = (jclass)jenv_->FindClass("java/lang/OutOfMemoryError");
    if (lJavaClass != NULL) {
      gOOMErrorClass = (jclass)jenv_->NewGlobalRef(lJavaClass);
      jenv_->DeleteLocalRef(lJavaClass);
    }
  }
  if (gLockTimeOutExceptionClass == NULL) {
    lJavaClass = (jclass)jenv_->FindClass(
        "org/apache/hadoop/hbase/coprocessor/transactional/lock/exception/LockTimeOutException");
    if (lJavaClass != NULL) {
      gLockTimeOutExceptionClass = (jclass)jenv_->NewGlobalRef(lJavaClass);
      jenv_->DeleteLocalRef(lJavaClass);
    }
  }
  if (gDeadLockExceptionClass == NULL) {
    lJavaClass =
        (jclass)jenv_->FindClass("org/apache/hadoop/hbase/coprocessor/transactional/lock/exception/DeadLockException");
    if (lJavaClass != NULL) {
      gDeadLockExceptionClass = (jclass)jenv_->NewGlobalRef(lJavaClass);
      jenv_->DeleteLocalRef(lJavaClass);
    }
  }
  if (gLockCancelOperationExceptionClass == NULL) {
    lJavaClass = (jclass)jenv_->FindClass(
        "org/apache/hadoop/hbase/coprocessor/transactional/lock/exception/LockCancelOperationException");
    if (lJavaClass != NULL) {
      gLockCancelOperationExceptionClass = (jclass)jenv_->NewGlobalRef(lJavaClass);
      jenv_->DeleteLocalRef(lJavaClass);
    }
  }
  if (gRegionServerStoppedExceptionClass == NULL) {
    lJavaClass = (jclass)jenv_->FindClass("org/apache/hadoop/hbase/regionserver/RegionServerStoppedException");
    if (lJavaClass != NULL) {
      gRegionServerStoppedExceptionClass = (jclass)jenv_->NewGlobalRef(lJavaClass);
      jenv_->DeleteLocalRef(lJavaClass);
    }
  }
  if (gNotServingRegionExceptionClass == NULL) {
    lJavaClass = (jclass)jenv_->FindClass("org/apache/hadoop/hbase/NotServingRegionException");
    if (lJavaClass != NULL) {
      gNotServingRegionExceptionClass = (jclass)jenv_->NewGlobalRef(lJavaClass);
      jenv_->DeleteLocalRef(lJavaClass);
    }
  }
  if (gLockNotEnoughResourcsExceptionClass == NULL) {
    lJavaClass = (jclass)jenv_->FindClass(
        "org/apache/hadoop/hbase/coprocessor/transactional/lock/exception/LockNotEnoughResourcsException");
    if (lJavaClass != NULL) {
      gLockNotEnoughResourcsExceptionClass = (jclass)jenv_->NewGlobalRef(lJavaClass);
      jenv_->DeleteLocalRef(lJavaClass);
    }
  }
  return JOI_OK;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
JOI_RetCode JavaObjectInterface::init(const char *className, jclass &javaClass, JavaMethodInit *JavaMethods,
                                      int howManyMethods, bool methodsInitialized) {
  if (isInitialized_) return JOI_OK;

  JOI_RetCode retCode = JOI_OK;

  // Make sure the JVM environment is set up correctly.
  jclass lJavaClass;
  LmJavaOptions *options = new LmJavaOptions();
  initJavaOptionFromEnv(options);
  retCode = initJVM(options);
  delete options;
  if (retCode != JOI_OK) return retCode;

  if (methodsInitialized == FALSE || javaObj_ == NULL) {
    if (javaClass == 0) {
      lJavaClass = jenv_->FindClass(className);
      if (jenv_->ExceptionCheck()) {
        char errMsg[200];
        snprintf(errMsg, sizeof(errMsg), "Exception in FindClass(%s)", className);
        getExceptionDetails(__FILE__, __LINE__, errMsg);
        return JOI_ERROR_FINDCLASS;
      }
      if (lJavaClass == 0) {
        QRLogger::log(CAT_SQL_HDFS_JNI_TOP, LL_ERROR, "Error in FindClass(%s).", className);
        return JOI_ERROR_FINDCLASS;
      }
      javaClass = (jclass)jenv_->NewGlobalRef(lJavaClass);
      jenv_->DeleteLocalRef(lJavaClass);
    }
    // Initialize the method pointers.
    if (!methodsInitialized) {
      for (int i = 0; i < howManyMethods; i++) {
        JavaMethods[i].jm_full_name = new (heap_) NAString(className, heap_);
        JavaMethods[i].jm_full_name->append('.', 1);
        JavaMethods[i].jm_full_name->append(JavaMethods[i].jm_name);
        if (JavaMethods[i].isStatic)  // annoyingly, JNI makes us call a different method for static methods
          JavaMethods[i].methodID =
              jenv_->GetStaticMethodID(javaClass, JavaMethods[i].jm_name, JavaMethods[i].jm_signature);
        else
          JavaMethods[i].methodID = jenv_->GetMethodID(javaClass, JavaMethods[i].jm_name, JavaMethods[i].jm_signature);

        if (JavaMethods[i].methodID == 0 || jenv_->ExceptionCheck()) {
          jenv_->ExceptionClear();
          JavaMethods[i].methodID =
              jenv_->GetStaticMethodID(javaClass, JavaMethods[i].jm_name, JavaMethods[i].jm_signature);
          if (JavaMethods[i].methodID == 0 || jenv_->ExceptionCheck()) {
            getExceptionDetails(__FILE__, __LINE__, "GetMethodID()");
            return JOI_ERROR_GETMETHOD;
          }
        }
      }
    }

    if (javaObj_ == NULL) {
      // Allocate an object of the Java class, and call its constructor.
      // The constructor must be the first entry in the methods array.
      jobject jObj = jenv_->NewObject(javaClass, JavaMethods[0].methodID);
      if (jObj == 0 || jenv_->ExceptionCheck()) {
        char errMsg[200];
        snprintf(errMsg, sizeof(errMsg), "Error in NewObject() for class %s.", className);
        getExceptionDetails(__FILE__, __LINE__, errMsg);
        return JOI_ERROR_NEWOBJ;
      }
      javaObj_ = jenv_->NewGlobalRef(jObj);
      jenv_->DeleteLocalRef(jObj);
    }
  }

  isInitialized_ = true;
  return JOI_OK;
}

//////////////////////////////////////////////////////////////////////////////
//
//////////////////////////////////////////////////////////////////////////////
void JavaObjectInterface::logError(std::string &cat, const char *methodName, const char *result) {
  if (result == NULL)
    QRLogger::log(cat, LL_ERROR, "Unknown Java error in %s.", methodName);
  else
    QRLogger::log(cat, LL_ERROR, "%s error: %s.", methodName, result);
}

//
//////////////////////////////////////////////////////////////////////////////
void JavaObjectInterface::logError(std::string &cat, const char *methodName, jstring jresult) {
  if (jresult == NULL)
    QRLogger::log(cat, LL_ERROR, "Unknown Java error in %s.", methodName);
  else {
    const char *char_result = jenv_->GetStringUTFChars(jresult, 0);
    QRLogger::log(cat, LL_ERROR, "%s error: %s.", methodName, char_result);
    jenv_->ReleaseStringUTFChars(jresult, char_result);
    jenv_->DeleteLocalRef(jresult);
  }
}

void JavaObjectInterface::logError(std::string &cat, const char *file, int line, const char *methodName,
                                   const char *tableName, const char *error_msg) {
  if (tableName != NULL)
    QRLogger::log(cat, LL_ERROR, "Java exception in File:%s, Line:%d Method:%s TableName:%s %s", file, line, methodName,
                  tableName, error_msg);
  else
    QRLogger::log(cat, LL_ERROR, "Java exception in File:%s, Line:%d Method:%s TableName:NA %s", file, line, methodName,
                  error_msg);
}

NABoolean JavaObjectInterface::getExceptionDetails(const char *fileName, int lineNo, const char *methodName,
                                                   const char *tableName, NABoolean noDetails) {
  NABoolean killProcess = FALSE;
  CliGlobals *cliGlobals = GetCliGlobals();
  NAString error_msg;
  HBaseClient_JNI *hbaseClient;
  if (gThrowableClass == NULL) {
    jenv_->ExceptionDescribe();
    error_msg = "Internal Error - Unable to find Throwable class";
    setSqlJniErrorStr(error_msg);
    return FALSE;
  }
  jthrowable a_exception = jenv_->ExceptionOccurred();
  if (a_exception == NULL) {
    error_msg = "No java exception was thrown";
    setSqlJniErrorStr(error_msg);
    return FALSE;
  }
  jenv_->ExceptionClear();
  if (appendExceptionMessages(a_exception, error_msg, noDetails)) killProcess = TRUE;
  setSqlJniErrorStr(error_msg);
  logError(CAT_SQL_EXE, fileName, lineNo, methodName, tableName, error_msg);
  if (killProcess) {
    // wait to get the hprof dump by JVM
    sleep(30);
    abort();
  }
  size_t rsseIndex = error_msg.index("RegionServerStoppedException");
  size_t nsreIndex = error_msg.index("NotServingRegionException");
  size_t nrheIndex = error_msg.index("NoRouteToHostException");
  size_t mnreIndex = error_msg.index("MasterNotRunningException");
  size_t fseIndex = error_msg.index("FailedServerException");
  size_t cceIndex = error_msg.index("ConnectionClosingException");
  size_t rsaeIndex = error_msg.index("RegionServerAbortedException");
  size_t illArgIndex = error_msg.index("IllegalArgumentException");

  // only for IllegalArgumentException
  if ((illArgIndex != std::string::npos)) illArgIndex = error_msg.index("Connection is null or closed.");

  if ((rsseIndex != std::string::npos) || (nsreIndex != std::string::npos) || (nrheIndex != std::string::npos) ||
      (fseIndex != std::string::npos) || (cceIndex != std::string::npos) || (rsaeIndex != std::string::npos) ||
      (mnreIndex != std::string::npos) || (illArgIndex != std::string::npos)) {
    hbaseClient = cliGlobals->getHBaseClient();
    if (hbaseClient != NULL && hbaseClient->isInitialized()) hbaseClient->reconnectHBase();
  }
  return TRUE;
}

NABoolean JavaObjectInterface::appendExceptionMessages(jthrowable a_exception, NAString &error_msg,
                                                       NABoolean noDetails) {
  NABoolean killProcess = FALSE;
  HBaseClient_JNI *hbaseClient;
  NABoolean reconnect = FALSE;
  if (gOOMErrorClass != NULL && jenv_->IsInstanceOf(a_exception, gOOMErrorClass) == JNI_TRUE) killProcess = TRUE;
  jstring msg_obj = (jstring)jenv_->CallObjectMethod(a_exception, gThrowableToStringMethodID);
  const char *msg_str;
  if (msg_obj != NULL) {
    msg_str = jenv_->GetStringUTFChars(msg_obj, 0);
    error_msg += msg_str;
    jenv_->ReleaseStringUTFChars(msg_obj, msg_str);
    jenv_->DeleteLocalRef(msg_obj);
  } else
    msg_str = "Exception is thrown, but tostring is null";

  // Get the stack trace
  jobjectArray frames = (jobjectArray)jenv_->CallObjectMethod(a_exception, gGetStackTraceMethodID);
  if (frames == NULL) return killProcess;

  if (noDetails) return killProcess;

  jsize frames_length = jenv_->GetArrayLength(frames);

  jsize i = 0;
  for (i = 0; i < frames_length; i++) {
    jobject frame = jenv_->GetObjectArrayElement(frames, i);
    msg_obj = (jstring)jenv_->CallObjectMethod(frame, gStackFrameToStringMethodID);
    if (msg_obj != NULL) {
      msg_str = jenv_->GetStringUTFChars(msg_obj, 0);
      error_msg += "\n";
      error_msg += msg_str;
      jenv_->ReleaseStringUTFChars(msg_obj, msg_str);
      jenv_->DeleteLocalRef(msg_obj);
      jenv_->DeleteLocalRef(frame);
    }
  }
  jthrowable j_cause = (jthrowable)jenv_->CallObjectMethod(a_exception, gGetCauseMethodID);
  if (j_cause != NULL) {
    error_msg += " Caused by \n";
    if (appendExceptionMessages(j_cause, error_msg, noDetails)) killProcess = TRUE;
  }
  jenv_->DeleteLocalRef(a_exception);
  return killProcess;
}

JOI_RetCode JavaObjectInterface::initJNIEnv() {
  JOI_RetCode retcode;
  if (jenv_ == NULL) {
    LmJavaOptions *options = new LmJavaOptions();
    initJavaOptionFromEnv(options);
    if ((retcode = initJVM(options)) != JOI_OK)
      if ((retcode = initJVM()) != JOI_OK) {
        delete options;
        return retcode;
      }
    delete options;
  }
  if (jenv_->PushLocalFrame(jniHandleCapacity_) != 0) {
    return JOI_ERROR_INIT_JNI;
  }
  return JOI_OK;
}
