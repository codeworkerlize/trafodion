#ifndef DISTRIBUTED_CLIENT_H
#define DISTRIBUTED_CLIENT_H

#include "executor/JavaObjectInterface.h"

typedef enum {
  DL_OK = JOI_OK,
  DL_FIRST = JOI_LAST,
  DL_ERROR_INIT,
  DL_ERROR_INIT_JNI,
  DL_ERROR_INIT_JNI_ENV,
  DL_ERROR_LOCK,
  DL_ERROR_LONGHELDLOCK,
  DL_ERROR_UNLOCK,
  DL_ERROR_CLEARLOCK,
  DL_ERROR_PARAM,
  DL_ERROR_CLOSE,
  DL_ERROR_OBSERVE,
  DL_ERROR_LISTNODES,
  DL_LAST
} DL_RetCode;

class DistributedLock_JNI : public JavaObjectInterface {
 public:
  static DistributedLock_JNI *newInstance(NAHeap *heap, DL_RetCode &retCode);

  // get instance stored in currContext(), if NULL, then create it
  static DistributedLock_JNI *getInstance();
  static void deleteInstance();

  char *getErrorText(DL_RetCode errEnum);
  DL_RetCode init();
  DL_RetCode lock(const char *lockName, long timeout = 0);
  DL_RetCode longHeldLock(const char *lockName, const char *data, char *returnedData /* out */,
                          size_t returnedDataMaxLength);
  DL_RetCode unlock();
  DL_RetCode clearlock();
  DL_RetCode observe(const char *lockName, bool &locked);
  DL_RetCode listNodes(const char *lockName);

 private:
  DistributedLock_JNI(NAHeap *heap) : JavaObjectInterface(heap) {}

  enum JAVA_METHODS {
    JM_CTOR = 0,
    JM_LOCK,
    JM_LONGHELDLOCK,
    JM_UNLOCK,
    JM_CLEARLOCK,
    JM_OBSERVE,
    JM_LISTNODES,
    JM_LAST
  };

  static jclass javaClass_;
  static JavaMethodInit *JavaMethods_;
  static bool javaMethodsInitialized_;
  static pthread_mutex_t javaMethodsInitMutex_;
};

#endif
