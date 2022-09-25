// @@@ START COPYRIGHT @@@
// //
// // (C) Copyright 2018 Esgyn Corporation
// //
// // @@@ END COPYRIGHT @@@
//
#ifndef TRAF_EXT_STORAGE_UTILS_H
#define TRAF_EXT_STORAGE_UTILS_H
#include <list>
#include "Platform.h"
#include "Collections.h"
#include "NABasicObject.h"
#include "NAStringDef.h"
#include "JavaObjectInterface.h"
#include "HBaseClient_JNI.h"
#include "NAMemory.h"
#include "ExStats.h"

class OrcFileVectorReader;
// ===========================================================================
// ===== The TrafExtSorageUtils class implements access to the Java 
// ===== TrafExtStorageUtils class.
// ===========================================================================

typedef enum {
  TSU_OK     = JOI_OK
 ,TSU_FIRST  = JOI_LAST
 ,TSU_ERROR_INITSTRAWSCAN_EXCEPTION  //Java exception in initStrawScan()
 ,TSU_ERROR_INITSTRAWSCAN_PARAM
 ,TSU_ERROR_GETNEXTRANGENUMSTRAWSCAN_EXCEPTION //Java exception in getNextRangeNumStrawScan()
 ,TSU_ERROR_GETNEXTRANGENUMSTRAWSCAN_PARAM
 ,TSU_ERROR_FREESTRAWSCAN_EXCEPTION //Java exception in freeStrawScan()"
 ,TSU_ERROR_FREESTRAWSCAN_PARAM
 ,TSU_LAST
} TSU_RetCode;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------

class TrafExtStorageUtils : public JavaObjectInterface
{
friend class OrcFileVectorReader;

public:
  // Default constructor - for creating a new JVM		
  TrafExtStorageUtils(NAHeap *heap, JavaMethodInit *javaMethods)
    :  JavaObjectInterface(heap) 
  {
     childJavaMethods_ = javaMethods;
  }

  TrafExtStorageUtils(NAHeap *heap, JavaVM *jvm, JNIEnv *jenv, JavaMethodInit *javaMethods)
    :  JavaObjectInterface(heap)
  {
     childJavaMethods_ = javaMethods;
  }

  virtual ~TrafExtStorageUtils()
  {}
 
  virtual JOI_RetCode init() = 0;  
  void initMethodNames(JavaMethodInit *javaMethods);
  TSU_RetCode initStrawScan(char* webServers, char* queryId, Lng32 explainNodeId, bool isFactTable, NAText* entries, CollIndex entriesLength);
  TSU_RetCode getNextRangeNumStrawScan(char* webServers,
                       char* queryId,
                       Lng32 explainNodeId,
                       Lng32 sequenceNb,
                       ULng32 executionCount,
                       Lng32 espNb,
                       Lng32 nodeId,
                       bool isFactTable,
                       Lng32& nextRangeNum);
  TSU_RetCode freeStrawScan(char* queryId, Lng32 explainNodeId );
  static char*  getErrorText(TSU_RetCode errEnum);
protected:
  enum JAVA_METHODS {
    JM_CTOR = 0,
    JM_INIT_STRAWSCAN,
    JM_GETNEXTRANGENUM_STRAWSCAN,
    JM_FREE_STRAWSCAN,
    JM_LAST,
    JM_LAST_TRAF_EXT_STORAGE_UTILS = JM_LAST
  };
 
private:
   JavaMethodInit *childJavaMethods_;
};

#endif
