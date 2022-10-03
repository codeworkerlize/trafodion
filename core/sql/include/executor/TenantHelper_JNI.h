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
#ifndef TENANT_HELPER_H
#define TENANT_HELPER_H

#include <list>
#include "common/Platform.h"
#include "common/Collections.h"
#include "export/NABasicObject.h"

#include "executor/JavaObjectInterface.h"
#include "exp/ExpHbaseDefs.h"
#include "common/NAMemory.h"
#include "MonarchClient_JNI.h"  // to get MC_LAST
#include "common/NAWNodeSet.h"


// forward declare

class ContextCli;



// ===========================================================================
// ===== The TenantHelper_JNI class implements access to the Java 
// ===== TenantHelper class.
// ===========================================================================

// Keep in sync with thErrorEnumStr array in TenantHelper_JNI.cpp.
typedef enum {
  TH_OK     = JOI_OK
 ,TH_FIRST  = MC_LAST
 ,TH_ERROR_REGISTER_TENANT_PARAM
 ,TH_ERROR_REGISTER_TENANT_EXCEPTION
 ,TH_ERROR_REGISTER_TENANT_NO_RESULT
 ,TH_ERROR_ALTER_TENANT_PARAM
 ,TH_ERROR_ALTER_TENANT_EXCEPTION
 ,TH_ERROR_ALTER_TENANT_NO_RESULT
 ,TH_ERROR_UNREGISTER_TENANT_PARAM
 ,TH_ERROR_UNREGISTER_TENANT_EXCEPTION
 ,TH_ERROR_CREATE_LOCAL_CGROUP_PARAM
 ,TH_ERROR_CREATE_LOCAL_CGROUP_EXCEPTION
 ,TH_LAST
} TH_RetCode;

// ---------------------------------------
// keep this class in sync with Java class
// com.esgyn.common.OversubscriptionInfo
// ---------------------------------------
struct OversubscriptionInfo
{
  int measures_[7];

  void init();
  static int getNumMeasures()                                    { return 7; }
  bool isOversubscribed()                         { return measures_[0] > 0; }
  float nodeOversubscriptionRatio()
                     { return  (float) measures_[6] / MAXOF(measures_[5],1); }
  void setFromJavaArray(JNIEnv *jenv,
                        jintArray &javaArray);
  bool addDiagnostics(ComDiagsArea *diags,
                      const char *tenantName,
                      float warningThreshold,
                      float errorThreshold);
};

class TenantHelper_JNI : public JavaObjectInterface
{
public:

  TenantHelper_JNI(NAHeap *heap, jobject jObj = NULL)
  :  JavaObjectInterface(heap, jObj)
  {
     heap_ = heap;
  }

  // Destructor
  virtual ~TenantHelper_JNI();

  char * getErrorText(TH_RetCode errEnum);

  static TenantHelper_JNI * getInstance();

  static void deleteInstance();
  
  TH_RetCode init();

  // assign nodes (if not already done) and add
  // zookeeper data for a new tenant to be created
  TH_RetCode registerTenant(const NAString & tenantName,
                            const NAWNodeSet &assignedNodes,
                            const NAString & sessionLimit,
                            const NAString & defaultSchema, 
                            NABoolean overrideWarning,
                            NAWNodeSet *&resultAssignedNodes,
                            OversubscriptionInfo &osi,
                            NAHeap *outHeap);

  // Assign new nodes (if not already done) and update the
  // zookeeper data for a tenant to be altered.
  TH_RetCode alterTenant(const NAString & tenantName,
                         const NAWNodeSet &oldAssignedNodes,
                         const NAWNodeSet &newAssignedNodes,
                         const NAString & sessionLimit,
                         const NAString & defaultSchema, 
                         NABoolean overrideWarning,
                         NAWNodeSet *&resultAssignedNodes,
                         OversubscriptionInfo &osi,
                         NAHeap *outHeap);

  // remove zookeeper data for a tenant, use this for
  // DROP TENANT or to undo a call to registerTenant()
  // or alterTenant() above when we encounter later errors
  TH_RetCode unregisterTenant(const NAString & tenantName);

  // create cgroup for a tenant on the local node, called
  // at runtime if a cgroup is missing
  TH_RetCode createLocalCGroup(const char *tenantName);

private:

  enum JAVA_METHODS {
    JM_CTOR = 0
   ,JM_REGISTER_TENANT
   ,JM_ALTER_TENANT 
   ,JM_UNREGISTER_TENANT  
   ,JM_CREATE_LOCAL_CGROUP
   ,JM_LAST
  };

  NAHeap * heap_;

  static jclass          javaClass_;  
  static JavaMethodInit* JavaMethods_;
  static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;
};


#endif


