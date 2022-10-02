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

// ===========================================================================
// ===== The HiveClient_JNI class implements access to the Java 
// ===== HiveClient class.
// ===========================================================================
#ifndef HIVE_CLIENT_H
#define HIVE_CLIENT_H

#include <list>
#include "JavaObjectInterface.h"


typedef enum {
  HVC_OK     = JOI_OK
 ,HVC_FIRST  = JOI_LAST
 ,HVC_DONE   = HVC_FIRST
 ,HVC_ERROR_INIT_PARAM
 ,HVC_ERROR_INIT_EXCEPTION
 ,HVC_ERROR_CLOSE_EXCEPTION
 ,HVC_ERROR_EXISTS_PARAM
 ,HVC_ERROR_EXISTS_EXCEPTION
 ,HVC_ERROR_GET_REDEFTIME_PARAM
 ,HVC_ERROR_GET_REDEFTIME_EXCEPTION
 ,HVC_ERROR_GET_ALLSCH_EXCEPTION
 ,HVC_ERROR_GET_ALLTBL_PARAM
 ,HVC_ERROR_GET_ALLTBL_EXCEPTION
 ,HVC_ERROR_GET_ALLTBLMETA_PARAM
 ,HVC_ERROR_GET_ALLTBLMETA_EXCEPTION
 ,HVC_ERROR_GET_ALLVIEW_PARAM
 ,HVC_ERROR_GET_ALLVIEW_EXCEPTION
 ,HVC_ERROR_EXECUTE_HIVE_SQL_PARAM
 ,HVC_ERROR_EXECUTE_HIVE_SQL_EXCEPTION
 ,HVC_ERROR_CREATEHIVEPART_PARAM
 ,HVC_ERROR_CREATEHIVEPART_EXCEPTION
 ,HVC_ERROR_SENTRY_GET_PARAM
 ,HVC_ERROR_SENTRY_GET_EXCEPTION
 ,HVC_ERROR_GET_HVT_INFO_PARAM
 ,HVC_ERROR_GET_HVT_INFO_EXCEPTION
 ,HVC_ERROR_GET_HIVE_TABLE_INFO_ERROR
 ,HVC_ERROR_POPULATE_SDS_ERROR
 ,HVC_ERROR_GET_STRIPE_OR_ROWGROUP_INFO_PARAM
 ,HVC_ERROR_GET_STRIPE_OR_ROWGROUP_INFO_EXCEPTION
 ,HVC_LAST
} HVC_RetCode;

class HiveClient_JNI : public JavaObjectInterface
{
public:
  static HiveClient_JNI* newInstance(NAHeap *heap, HVC_RetCode &retCode);
  static HiveClient_JNI* getInstance();
  static void deleteInstance();

  // Destructor
  virtual ~HiveClient_JNI();
  
  // Initialize JVM and all the JNI configuration.
  // Must be called.
  HVC_RetCode init();

  HVC_RetCode close();
  static HVC_RetCode exists(const char* schName, const char* tabName);
  HVC_RetCode getHiveTableInfo(const char* schName, const char* tabName, NABoolean readPartnInfo);
  static HVC_RetCode getRedefTime(const char* schName, const char* tabName, NABoolean allowSubir,
                           Int64& redefTime);
  static HVC_RetCode getAllSchemas(NAHeap *heap, LIST(Text *)& schNames);
  static HVC_RetCode getAllTables(NAHeap *heap, const char* schName, LIST(Text *)& tblNames);
  static HVC_RetCode getAllTablesMeta(NAHeap *heap, const char* schName, LIST(Text *)& tblNamesMeta);
  static HVC_RetCode getAllViews(NAHeap *heap, const char* schName, LIST(Text *)& viewNames);

  static HVC_RetCode executeHiveSQL(const char* hiveSQL);
  // Get the error description.
  static char* getErrorText(HVC_RetCode errEnum);
  
  static void logIt(const char* str);
  static int getHostIds(int numBlocks, short rf, jobjectArray j_blockHosts, HostId *blockHosts); 
  HVC_RetCode createHiveTablePartition(const char *schemaName,
                                       const char *tableName,
                                       const char *partitionString,
                                       bool useDistributedLock);

  static HVC_RetCode getSentryPrivileges(set<string> & groupNames,
                                  const char * tableName,
                                  map<Lng32,char *> & columnNumberToNameMap,
                                  vector<Lng32> & bitMaps /* out */,
                                  vector<Lng32> & positionToColumnNumberVector /* out */);
  static HVC_RetCode getSentryPrivileges(const char * userName,
                                  const char * tableName,
                                  map<Lng32,char *> & columnNumberToNameMap,
                                  vector<Lng32> & bitMaps /* out */,
                                  vector<Lng32> & positionToColumnNumberVector /* out */);
  void setTableInfo(jobjectArray tableInfo, jobjectArray colInfo, jobjectArray partKeyInfo, 
                jobjectArray bucketCols, jobjectArray sortCols, jintArray sortColsOrder, jobjectArray paramsKeys, jobjectArray paramsValue,
                jobjectArray partNames, jobjectArray partKeyValues);
  void cleanupTableInfo();
  HVC_RetCode getHiveTableDesc(NAHeap *heap, hive_tbl_desc *&hiveTableDesc);
  HVC_RetCode getStripeOrRowgroupInfo(HiveFileType type, const char *dir, int numPartkeys, int numFilesToSample, NABoolean readSubdirs,
                   Int64 &modificationTs, HHDFSTableStats *hhdfsTableStats);
  void setStripeOrRowgroupInfo(jint j_partitionNum, jstring j_fileName, jstring j_partDir, jstring j_partKeyValues, 
    jboolean j_isFile, jlong j_modificationTs, jlong j_fileSize, jshort j_replicationFactor, jlong j_blockSize, jlong j_accessTs, 
    jint j_numStripesOrRowgroups, jlongArray j_offsets, jlongArray j_lengths, jlongArray j_numRows, jint j_numBlocks, jobjectArray j_blockHosts);
  Int32 getNumFilesStatsRead() { return numFilesStatsRead_; }
private:   
  // Private Default constructor		
  HiveClient_JNI(NAHeap *heap)
  :  JavaObjectInterface(heap)
  , isConnected_(FALSE)
  {
     tableInfo_ = NULL;
     colInfo_ = NULL;
     partKeyInfo_ = NULL;
     bucketCols_ = NULL;
     sortCols_ = NULL;
     sortColsOrder_ = NULL;
     paramsKeys_ = NULL;
     paramsValues_ = NULL;
     partNames_ = NULL;
     partKeyValues_ = NULL;
     hhdfsTableStats_ = NULL;
     numFilesStatsRead_ = 0;
  }

private:  
  HVC_RetCode populateSD(NAHeap *heap, Int64 creationTs, hive_sd_desc* &sd);
  HVC_RetCode populateColumns(NAHeap *heap, hive_column_desc* &columns);
  HVC_RetCode populatePartKeyColumns(NAHeap *heap, hive_pkey_desc* &partKeyDesc);
  HVC_RetCode populateSortColumns(NAHeap *heap, hive_skey_desc* &sortKeyDesc);
  HVC_RetCode populateBucketColumns(NAHeap *heap, hive_bkey_desc* &bucketKeyDesc);
  HVC_RetCode populateTableParams(NAHeap *heap, hive_sd_desc *sd, hive_tblparams_desc* &tblparamsDesc);

  enum JAVA_METHODS {
    JM_CTOR = 0
   ,JM_CLOSE
   ,JM_EXISTS     
   ,JM_GET_RDT
   ,JM_GET_ASH
   ,JM_GET_ATL
   ,JM_GET_ATLM
   ,JM_GET_AVW
   ,JM_EXEC_HIVE_SQL
   ,JM_CREATE_HIVE_PART
   ,JM_GET_SENTRY_PRIVS
   ,JM_GET_SENTRY_PRIVS_U
   ,JM_GET_HVT_INFO
   ,JM_GET_STRIPE_OR_ROWGROUP_INFO
   ,JM_LAST
  };
  static jclass          javaClass_; 
  static JavaMethodInit* JavaMethods_;
  static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;
  bool isConnected_;
  jobjectArray tableInfo_;
  jobjectArray colInfo_;
  jobjectArray partKeyInfo_;
  jobjectArray bucketCols_;
  jobjectArray sortCols_;
  jintArray    sortColsOrder_;
  jobjectArray paramsKeys_;
  jobjectArray paramsValues_;
  jobjectArray partNames_;
  jobjectArray partKeyValues_;
  HHDFSTableStats *hhdfsTableStats_;
  Int32 numFilesStatsRead_;
};
#endif
