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

#include "Context.h"
#include "Globals.h"
#include "OrcFileVectorReader.h"
#include "QRLogger.h"
#include "HBaseClient_JNI.h"
#include "ttime.h"
#include "ComCextdecs.h"
#include "org_trafodion_sql_OrcFileVectorReader.h"
#include "DatetimeType.h"

// ===========================================================================
// ===== Class OrcFileVectorReader
// ===========================================================================

JavaMethodInit* OrcFileVectorReader::JavaMethods_ = NULL;
jclass OrcFileVectorReader::javaClass_ = 0;
bool OrcFileVectorReader::javaMethodsInitialized_ = false;
pthread_mutex_t OrcFileVectorReader::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

// array is indexed by enum OFVR_RetCode
static const char* const ofvrErrorEnumStr[] = 
{
  "No more data."
 ,"JNI NewStringUTF() in open()"
 ,"Java exception in open()"
 ,"Java exception in nextBatch()"
 ,"Java exception in close()"
 ,"Incompatible conversion"
 , "Internal Error: column vector is NULL"
 , "Internal Error: Error while copying column vector data"
 , "Internal Error: column vector EXT1 is NULL"
 , "Internal Error: column vector EXT2 is NULL"
 , "Internal Error: column null value is NULL"
 , "Internal Error: BIGNUM column internal buffer overflow"
};


//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
char* OrcFileVectorReader::getErrorText(OFVR_RetCode pv_errEnum)
{
  if (pv_errEnum < (OFVR_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)pv_errEnum);
  else    
    return (char*)ofvrErrorEnumStr[pv_errEnum - JOI_LAST];
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OrcFileVectorReader::~OrcFileVectorReader()
{
  close();
  if (path_ != NULL) {
     NADELETEBASIC(path_, getHeap());
     path_ = NULL;
  }
  
}
 
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
JOI_RetCode OrcFileVectorReader::init()
{
  static char className[]="org/trafodion/sql/OrcFileVectorReader";

  JOI_RetCode lv_retcode = JOI_OK;
 
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"Enter OrcFileVectorReader::init()");

  if (isInitialized())
    return lv_retcode;

  if (javaMethodsInitialized_)
    return JavaObjectInterface::init(className, 
							javaClass_, 
							JavaMethods_, 
                                                        (Int32)JM_LAST, javaMethodsInitialized_);
  else 
  {
    pthread_mutex_lock(&javaMethodsInitMutex_);
    if (javaMethodsInitialized_)
    {
      pthread_mutex_unlock(&javaMethodsInitMutex_);
      return JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    }
    JavaMethods_ = new JavaMethodInit[JM_LAST];
    TrafExtStorageUtils::initMethodNames(JavaMethods_); 
    JavaMethods_[JM_OPEN      ].jm_name      = "open";
    JavaMethods_[JM_OPEN      ].jm_signature = "(JLjava/lang/String;JJII[I[Ljava/lang/Object;[Ljava/lang/Object;)J";
    JavaMethods_[JM_CLOSE     ].jm_name      = "close";
    JavaMethods_[JM_CLOSE     ].jm_signature = "()V";
    JavaMethods_[JM_FETCH_NEXT_BATCH].jm_name      = "fetchNextBatch";
    JavaMethods_[JM_FETCH_NEXT_BATCH].jm_signature = "()Z";

    lv_retcode = JavaObjectInterface::init(className,
							javaClass_,
							JavaMethods_,
							(Int32)JM_LAST, javaMethodsInitialized_);
    if (lv_retcode == JOI_OK)
       javaMethodsInitialized_ = TRUE;
    pthread_mutex_unlock(&javaMethodsInitMutex_);
  }
  return lv_retcode;
}

	
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFVR_RetCode OrcFileVectorReader::open(ExHdfsScanStats *hdfsStats, const char * tableName,
                                const char *pv_path,
                                const int vector_size,
                                Int64 offset,  
                                Int64 length,  
				int   pv_num_cols_in_projection,
				int  *pv_which_cols,
                                TextVec *ppiVec,
                                TextVec *ppiAllCols,
                                TextVec *colNameVec,
                                TextVec *colTypeVec)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileVectorReader::open(start_time=%s, %s, %d, %p) called.",
		reportTimestamp(),
		pv_path,
		pv_num_cols_in_projection,
		pv_which_cols);

  OFVR_RetCode lv_retcode = OFVR_OK;
  if (path_ != NULL)
     NADELETEBASIC(path_, getHeap());
  int len = strlen(pv_path);
  path_ = new (getHeap()) char[len+2];
  strcpy(path_, pv_path);
  jlong     jl_jniObject = (long)this;
  jstring   js_path = NULL;
  jintArray jia_which_cols = NULL;
  jint      ji_num_cols_in_projection = pv_num_cols_in_projection;
  jlong     jl_offset = offset;
  jlong     jl_length = length;
  jobject   jo_ppiBuf = NULL;
  jlong     jresult;
  jobjectArray jor_ppiVec = NULL;
  jobjectArray jor_ppiAllCols = NULL;
  jint      ji_vector_size = vector_size;
  
  if (initJNIEnv() != JOI_OK)
     return OFVR_ERROR_OPEN_PARAM;
  hdfsStats_ = hdfsStats;
  cleanupResultInfo();
  js_path = jenv_->NewStringUTF(pv_path);
  if (js_path == NULL) {
    lv_retcode = OFVR_ERROR_OPEN_PARAM;
    goto fn_exit;
  }
  
  if ((pv_num_cols_in_projection > 0) && 
      (pv_which_cols != 0)) {
    jia_which_cols = jenv_->NewIntArray(pv_num_cols_in_projection);
    if (jia_which_cols == NULL) {

      QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		    LL_ERROR,
		    "OrcFileVectorReader::open(%s, %d, %p). Error while allocating memory for j_col_array",
		    pv_path,
		    pv_num_cols_in_projection,
		    pv_which_cols);

      
      lv_retcode = OFVR_ERROR_OPEN_PARAM;
      goto fn_exit;
    }
    
    jenv_->SetIntArrayRegion(jia_which_cols,
			     0,
			     pv_num_cols_in_projection,
			     pv_which_cols);
  }

  if (ppiVec && (!ppiVec->empty()))
    {
      jor_ppiVec = convertToByteArrayObjectArray(*ppiVec);
      if (jor_ppiVec == NULL) {
        getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorReader::open()");
        lv_retcode = OFVR_ERROR_OPEN_PARAM;
	goto fn_exit;
      }

      if (ppiAllCols && (!ppiAllCols->empty()))
        {
          QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  Adding %d cols.", 
                        ppiAllCols->size());
          jor_ppiAllCols = convertToStringObjectArray(*ppiAllCols);
          if (jor_ppiAllCols == NULL)
            {
              getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorReader::open()");
              lv_retcode = OFVR_ERROR_OPEN_PARAM;
	      goto fn_exit;
            }
        }
    }
  if (hdfsStats_ != NULL)
     hdfsStats_->getHdfsTimer().start();
  tsRecentJMFromJNI = JavaMethods_[JM_OPEN].jm_full_name;
  jresult = jenv_->CallLongMethod(javaObj_, JavaMethods_[JM_OPEN].methodID,
                                             jl_jniObject,
					     js_path,
					     jl_offset,
					     jl_length,
                                             ji_vector_size,
					     ji_num_cols_in_projection,
					     jia_which_cols,
                                             jor_ppiVec,
                                             jor_ppiAllCols);
  if (hdfsStats_ != NULL) {
     hdfsStats_->incMaxHdfsIOTime(hdfsStats_->getHdfsTimer().stop());
     hdfsStats_->incHdfsCalls();
  }

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorReader::open()");
    lv_retcode = OFVR_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }
  isEOR_ = FALSE;
fn_exit:  
  qualifyingRowCount_ = jresult;
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}


OFVR_RetCode OrcFileVectorReader::fetchNextBatch() 
{

   if (initJNIEnv() != JOI_OK)
      return OFVR_ERROR_FETCH_NEXTBATCH_EXCEPTION;

   tsRecentJMFromJNI = JavaMethods_[JM_FETCH_NEXT_BATCH].jm_full_name;
   if (hdfsStats_ != NULL)
      hdfsStats_->getHdfsTimer().start();
   if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorReader::fetchNextRow()");
      jenv_->PopLocalFrame(NULL);
      return OFVR_ERROR_FETCH_NEXTBATCH_EXCEPTION;
   }
   jboolean jresult = jenv_->CallBooleanMethod(javaObj_,
                      JavaMethods_[JM_FETCH_NEXT_BATCH].methodID);
   if (hdfsStats_ != NULL) {
      hdfsStats_->incMaxHdfsIOTime(hdfsStats_->getHdfsTimer().stop());
      hdfsStats_->incHdfsCalls();
   }
   if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorReader::fetchNextRow()");
      jenv_->PopLocalFrame(NULL);
      return OFVR_ERROR_FETCH_NEXTBATCH_EXCEPTION;
   }
   if (jresult) 
      isEOR_ = TRUE;
   else
      isEOR_ = FALSE;
   jenv_->PopLocalFrame(NULL);
   return (OFVR_OK);
}

OFVR_RetCode OrcFileVectorReader::close()
{
   QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileVectorReader::close()");
   if (initJNIEnv() != JOI_OK)
      return OFVR_ERROR_CLOSE_EXCEPTION;
    
   if (hdfsStats_ != NULL)
      hdfsStats_->getHdfsTimer().start();
   // String close();
   tsRecentJMFromJNI = JavaMethods_[JM_CLOSE].jm_full_name;
   jenv_->CallVoidMethod(javaObj_, JavaMethods_[JM_CLOSE].methodID);
   if (hdfsStats_ != NULL) {
      hdfsStats_->incMaxHdfsIOTime(hdfsStats_->getHdfsTimer().stop());
      hdfsStats_->incHdfsCalls();
   }

   if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorReader::close()");
      jenv_->PopLocalFrame(NULL);
      return OFVR_ERROR_CLOSE_EXCEPTION;
   }
   jenv_->PopLocalFrame(NULL);
   cleanupResultInfo();
   return OFVR_OK;
}

#ifdef __cplusplus
extern "C" {
#endif
/*
 ** Class:     org_trafodion_sql_OrcFileVectorReader
 ** Method:    setResultInfo
 ** Signature: (JZI[I[II[Ljava/lang/Object;[Ljava/lang/Object;[Ljava/lang/Object;[Ljava/lang/Object;[Z)V
 **/

JNIEXPORT void JNICALL Java_org_trafodion_sql_OrcFileVectorReader_setResultInfo
  (JNIEnv *jenv, jobject jobj, jlong jniObject, jboolean useUTCTimestamp, jint colCount, jintArray colIds, jintArray colTypeOrdinal, 
                    jint vectorSize, jobjectArray colValArrays, jobjectArray colValExtArrays1, jobjectArray colValExtArrays2,
                    jbooleanArray colNoNulls, jobjectArray nullValArrays, jbooleanArray isRepeating)
{
    OrcFileVectorReader *ofvr = (OrcFileVectorReader *)jniObject; 
    ofvr->setResultInfo(jenv, jobj, useUTCTimestamp, colCount, colIds, colTypeOrdinal, vectorSize, colValArrays, colValExtArrays1, 
                       colValExtArrays2, colNoNulls, nullValArrays, isRepeating);
}
#ifdef __cplusplus
}
#endif

void OrcFileVectorReader::setResultInfo
  (JNIEnv *jenv, jobject jobj, jboolean useUTCTimestamp, jint colCount, jintArray colIds, jintArray colTypeOrdinals, 
                    jint vectorSize, jobjectArray colValArrays, jobjectArray colValExtArrays1, jobjectArray colValExtArrays2, 
                    jbooleanArray colNoNulls, jobjectArray nullValArrays, jbooleanArray isRepeating)
{
    cleanupResultInfo();
    NABoolean exceptionFound = FALSE;
    currRowNum_ = -1;
    useUTCTimestamp_ = useUTCTimestamp;
    colCount_ = colCount;
    vectorSize_ = vectorSize;
    if (vectorSize > 0) {
       if (colCount_ == 0)
          return;
       jColIds_ = (jintArray)jenv->NewGlobalRef(colIds);
       if (jenv->ExceptionCheck())
          exceptionFound = TRUE;
       if (! exceptionFound) {
          jColTypeOrdinals_ = (jintArray)jenv->NewGlobalRef(colTypeOrdinals);
          if (jenv->ExceptionCheck())
             exceptionFound = TRUE;
       }
       if (! exceptionFound) {
          jColNoNulls_ = (jbooleanArray)jenv->NewGlobalRef(colNoNulls);
          if (jenv->ExceptionCheck())
             exceptionFound = TRUE;
       }
       if (! exceptionFound) {
          jIsRepeatable_ = (jbooleanArray)jenv->NewGlobalRef(isRepeating);
          if (jenv->ExceptionCheck())
             exceptionFound = TRUE;
       }
       if (! exceptionFound) {
          jColValArrays_ = (jobjectArray)jenv->NewGlobalRef(colValArrays);
          if (jenv->ExceptionCheck())
             exceptionFound = TRUE;
       }
       if (! exceptionFound) {
          jColValExtArrays1_ = (jobjectArray)jenv->NewGlobalRef(colValExtArrays1);
          if (jenv->ExceptionCheck())
              exceptionFound = TRUE;
       }
       if (! exceptionFound) {
          jColValExtArrays2_ = (jobjectArray)jenv->NewGlobalRef(colValExtArrays2);
          if (jenv->ExceptionCheck())
             exceptionFound = TRUE;
       }
       if (! exceptionFound) {
          jColNullValArrays_ = (jobjectArray)jenv->NewGlobalRef(nullValArrays);
          if (jenv->ExceptionCheck())
             exceptionFound = TRUE;
       }
       if (! exceptionFound) {
          p_colIds_ = jenv->GetIntArrayElements(colIds, NULL);
          p_colTypeOrdinals_ = jenv->GetIntArrayElements(colTypeOrdinals, NULL);
          p_colNoNulls_ = jenv->GetBooleanArrayElements(jColNoNulls_, NULL);
          p_isRepeating_ = jenv->GetBooleanArrayElements(jIsRepeatable_, NULL);
          jColVector_ = (jlongArray *) new (getHeap()) BYTE[sizeof(jlongArray*) * colCount_];
          jColVectorExt1_ = (jintArray *) new (getHeap()) BYTE[sizeof(jintArray*) * colCount_];
          jColVectorExt2_ = (jintArray *) new (getHeap()) BYTE[sizeof(jintArray*) * colCount_];
          jColNullVals_ = (jbooleanArray *) new (getHeap()) BYTE[sizeof(jbooleanArray*) * colCount_];
          p_colValArrays_ = (jlong **)new (getHeap()) BYTE[sizeof(jlong*) * colCount_];
          p_colValExtArrays1_ = (jint **)new (getHeap()) BYTE[sizeof(jint*) * colCount_];
          p_colValExtArrays2_ = (jint **)new (getHeap()) BYTE[sizeof(jint *) * colCount_];
          p_colNullVals_ = (jboolean **)new (getHeap()) BYTE[sizeof(jboolean *) * colCount_];
          for (int colNo = 0; colNo < colCount_; colNo++) {
              jColVector_[colNo] = NULL;
              jColVectorExt1_[colNo] = NULL;
              jColVectorExt2_[colNo] = NULL;
              p_colValArrays_[colNo] = NULL;
              p_colValExtArrays1_[colNo] = NULL;
              p_colValExtArrays2_[colNo] = NULL;
              p_colNullVals_[colNo] = NULL;

              if (! p_colNoNulls_[colNo]) {
                 jbooleanArray jColNullVals = (jbooleanArray)jenv_->GetObjectArrayElement(jColNullValArrays_, colNo);
                 if (jColNullVals == NULL)
                    ex_assert(false, "Exception in OrcFileVectorReader::setResultInfo");
                    //return OFVR_ERROR_COL_NULL_VAL_NULL;   
                 jColNullVals_[colNo] = (jbooleanArray)jenv_->NewGlobalRef(jColNullVals);
                 p_colNullVals_[colNo] = jenv_->GetBooleanArrayElements(jColNullVals_[colNo], NULL);
              }
              int hiveColType = p_colTypeOrdinals_[colNo];
              switch (hiveColType) {
                case org_trafodion_sql_OrcFileVectorReader_HIVE_CHAR_TYPE:
                case org_trafodion_sql_OrcFileVectorReader_HIVE_VARCHAR_TYPE:
                case org_trafodion_sql_OrcFileVectorReader_HIVE_STRING_TYPE:
                case org_trafodion_sql_OrcFileVectorReader_HIVE_BINARY_TYPE:
                 {
                   jbyteArray jColVector = (jbyteArray)jenv_->GetObjectArrayElement(jColValArrays_, colNo);
                   if (jColVector == NULL)
                      ex_assert(false, "Exception in OrcFileVectorReader::setResultInfo");
                      //return OFVR_ERROR_COL_VECTOR_NULL;   
                   jColVector_[colNo] = (jlongArray)jenv_->NewGlobalRef(jColVector);
                   jenv_->DeleteLocalRef(jColVector);
                   jintArray startOffsets = (jintArray)jenv_->GetObjectArrayElement(jColValExtArrays1_, colNo);
                   if (startOffsets == NULL)
                      ex_assert(false, "Exception in OrcFileVectorReader::setResultInfo");
                      //return OFVR_ERROR_COL_VECTOR_EXT1_NULL;
                   jColVectorExt1_[colNo] = (jintArray)jenv_->NewGlobalRef(startOffsets);
                   jenv_->DeleteLocalRef(startOffsets);
                   p_colValExtArrays1_[colNo] = jenv_->GetIntArrayElements(jColVectorExt1_[colNo], NULL);
                   jintArray lengths = (jintArray)jenv_->GetObjectArrayElement(jColValExtArrays2_, colNo);
                   if (lengths == NULL)
                      ex_assert(false, "Exception in OrcFileVectorReader::setResultInfo");
                      //return OFVR_ERROR_COL_VECTOR_EXT2_NULL;   
                   jColVectorExt2_[colNo] = (jintArray)jenv_->NewGlobalRef(lengths);
                   jenv_->DeleteLocalRef(lengths);
                   p_colValExtArrays2_[colNo] = jenv_->GetIntArrayElements(jColVectorExt2_[colNo], NULL);
                   break;
                 }
                case org_trafodion_sql_OrcFileVectorReader_HIVE_SHORT_TYPE:
                case org_trafodion_sql_OrcFileVectorReader_HIVE_INT_TYPE:
                case org_trafodion_sql_OrcFileVectorReader_HIVE_LONG_TYPE:
                case org_trafodion_sql_OrcFileVectorReader_HIVE_BYTE_TYPE:
                case org_trafodion_sql_OrcFileVectorReader_HIVE_BOOLEAN_TYPE:
                case org_trafodion_sql_OrcFileVectorReader_HIVE_DATE_TYPE:
                case org_trafodion_sql_OrcFileVectorReader_HIVE_TIMESTAMP_TYPE: 
                 {
                   jlongArray jColVector = (jlongArray)jenv_->GetObjectArrayElement(jColValArrays_, colNo);
                   if (jColVector == NULL)
                      ex_assert(false, "Exception in OrcFileVectorReader::setResultInfo");
                      //return OFVR_ERROR_COL_VECTOR_NULL;
                   jColVector_[colNo] = (jlongArray)jenv_->NewGlobalRef(jColVector);
                   jenv_->DeleteLocalRef(jColVector);
                   p_colValArrays_[colNo] = jenv_->GetLongArrayElements(jColVector_[colNo], NULL);
                   if (hiveColType == org_trafodion_sql_OrcFileVectorReader_HIVE_TIMESTAMP_TYPE) {
                      jintArray nanos = (jintArray)jenv_->GetObjectArrayElement(jColValExtArrays1_, colNo);
                      if (nanos == NULL)
                         ex_assert(false, "Exception in OrcFileVectorReader::setResultInfo");
                         //return OFVR_ERROR_COL_VECTOR_EXT1_NULL;   
                      jColVectorExt1_[colNo] = (jintArray)jenv_->NewGlobalRef(nanos);
                      jenv_->DeleteLocalRef(nanos);
                      p_colValExtArrays1_[colNo] = jenv_->GetIntArrayElements(jColVectorExt1_[colNo], NULL);
                   }
                   break;
                 }
                case org_trafodion_sql_OrcFileVectorReader_HIVE_DOUBLE_TYPE:
                case org_trafodion_sql_OrcFileVectorReader_HIVE_FLOAT_TYPE:
                 {
                   jdoubleArray jColVector = (jdoubleArray)jenv_->GetObjectArrayElement(jColValArrays_, colNo);
                   if (jColVector == NULL)
                      ex_assert(false, "Exception in OrcFileVectorReader::setResultInfo");
                      //return OFVR_ERROR_COL_VECTOR_NULL;   
                   jColVector_[colNo] = (jlongArray)jenv_->NewGlobalRef(jColVector);
                   jenv_->DeleteLocalRef(jColVector);
                   p_colValArrays_[colNo] = (jlong *)jenv_->GetDoubleArrayElements((jdoubleArray)jColVector_[colNo], NULL);
                   break;
                 }
               case org_trafodion_sql_OrcFileVectorReader_HIVE_DECIMAL_TYPE:
                 {
                    jintArray precisions = (jintArray)jenv_->GetObjectArrayElement(jColValExtArrays1_, colNo);
                    if (precisions == NULL)
                       ex_assert(false, "Exception in OrcFileVectorReader::setResultInfo");
                       //return OFVR_ERROR_COL_VECTOR_EXT1_NULL;   
                    jColVectorExt1_[colNo] = (jintArray)jenv_->NewGlobalRef(precisions);
                    jenv_->DeleteLocalRef(precisions);
                    p_colValExtArrays1_[colNo] = jenv_->GetIntArrayElements(jColVectorExt1_[colNo], NULL);
                    // precsion is an integer array with 1 element. All rows have same precison value
                    jint precision = p_colValExtArrays1_[colNo][0];
                    if (precision < 19) {
                       jlongArray jColVector = (jlongArray)jenv_->GetObjectArrayElement(jColValArrays_, colNo);
                       if (jColVector == NULL)
                          ex_assert(false, "Exception in OrcFileVectorReader::setResultInfo");
                          //return OFVR_ERROR_COL_VECTOR_NULL;
                       jColVector_[colNo] = (jlongArray)jenv_->NewGlobalRef(jColVector);
                       jenv_->DeleteLocalRef(jColVector);
                       p_colValArrays_[colNo] = jenv_->GetLongArrayElements(jColVector_[colNo], NULL);
                    }
                    else {
                      jobjectArray jColVector = (jobjectArray)jenv_->GetObjectArrayElement(jColValArrays_, colNo);
                      if (jColVector == NULL)
                         ex_assert(false, "Exception in OrcFileVectorReader::setResultInfo");
                         //return OFVR_ERROR_COL_VECTOR_NULL;   
                      jColVector_[colNo] = (jlongArray)jenv_->NewGlobalRef(jColVector);
                      jenv_->DeleteLocalRef(jColVector);
                    }
                 }
                 break;
                default:
                   ex_assert(false, "Hive data type not yet supported");
              }
          }
      } 
    }
    cleanupDone_ = FALSE;
    ex_assert(! exceptionFound, "Exception in OrcFileVectorReader::setResultInfo");
    return;
}

void OrcFileVectorReader::cleanupResultInfo()
{
   if (cleanupDone_)
      return;
   if (p_colTypeOrdinals_ != NULL) {
      jenv_->ReleaseIntArrayElements(jColTypeOrdinals_, p_colTypeOrdinals_, JNI_ABORT);
      jenv_->DeleteGlobalRef(jColTypeOrdinals_);
      p_colTypeOrdinals_ = NULL;
      jColTypeOrdinals_ = NULL;
   }
   if (p_colIds_ != NULL) {
      jenv_->ReleaseIntArrayElements(jColIds_, p_colIds_, JNI_ABORT);
      jenv_->DeleteGlobalRef(jColIds_);
      p_colIds_ = NULL;
      jColIds_ = NULL;
   }
   if (p_colNoNulls_ != NULL) {
      jenv_->ReleaseBooleanArrayElements(jColNoNulls_, p_colNoNulls_, JNI_ABORT);
      jenv_->DeleteGlobalRef(jColNoNulls_);
      p_colNoNulls_ = NULL;
      jColNoNulls_ = NULL;
   }
   if (p_isRepeating_ != NULL) {
      jenv_->ReleaseBooleanArrayElements(jIsRepeatable_, p_isRepeating_, JNI_ABORT);
      jenv_->DeleteGlobalRef(jIsRepeatable_);
      p_isRepeating_ = NULL;
      jIsRepeatable_ = NULL;
   }
   if (p_colValArrays_ != NULL) {
      for (int colNo = 0; colNo < colCount_;  colNo++) {
          if (p_colValArrays_[colNo] != NULL) {
             jenv_->ReleaseLongArrayElements(jColVector_[colNo], (jlong *)p_colValArrays_[colNo], JNI_ABORT);
             jenv_->DeleteGlobalRef(jColVector_[colNo]);
          }  
          else if (jColVector_[colNo] != NULL) 
             jenv_->DeleteGlobalRef(jColVector_[colNo]);
          jColVector_[colNo] = NULL;
      }
      NADELETEBASIC(p_colValArrays_, getHeap());
      NADELETEBASIC(jColVector_, getHeap());
      p_colValArrays_ = NULL;
      jColVector_ = NULL;
   }
   if (p_colValExtArrays1_ != NULL) {
      for (int colNo = 0; colNo < colCount_; colNo++) {
          if (p_colValExtArrays1_[colNo] != NULL) {
             jenv_->ReleaseIntArrayElements(jColVectorExt1_[colNo], (jint *)p_colValExtArrays1_[colNo], JNI_ABORT);
             jenv_->DeleteGlobalRef(jColVectorExt1_[colNo]);
          }  
      }
      NADELETEBASIC(p_colValExtArrays1_, getHeap());
      NADELETEBASIC(jColVectorExt1_, getHeap());
      p_colValExtArrays1_ = NULL;
      jColVectorExt1_ = NULL;
   }
   if (p_colValExtArrays2_ != NULL) {
      for (int colNo = 0; colNo < colCount_; colNo++) {
          if (p_colValExtArrays2_[colNo] != NULL) {
             jenv_->ReleaseIntArrayElements(jColVectorExt2_[colNo], (jint *)p_colValExtArrays2_[colNo], JNI_ABORT);
             jenv_->DeleteGlobalRef(jColVectorExt2_[colNo]);
          }  
      } 
      NADELETEBASIC(p_colValExtArrays2_, getHeap());
      NADELETEBASIC(jColVectorExt2_, getHeap());
      p_colValExtArrays2_ = NULL;
      jColVectorExt2_ = NULL;
   }
   if (p_colNullVals_ != NULL) {
      for (int colNo = 0; colNo < colCount_;  colNo++) {
          if (p_colNullVals_[colNo] != NULL) {
             jenv_->ReleaseBooleanArrayElements(jColNullVals_[colNo], (jboolean *)p_colNullVals_[colNo], JNI_ABORT);
             jenv_->DeleteGlobalRef(jColNullVals_[colNo]);
          }  
      }
      NADELETEBASIC(p_colValArrays_, getHeap());
      NADELETEBASIC(jColNullVals_, getHeap());
      p_colNullVals_ = NULL;
      jColNullVals_ = NULL;
   }
   if (jColValArrays_ != NULL) {
      jenv_->DeleteGlobalRef(jColValArrays_);
      jColValArrays_ = NULL;
   }
   if (jColValExtArrays1_ != NULL) {
      jenv_->DeleteGlobalRef(jColValExtArrays1_);
      jColValExtArrays1_ = NULL;
   }
   if (jColValExtArrays2_ != NULL) {
      jenv_->DeleteGlobalRef(jColValExtArrays2_);
      jColValExtArrays2_ = NULL;
   }
   if (jColNullValArrays_ != NULL) {
      jenv_->DeleteGlobalRef(jColNullValArrays_);
      jColNullValArrays_ = NULL;
   }
   isEOR_ = FALSE;
   currRowNum_ = -1;
   vectorSize_ = 0;
   cleanupDone_ = TRUE;
}

OFVR_RetCode OrcFileVectorReader::nextRow()
{
   OFVR_RetCode ofvrRetcode;
   if (isEOR_) {
      if (vectorSize_ <= 0)
         return OFVR_NOMORE;
      else if (vectorSize_ == (currRowNum_+1)) 
         return OFVR_NOMORE;
   }
   if (vectorSize_ > 0 && vectorSize_ > currRowNum_+1) {
      currRowNum_++;
      if (hdfsStats_ != NULL)
         hdfsStats_->incAccessedRows();
      return OFVR_OK;
   }
   ofvrRetcode = fetchNextBatch();
   if (ofvrRetcode != OFVR_OK) {
      cleanupResultInfo();
      return ofvrRetcode;
   }
   return nextRow();
}

OFVR_RetCode OrcFileVectorReader::getNullValue(int colNo, short &nullVal)
{
   jboolean isNull;
   if (p_colNoNulls_[colNo]) {
      nullVal = 0;
      return OFVR_OK;
   } 
   isNull = p_colNullVals_[colNo][currRowNum_];
   if (isNull)
      nullVal = -1;
   else
      nullVal = 0;
   return OFVR_OK; 
}

OFVR_RetCode OrcFileVectorReader::getBytesCol(int colNo, BYTE *colVal,
                                        Lng32 &colValLen, short &nullVal)
{
   if (colNo >= colCount_) {  // if this column is missing from the ORC file
      nullVal = -1;   // then return a null value for it
      colValLen = -1;
      return OFVR_OK;
   }
   OFVR_RetCode rc;
   int hiveColType = p_colTypeOrdinals_[colNo];
   if (hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_CHAR_TYPE
         && hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_VARCHAR_TYPE
         && hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_STRING_TYPE
         && hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_BINARY_TYPE)
      return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
   if ((rc = getNullValue(colNo, nullVal)) != OFVR_OK)
      return rc;
   if (nullVal == -1) {
      colValLen = -1;
      return OFVR_OK;
   }
   int rowNo = (p_isRepeating_[colNo] ? 0 : currRowNum_);
   jbyteArray jColBytes = (jbyteArray)jenv_->GetObjectArrayElement((jobjectArray)jColVector_[colNo], rowNo); 
   if (jenv_->ExceptionCheck()) {
       getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorReader::open()");
       return OFVR_ERROR_COL_VECTOR_NULL;
   }
   int startOffset = p_colValExtArrays1_[colNo][rowNo];
   int length = p_colValExtArrays2_[colNo][rowNo];
   if (colValLen >= length) 
      colValLen = length;
   if (colValLen > 0) {
      jenv_->GetByteArrayRegion(jColBytes, startOffset, colValLen, (jbyte *)colVal);
      if (jenv_->ExceptionCheck()) {
         getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorReader::open()");
         return OFVR_ERROR_COL_VECTOR_DATA_COPY_ERROR;
      }
      if (hdfsStats_ != NULL)
          hdfsStats_->incBytesRead(colValLen);
   }
   jenv_->DeleteLocalRef(jColBytes);
   return OFVR_OK;
}

OFVR_RetCode OrcFileVectorReader::getNumericCol(int colNo, Int16 dataType, BYTE *colVal,
                                        Lng32 &colValLen, short &nullVal)
{
   if (colNo >= colCount_) {  // if this column is missing from the ORC file
      nullVal = -1;   // then return a null value for it
      colValLen = -1;
      return OFVR_OK;
   }
   int hiveColType = p_colTypeOrdinals_[colNo];
   OFVR_RetCode rc; 
   if (hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_SHORT_TYPE
         && hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_INT_TYPE
         && hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_LONG_TYPE
         && hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_BYTE_TYPE
         && hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_BOOLEAN_TYPE)
      return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
   if ((rc = getNullValue(colNo, nullVal)) != OFVR_OK)
      return rc;
   if (nullVal == -1) {
      colValLen = -1;
      return OFVR_OK;
   }
   short shortVal;
   BYTE byteVal;
   int intVal;
   int rowNo = (p_isRepeating_[colNo] ? 0 : currRowNum_);
   switch (dataType) {
      case REC_BIN16_SIGNED:
         if (hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_SHORT_TYPE)
            return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
         shortVal = (short)p_colValArrays_[colNo][rowNo]; 
         *(short *)colVal = shortVal;
         colValLen = sizeof(short);
         break;
     case REC_BIN32_SIGNED: 
         if (hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_INT_TYPE)
            return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
         intVal = (int)p_colValArrays_[colNo][rowNo]; 
         *(int *)colVal = intVal;
         colValLen = sizeof(int);
         break;
     case REC_BIN64_SIGNED: 
         if (hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_LONG_TYPE)
            return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
         *(long *)colVal = (long)p_colValArrays_[colNo][rowNo]; 
         colValLen = sizeof(long);
         break;
     case REC_BIN8_SIGNED:
     case REC_BOOLEAN:
         if (hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_BOOLEAN_TYPE
            && hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_BYTE_TYPE)
            return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
         byteVal = (BYTE)p_colValArrays_[colNo][rowNo]; 
         *(BYTE *)colVal = byteVal;
         colValLen = sizeof(BYTE);
         break;
     default:
         return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
   }
   if (hdfsStats_)
      hdfsStats_->incBytesRead(colValLen);
   return OFVR_OK;
}

OFVR_RetCode OrcFileVectorReader::getDoubleOrFloatCol(int colNo, Int16 dataType, BYTE *colVal,
                                        Lng32 &colValLen, short &nullVal)
{
   if (colNo >= colCount_) {  // if this column is missing from the ORC file
      nullVal = -1;   // then return a null value for it
      colValLen = -1;
      return OFVR_OK;
   }
   int hiveColType = p_colTypeOrdinals_[colNo];
   OFVR_RetCode rc; 
   if (hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_DOUBLE_TYPE
         && hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_FLOAT_TYPE)
      return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
   if ((rc = getNullValue(colNo, nullVal)) != OFVR_OK)
      return rc;
   if (nullVal == -1) {
      colValLen = -1;
      return OFVR_OK;
   }
   double doubleVal;
   float floatVal;
   int rowNo = (p_isRepeating_[colNo] ? 0 : currRowNum_);
   switch (dataType) {
      case REC_FLOAT32:
         floatVal = (float)(*(double *)&(p_colValArrays_[colNo][rowNo])); 
         *(float*)colVal = floatVal;
         colValLen = sizeof(float);
         break;
      case REC_FLOAT64:
         doubleVal = (double)(*(double *)&(p_colValArrays_[colNo][rowNo])); 
         *(double*)colVal = doubleVal;
         colValLen = sizeof(double);
         break;
     default:
         return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
   }
   if (hdfsStats_)
      hdfsStats_->incBytesRead(colValLen);
   return OFVR_OK;
}

OFVR_RetCode OrcFileVectorReader::getDateOrTimestampCol(int colNo, Int16 datetimeCode, BYTE *colVal,
                                        Lng32 &colValLen, short &nullVal)
{
   if (colNo >= colCount_) {  // if this column is missing from the ORC file
      nullVal = -1;   // then return a null value for it
      colValLen = -1;
      return OFVR_OK;
   }
   int hiveColType = p_colTypeOrdinals_[colNo];
   int nanoVal;
   Int64 timeVal;
   short tsFields[DatetimeValue::N_DATETIME_FIELDS+1];

   OFVR_RetCode rc; 
   if (hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_DATE_TYPE
         && hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_TIMESTAMP_TYPE)
      return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
   if ((rc = getNullValue(colNo, nullVal)) != OFVR_OK)
      return rc;
   if (nullVal == -1) {
      colValLen = -1;
      return OFVR_OK;
   }
   int rowNo = (p_isRepeating_[colNo] ? 0 : currRowNum_);
   switch (hiveColType) {
      case org_trafodion_sql_OrcFileVectorReader_HIVE_DATE_TYPE: 
         if (datetimeCode != REC_DTCODE_DATE)
            return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
          // Date is stored as days since 1970-01-01
          // Hence, multiply by 86400 secs in a day
         timeVal = ((long)p_colValArrays_[colNo][rowNo]) * 86400;
         DatetimeValue::decodeTimestamp(TRUE, timeVal, FALSE, tsFields);
         *(short *)colVal = tsFields[DatetimeValue::YEAR];
         *(colVal+2) = tsFields[DatetimeValue::MONTH];
         *(colVal+3) = tsFields[DatetimeValue::DAY];
         colValLen = 4;
         break;
      case org_trafodion_sql_OrcFileVectorReader_HIVE_TIMESTAMP_TYPE: 
         if (datetimeCode != REC_DTCODE_TIMESTAMP)
            return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
         // Timestamp is stored as milliseconds since 1970-01-01 as long 
         timeVal  = p_colValArrays_[colNo][rowNo];
         nanoVal = p_colValExtArrays1_[colNo][rowNo];
         DatetimeValue::decodeTimestamp(useUTCTimestamp_, timeVal, TRUE, tsFields);
         *(short *)colVal = tsFields[DatetimeValue::YEAR];
         *(colVal+2) = tsFields[DatetimeValue::MONTH];
         *(colVal+3) = tsFields[DatetimeValue::DAY];
         *(colVal+4) = tsFields[DatetimeValue::HOUR];
         *(colVal+5) = tsFields[DatetimeValue::MINUTE];
         *(colVal+6) = tsFields[DatetimeValue::SECOND];
         memcpy((colVal+7), &nanoVal, 4);
         colValLen = 11;
         break;
     default:
         return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
   }
   if (hdfsStats_)
      hdfsStats_->incBytesRead(colValLen);
   return OFVR_OK;
}

OFVR_RetCode OrcFileVectorReader::getDecimalCol(int colNo, Int16 dataType, BYTE *colVal,
                                        Lng32 &colValLen, short &nullVal, char *tempStr, Lng32 &tempStrLen, bool &convFromStr)

{
   if (colNo >= colCount_) {  // if this column is missing from the ORC file
      nullVal = -1;   // then return a null value for it
      colValLen = -1;
      return OFVR_OK;
   }
   int hiveColType = p_colTypeOrdinals_[colNo];
   OFVR_RetCode rc; 
   if (hiveColType != org_trafodion_sql_OrcFileVectorReader_HIVE_DECIMAL_TYPE)
      return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
   if ((rc = getNullValue(colNo, nullVal)) != OFVR_OK)
      return rc;
   if (nullVal == -1) {
      colValLen = -1;
      return OFVR_OK;
   }
   // precision is an integer array with 1 element. All rows have same precison value
   jint precision = p_colValExtArrays1_[colNo][0];
   int rowNo = (p_isRepeating_[colNo] ? 0 : currRowNum_);
   if (precision < 19) {
      short shortVal;
      int intVal;
      convFromStr = FALSE;
      switch (dataType) {
         case REC_BIN16_SIGNED:
            shortVal = (short)p_colValArrays_[colNo][rowNo]; 
            *(short *)colVal = shortVal;
            colValLen = sizeof(short);
            break;
         case REC_BIN32_SIGNED: 
            intVal = (int)p_colValArrays_[colNo][rowNo]; 
            *(int *)colVal = intVal;
            colValLen = sizeof(int);
            break;
         case REC_BIN64_SIGNED: 
           *(long *)colVal = (long)p_colValArrays_[colNo][rowNo]; 
           colValLen = sizeof(long);
           break;
         default:
            return OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION;
       } // switch
       if (hdfsStats_)
          hdfsStats_->incBytesRead(colValLen);
   } else {
      convFromStr = TRUE;
      jstring jColBytes = (jstring)jenv_->GetObjectArrayElement((jobjectArray)jColVector_[colNo], rowNo); 
      if (jenv_->ExceptionCheck()) {
         getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorReader::open()");
         return OFVR_ERROR_COL_VECTOR_NULL;
      }
      jsize strLen = jenv_->GetStringUTFLength(jColBytes);
      if (strLen >= tempStrLen) {
         jenv_->DeleteLocalRef(jColBytes);
         return OFVR_ERROR_BIG_NUM_BUFFER_OVERFLOW;
      }
      const char *colBytes = jenv_->GetStringUTFChars(jColBytes, NULL);
      if (jenv_->ExceptionCheck()) {
         getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorReader::open()");
         return OFVR_ERROR_COL_VECTOR_DATA_COPY_ERROR;
      }
      strcpy(tempStr, colBytes);
      tempStrLen = strLen;
      jenv_->ReleaseStringUTFChars(jColBytes, colBytes); 
      jenv_->DeleteLocalRef(jColBytes);
      if (hdfsStats_)
         hdfsStats_->incBytesRead(tempStrLen);
   }
   return OFVR_OK;
}

