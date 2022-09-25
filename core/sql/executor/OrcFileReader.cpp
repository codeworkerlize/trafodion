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
#include "TrafExtStorageUtils.h"
#include "OrcFileReader.h"
#include "QRLogger.h"
#include "HBaseClient_JNI.h"
#include "ttime.h"
#include "ComCextdecs.h"

// ===========================================================================
// ===== Class OrcFileReader
// ===========================================================================

JavaMethodInit* OrcFileReader::JavaMethods_ = NULL;
jclass OrcFileReader::javaClass_ = 0;
bool OrcFileReader::javaMethodsInitialized_ = false;
pthread_mutex_t OrcFileReader::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

// array is indexed by enum OFR_RetCode
static const char* const ofrErrorEnumStr[] = 
{
  "No more data."
 ,"JNI NewStringUTF() in open()"
 ,"Java exception in open()"
 ,"Java file not found exception in open()"
 ,"Java exception in close()"
 ,"Error from GetStripeInfo()"
 ,"Java exception in GetColStats()"
 ,"Java exception in getSumStringLengths()"
 ,"Java javax.security.sasl.SaslException "
 ,"Unknown error returned from ORC interface"
 ,"Java exception in getRowNum()"
};


//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
char* OrcFileReader::getErrorText(OFR_RetCode pv_errEnum)
{
  if (pv_errEnum < (OFR_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)pv_errEnum);
  else    
    return (char*)ofrErrorEnumStr[pv_errEnum - JOI_LAST];
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OrcFileReader::~OrcFileReader()
{
  close();
}
 
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
JOI_RetCode OrcFileReader::init()
{
  static char className[]="org/trafodion/sql/OrcFileReader";

  JOI_RetCode lv_retcode = JOI_OK;
 
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"Enter OrcFileReader::init()");

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
    JavaMethods_[JM_OPEN      ].jm_signature = "(Ljava/lang/String;JJI[IIII[Ljava/lang/Object;[Ljava/lang/Object;I)Ljava/lang/String;";
    JavaMethods_[JM_GETCOLSTATS ].jm_name      = "getColStats";
    JavaMethods_[JM_GETCOLSTATS ].jm_signature = "(I)[Ljava/lang/Object;";
    JavaMethods_[JM_GETSUMSTRINGLENGTHS ].jm_name = "getSumStringLengths";
    JavaMethods_[JM_GETSUMSTRINGLENGTHS ].jm_signature = "()J";
    JavaMethods_[JM_CLOSE     ].jm_name      = "close";
    JavaMethods_[JM_CLOSE     ].jm_signature = "()V";
    JavaMethods_[JM_GETSTRIPE_NUMROWS].jm_name      = "getStripeNumRows";
    JavaMethods_[JM_GETSTRIPE_NUMROWS].jm_signature = "()[J";
    JavaMethods_[JM_GETSTRIPE_OFFSETS].jm_name      = "getStripeOffsets";
    JavaMethods_[JM_GETSTRIPE_OFFSETS].jm_signature = "()[J";
    JavaMethods_[JM_GETSTRIPE_LENGTHS].jm_name      = "getStripeLengths";
    JavaMethods_[JM_GETSTRIPE_LENGTHS].jm_signature = "()[J";

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
OFR_RetCode OrcFileReader::open(const char * tableName,
                                const char *pv_path,
                                const int expected_row_size,
                                const int max_rows_to_fill,
                                const int max_allocation_in_mb,
                                Int64 offset,  
                                Int64 length,  
				int   pv_num_cols_in_projection,
				int  *pv_which_cols,
                                TextVec *ppiVec,
                                TextVec *ppiAllCols,
                                TextVec *colNameVec,
                                TextVec *colTypeVec,
                                int flags)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileReader::open(start_time=%s, %s, %d, %p) called.",
		reportTimestamp(),
		pv_path,
		pv_num_cols_in_projection,
		pv_which_cols);

  startTime_ = NA_JulianTimestamp();

  OFR_RetCode lv_retcode = OFR_OK;

  jstring   js_path = NULL;
  jintArray jia_which_cols = NULL;
  jint      ji_num_cols_in_projection = pv_num_cols_in_projection;
  jlong     jl_offset = offset;
  jlong     jl_length = length;
  jint      ji_expected_row_size = expected_row_size;
  jint      ji_max_rows_to_fill = max_rows_to_fill;
  jint      ji_max_allocation_in_mb = max_allocation_in_mb;
  jobject   jo_ppiBuf = NULL;
  jstring   jresult = NULL;
  jobjectArray jor_ppiVec = NULL;
  jobjectArray jor_ppiAllCols = NULL;
  jint      ji_flags = flags;
  
  if (initJNIEnv() != JOI_OK)
     return OFR_ERROR_OPEN_PARAM;

  js_path = jenv_->NewStringUTF(pv_path);
  if (js_path == NULL) {
    lv_retcode = OFR_ERROR_OPEN_PARAM;
    goto fn_exit;
  }
  
  if ((pv_num_cols_in_projection > 0) && 
      (pv_which_cols != 0)) {
    jia_which_cols = jenv_->NewIntArray(pv_num_cols_in_projection);
    if (jia_which_cols == NULL) {

      QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		    LL_ERROR,
		    "OrcFileReader::open(%s, %d, %p). Error while allocating memory for j_col_array",
		    pv_path,
		    pv_num_cols_in_projection,
		    pv_which_cols);

      
      lv_retcode = OFR_ERROR_OPEN_PARAM;
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
        getExceptionDetails(__FILE__, __LINE__, "OrcFileReader::open()");
        lv_retcode = OFR_ERROR_OPEN_PARAM;
	goto fn_exit;
      }

      if (ppiAllCols && (!ppiAllCols->empty()))
        {
          QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  Adding %d cols.", 
                        ppiAllCols->size());
          jor_ppiAllCols = convertToStringObjectArray(*ppiAllCols);
          if (jor_ppiAllCols == NULL)
            {
              getExceptionDetails(__FILE__, __LINE__, "OrcFileReader::open()");
              lv_retcode = OFR_ERROR_OPEN_PARAM;
	      goto fn_exit;
            }
        }
    }

  // String open(java.lang.String, long, long, int, int[]);
  tsRecentJMFromJNI = JavaMethods_[JM_OPEN].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
					     JavaMethods_[JM_OPEN].methodID,
					     js_path,
					     jl_offset,
					     jl_length,
					     ji_num_cols_in_projection,
					     jia_which_cols,
					     ji_expected_row_size,
					     ji_max_rows_to_fill,
					     ji_max_allocation_in_mb,
                                             jor_ppiVec,
                                             jor_ppiAllCols,
                                             ji_flags);


  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "OrcFileReader::open()");
    lv_retcode = OFR_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }

  if (jresult != NULL) {
    const char *my_string = jenv_->GetStringUTFChars(jresult, JNI_FALSE);
    QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		  LL_DEBUG,
		  "OrcFileReader::open(%s), error:%s",
		  pv_path,
		  my_string);
    logError(CAT_SQL_HDFS_ORC_FILE_READER,
	     "OrcFileReader::open()",
	     jresult);
    if (strcmp(my_string, "file not found") == 0)
      lv_retcode = OFR_ERROR_FILE_NOT_FOUND_EXCEPTION;
    else if (strcmp(my_string, "GSS exception") == 0)
      lv_retcode = OFR_ERROR_GSS_EXCEPTION;
    else
      lv_retcode = OFR_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }

 fn_exit:  

  path_ = pv_path;

  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

OFR_RetCode OrcFileReader::close()
{
  Int64 timeDiff = NA_JulianTimestamp() - startTime_;

  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileReader::close() ElapsedTime=%s) called", reportTimeDiff(timeDiff));

  if (initJNIEnv() != JOI_OK)
     return OFR_ERROR_CLOSE_EXCEPTION;
    
  // String close();
  tsRecentJMFromJNI = JavaMethods_[JM_CLOSE].jm_full_name;
  jenv_->CallObjectMethod(javaObj_, JavaMethods_[JM_CLOSE].methodID);

  if (jenv_->ExceptionCheck()) 
  {
    getExceptionDetails(__FILE__, __LINE__, "OrcFileReader::close()");
    jenv_->PopLocalFrame(NULL);
    return OFR_ERROR_CLOSE_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return OFR_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr> *OrcFileReader::getColStats(NAHeap *heap, 
                                              int colNum, char * colName, int colType)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileReader::getColStats() called.");
  if (initJNIEnv() != JOI_OK)
     return NULL;

  jint      ji_colNum = colNum;

  tsRecentJMFromJNI = JavaMethods_[JM_GETCOLSTATS].jm_full_name;
  jobject j_colStats = jenv_->CallObjectMethod
    (javaObj_,
     JavaMethods_[JM_GETCOLSTATS].methodID,
     ji_colNum);
  
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "OrcFileReader::getColStats()");
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  NAArray<HbaseStr> *colStats = NULL;
  jint retcode = convertByteArrayObjectArrayToNAArray(heap, (jarray)j_colStats, &colStats);
  jenv_->PopLocalFrame(NULL);
  if (retcode == 0)
     return NULL;
  else
     return colStats;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
  OFR_RetCode    OrcFileReader::getSumStringLengths(Int64& result)
{
  result = 0;  // initialize output

  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER,
		LL_DEBUG,
		"OrcFileReader::getSumStringLengths() called.");
  if (initJNIEnv() != JOI_OK)
     return OFR_ERROR_GETSUMSTRINGLENGTHS_EXCEPTION;
  if (javaObj_ == NULL) {
    // Maybe there was an initialization error.
    return OFR_ERROR_GETSUMSTRINGLENGTHS_EXCEPTION;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_GETSUMSTRINGLENGTHS].jm_full_name;
  result = jenv_->CallLongMethod(javaObj_,
                                 JavaMethods_[JM_GETSUMSTRINGLENGTHS].methodID);
  
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "OrcFileReader::getSumStringLengths");
    jenv_->PopLocalFrame(NULL);
    return OFR_ERROR_GETSUMSTRINGLENGTHS_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return OFR_OK;
}

OFR_RetCode 
OrcFileReader::getLongArray(OrcFileReader::JAVA_METHODS method, const char* msg, LIST(Int64)& resultArray)
{

  if (initJNIEnv() != JOI_OK)
     return OFR_ERROR_GETSTRIPEINFO_EXCEPTION;

  tsRecentJMFromJNI = JavaMethods_[method].jm_full_name;

  jlongArray jresult = (jlongArray)jenv_->CallObjectMethod(javaObj_, JavaMethods_[method].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "OrcFileReader::getLongArray");
    jenv_->PopLocalFrame(NULL);
    return OFR_ERROR_GETSTRIPEINFO_EXCEPTION;
  }

  if (jresult == NULL) {
    logError(CAT_SQL_HDFS_ORC_FILE_READER,
             msg,
             getLastError());
    jenv_->PopLocalFrame(NULL);
    return OFR_ERROR_GETSTRIPEINFO_EXCEPTION;
  }

  int numOffsets = convertLongObjectArrayToList(heap_, jresult, resultArray);

  jenv_->PopLocalFrame(NULL);
  return OFR_OK;
}

OFR_RetCode 
OrcFileReader::getStripeInfo(LIST(Int64)& stripeRows,
                             LIST(Int64)& stripeOffsets,
                             LIST(Int64)& stripeLengths 
                            )
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER, 
		LL_DEBUG, 
		"OrcFileReader::getStripeInfo() called.");

  OFR_RetCode retCode = OFR_OK;

  retCode = getLongArray(JM_GETSTRIPE_NUMROWS, "OrcFileReader::getStripeNumRows()", stripeRows);

  if ( retCode == OFR_OK ) 
    retCode = getLongArray(JM_GETSTRIPE_OFFSETS, "OrcFileReader::getStripeOffsets()", stripeOffsets);

  if ( retCode == OFR_OK ) 
    retCode = getLongArray(JM_GETSTRIPE_LENGTHS, "OrcFileReader::getStripeLengths()", stripeLengths);

  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER, LL_DEBUG, "=>Returning %d.", retCode);
  return retCode;
}

