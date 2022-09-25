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

#include "ParquetFileWriter.h"
#include "QRLogger.h"
#include "HBaseClient_JNI.h"

// ===========================================================================
// ===== Class ParquetFileWriter
// ===========================================================================

JavaMethodInit* ParquetFileWriter::JavaMethods_ = NULL;
jclass ParquetFileWriter::javaClass_ = 0;
bool ParquetFileWriter::javaMethodsInitialized_ = false;
pthread_mutex_t ParquetFileWriter::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

static const char* const sfrErrorEnumStr[] = 
{
  "No more data."
 ,"JNI param error in open()"
 ,"Java exception in open()"
 ,"JNI param error in insert()"
 ,"Java exception in insert()"
 ,"Java exception in close()"
 ,"Unknown error returned from PARQUET interface"
};

//////////////////////////////////////////////////////////////////////////////
// ParquetFileWriter::getErrorText
//////////////////////////////////////////////////////////////////////////////
char* ParquetFileWriter::getErrorText(OFW_RetCode pv_errEnum)
{
  if (pv_errEnum < (OFW_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)pv_errEnum);
  else    
    return (char*)sfrErrorEnumStr[pv_errEnum - JOI_LAST];
}

//////////////////////////////////////////////////////////////////////////////
// ParquetFileWriter::~ParquetFileWriter()
//////////////////////////////////////////////////////////////////////////////
ParquetFileWriter::~ParquetFileWriter()
{
  //  close();
  releaseJavaAllocation();
}
 
//////////////////////////////////////////////////////////////////////////////
// ParquetFileWriter::init()
//////////////////////////////////////////////////////////////////////////////
OFW_RetCode ParquetFileWriter::init()
{
  static char className[]="org/trafodion/sql/TrafParquetFileWriter";

  OFW_RetCode lv_retcode = OFW_OK;
 
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"Enter ParquetFileWriter::init()");

  if (isInitialized())
    return lv_retcode;

  if (javaMethodsInitialized_)
    lv_retcode = (OFW_RetCode)JavaObjectInterface::init(className, 
							javaClass_, 
							JavaMethods_, 
							(Int32)JM_LAST, javaMethodsInitialized_);
  else  
  {
    pthread_mutex_lock(&javaMethodsInitMutex_);
    if (javaMethodsInitialized_)
    {
      pthread_mutex_unlock(&javaMethodsInitMutex_);
      return (OFW_RetCode)JavaObjectInterface::init(className, javaClass_, JavaMethods_, (Int32)JM_LAST, javaMethodsInitialized_);
    }
    JavaMethods_ = new JavaMethodInit[JM_LAST];
    
    JavaMethods_[JM_CTOR      ].jm_name      = "<init>";
    JavaMethods_[JM_CTOR      ].jm_signature = "()V";
    JavaMethods_[JM_OPEN      ].jm_name      = "open";
    JavaMethods_[JM_OPEN      ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;[Ljava/lang/Object;[Ljava/lang/Object;JJJJLjava/lang/String;Ljava/lang/String;I)Ljava/lang/String;";
    JavaMethods_[JM_CLOSE     ].jm_name      = "close";
    JavaMethods_[JM_CLOSE     ].jm_signature = "()Ljava/lang/String;";
    JavaMethods_[JM_INSERTROWS].jm_name      = "insertRows";
    JavaMethods_[JM_INSERTROWS].jm_signature = "(Ljava/lang/Object;III)Ljava/lang/String;";

    lv_retcode = (OFW_RetCode)JavaObjectInterface::init(className,
							javaClass_,
							JavaMethods_,
							(Int32)JM_LAST, javaMethodsInitialized_);
    if (lv_retcode == OFW_OK)
       javaMethodsInitialized_ = TRUE;
    pthread_mutex_unlock(&javaMethodsInitMutex_);
  }
  return lv_retcode;
}

OFW_RetCode ParquetFileWriter::open(const char * tableName,
                                    const char * fileName,
                                    TextVec * colNameList, 
                                    TextVec * colTypeInfoList,
                                    Int64 blockSize,
                                    Int64 pageSize,
                                    Int64 dictionaryPageSize,
                                    Int64 maxPadding,
                                    const char * compression,
                                    const char * parqSchStr,
                                    int flags)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"ParquetFileWriter::open(%s) called.",
                fileName);

  OFW_RetCode lv_retcode = OFW_OK;
  if (initJNIEnv() != JOI_OK)
    return OFW_ERROR_OPEN_PARAM;

  jstring   js_tableName = NULL;
  jstring   js_fileName = NULL;
  jstring   jresult = NULL;
  jobjectArray jor_colNameList = NULL;
  jobjectArray jor_colTypeInfoList = NULL;
  jint      ji_flags = flags;

  jstring   js_compression = NULL; 
  jstring   js_parqSchStr = NULL;

  releaseJavaAllocation();
  js_tableName = jenv_->NewStringUTF(tableName);
  if (js_tableName == NULL) {
    lv_retcode = OFW_ERROR_OPEN_PARAM;
    goto fn_exit;
  }

  js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) {
    lv_retcode = OFW_ERROR_OPEN_PARAM;
    goto fn_exit;
  }
  
  if (colNameList && (!colNameList->empty())) {
    jor_colNameList = convertToStringObjectArray(*colNameList);
    if (jor_colNameList == NULL) {
      jenv_->PopLocalFrame(NULL);
      return OFW_ERROR_OPEN_PARAM;
    }
  }

  if (colTypeInfoList && (!colTypeInfoList->empty())) {
    jor_colTypeInfoList = convertToByteArrayObjectArray(*colTypeInfoList);
    if (jor_colTypeInfoList == NULL) {
      jenv_->PopLocalFrame(NULL);
      return OFW_ERROR_OPEN_PARAM;
    }
  }

  js_compression = jenv_->NewStringUTF(compression);
  if (js_compression == NULL) {
    lv_retcode = OFW_ERROR_OPEN_PARAM;
    goto fn_exit;
  }

  js_parqSchStr = jenv_->NewStringUTF(parqSchStr);
  if (js_compression == NULL) {
    lv_retcode = OFW_ERROR_OPEN_PARAM;
    goto fn_exit;
  }

  // String open(java.lang.String, long, long, int, int[]);
  tsRecentJMFromJNI = JavaMethods_[JM_OPEN].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
					     JavaMethods_[JM_OPEN].methodID,
                                             js_tableName,
					     js_fileName,
                                             jor_colNameList,
                                             jor_colTypeInfoList,
                                             (jlong)blockSize,
                                             (jlong)pageSize,
                                             (jlong)dictionaryPageSize,
                                             (jlong)maxPadding,
                                             js_compression,
                                             js_parqSchStr,
                                             ji_flags);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "ParquetFileWriter::open()");  
    lv_retcode = OFW_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }
  
  if (jresult != NULL) {
    const char *my_string = jenv_->GetStringUTFChars(jresult, JNI_FALSE);
    QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		  LL_DEBUG,
		  "ParquetFileWriter::open(%s), error:%s",
                  fileName,
		  my_string);
    logError(CAT_SQL_HDFS_ORC_FILE_WRITER,
	     "ParquetFileWriter::open()",
	     jresult);

    lv_retcode = OFW_ERROR_OPEN_EXCEPTION;

    goto fn_exit;
  }

 fn_exit:  
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFW_RetCode ParquetFileWriter::close()
{
  OFW_RetCode lv_retcode = OFW_OK;
  
  if (initJNIEnv() != JOI_OK)
    return OFW_ERROR_OPEN_PARAM;

  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"ParquetFileWriter::close() called.");
  // String close();
  tsRecentJMFromJNI = JavaMethods_[JM_CLOSE].jm_full_name;
  jstring jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
						     JavaMethods_[JM_CLOSE].methodID);

  if (jresult!=NULL) {
    logError(CAT_SQL_HDFS_ORC_FILE_WRITER,
	     "ParquetFileWriter::close()",
	     jresult);
    jenv_->PopLocalFrame(NULL);
    return OFW_ERROR_CLOSE_EXCEPTION;
  }
  
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

OFW_RetCode ParquetFileWriter::insertRows(char * directBuffer,
                                      int directBufferMaxLen, 
                                      int numRowsInBuffer,
                                      int directBufferCurrLen)
{
  OFW_RetCode lv_retcode = OFW_OK;

  if (initJNIEnv() != JOI_OK)
    return OFW_ERROR_OPEN_PARAM;

  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"ParquetFileWriter::insertRows() called.");

  tsRecentJMFromJNI = JavaMethods_[JM_INSERTROWS].jm_full_name;

  jobject jRows = jenv_->NewDirectByteBuffer(directBuffer, directBufferCurrLen);

  jstring jresult = 
    (jstring)jenv_->CallObjectMethod(javaObj_,
                                     JavaMethods_[JM_INSERTROWS].methodID,
                                     jRows,
                                     directBufferMaxLen,
                                     numRowsInBuffer,
                                     directBufferCurrLen);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "ParquetFileWriter::insertRows()");  
    jenv_->PopLocalFrame(NULL);
    return OFW_ERROR_INSERTROWS_EXCEPTION;
  }

  if (jresult != NULL) 
    {
      logError(CAT_SQL_HDFS_ORC_FILE_WRITER,
               "ParquetFileWriter::insertRows()",
               jresult);
      jenv_->PopLocalFrame(NULL);
      return OFW_ERROR_INSERTROWS_EXCEPTION;
    }
  
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

//////////////////////////////////////////////////////////////////////////////
// ParquetFileWriter::releaseJavaAllocation()
//////////////////////////////////////////////////////////////////////////////
void ParquetFileWriter::releaseJavaAllocation() {
  /*  if (! m_java_ba_released) {
    jenv_->ReleaseByteArrayElements(m_java_block,
				    m_java_ba,
				    JNI_ABORT);
    m_java_ba_released = true;
  }
  */
}
