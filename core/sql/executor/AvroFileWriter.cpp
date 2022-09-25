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

#include "AvroFileWriter.h"
#include "QRLogger.h"
#include "HBaseClient_JNI.h"

// ===========================================================================
// ===== Class AvroFileWriter
// ===========================================================================

JavaMethodInit* AvroFileWriter::JavaMethods_ = NULL;
jclass AvroFileWriter::javaClass_ = 0;
bool AvroFileWriter::javaMethodsInitialized_ = false;
pthread_mutex_t AvroFileWriter::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

static const char* const sfrErrorEnumStr[] = 
{
  "No more data."
 ,"JNI param error in open()"
 ,"Java exception in open()"
 ,"JNI param error in insert()"
 ,"Java exception in insert()"
 ,"Java exception in close()"
 ,"Unknown error returned from AVRO interface"
};

//////////////////////////////////////////////////////////////////////////////
// AvroFileWriter::getErrorText
//////////////////////////////////////////////////////////////////////////////
char* AvroFileWriter::getErrorText(OFW_RetCode pv_errEnum)
{
  if (pv_errEnum < (OFW_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)pv_errEnum);
  else    
    return (char*)sfrErrorEnumStr[pv_errEnum - JOI_LAST];
}

//////////////////////////////////////////////////////////////////////////////
// AvroFileWriter::~AvroFileWriter()
//////////////////////////////////////////////////////////////////////////////
AvroFileWriter::~AvroFileWriter()
{
  //  close();
  releaseJavaAllocation();
}
 
//////////////////////////////////////////////////////////////////////////////
// AvroFileWriter::init()
//////////////////////////////////////////////////////////////////////////////
OFW_RetCode AvroFileWriter::init()
{
  static char className[]="org/trafodion/sql/TrafAvroFileWriter";

  OFW_RetCode lv_retcode = OFW_OK;
 
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"Enter AvroFileWriter::init()");
  if (isInitialized())
    return lv_retcode;

  if (javaMethodsInitialized_)
     return (OFW_RetCode)JavaObjectInterface::init(className, 
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
    JavaMethods_[JM_OPEN      ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;[Ljava/lang/Object;[Ljava/lang/Object;I)Ljava/lang/String;";
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

OFW_RetCode AvroFileWriter::open(const char * tableName,
                                    const char * fileName,
                                    TextVec * colNameList, 
                                    TextVec * colTypeInfoList,
                                    int flags)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"AvroFileWriter::open(%s) called.",
                fileName);

  OFW_RetCode lv_retcode = OFW_OK;

  jstring   js_tableName = NULL;
  jstring   js_fileName = NULL;
  jstring   jresult = NULL;
  jobjectArray jor_colNameList = NULL;
  jobjectArray jor_colTypeInfoList = NULL;
  jint      ji_flags = flags;

  if (initJNIEnv() != JOI_OK)
     return OFW_ERROR_OPEN_EXCEPTION;

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

  // String open(java.lang.String, long, long, int, int[]);
  tsRecentJMFromJNI = JavaMethods_[JM_OPEN].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
					     JavaMethods_[JM_OPEN].methodID,
                                             js_tableName,
					     js_fileName,
                                             jor_colNameList,
                                             jor_colTypeInfoList,
                                             ji_flags);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "AvroFileWriter::open()");
    lv_retcode = OFW_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }
  
  if (jresult != NULL) {
    const char *my_string = jenv_->GetStringUTFChars(jresult, JNI_FALSE);
    QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		  LL_DEBUG,
		  "AvroFileWriter::open(%s), error:%s",
                  fileName,
		  my_string);
    logError(CAT_SQL_HDFS_ORC_FILE_WRITER,
	     "AvroFileWriter::open()",
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
OFW_RetCode AvroFileWriter::close()
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"AvroFileWriter::close() called.");

  if (initJNIEnv() != JOI_OK)
     return OFW_ERROR_CLOSE_EXCEPTION;
    
  // String close();
  tsRecentJMFromJNI = JavaMethods_[JM_CLOSE].jm_full_name;
  jstring jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
						     JavaMethods_[JM_CLOSE].methodID);

  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "AvroFileWriter::close()");
    jenv_->PopLocalFrame(NULL);
    return OFW_ERROR_CLOSE_EXCEPTION;
  }

  if (jresult!=NULL) {
    logError(CAT_SQL_HDFS_ORC_FILE_WRITER,
	     "AvroFileWriter::close()",
	     jresult);
    jenv_->PopLocalFrame(NULL);
    return OFW_ERROR_CLOSE_EXCEPTION;
  }
  
  jenv_->PopLocalFrame(NULL);
  return OFW_OK;

}

OFW_RetCode AvroFileWriter::insertRows(char * directBuffer,
                                      int directBufferMaxLen, 
                                      int numRowsInBuffer,
                                      int directBufferCurrLen)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"AvroFileWriter::insertRows() called.");

  OFW_RetCode lv_retcode = OFW_OK;

  if (initJNIEnv() != JOI_OK)
     return OFW_ERROR_INSERTROWS_EXCEPTION;

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
    getExceptionDetails(__FILE__, __LINE__, "AvroFileWriter::insertRows()");
    jenv_->PopLocalFrame(NULL);
    return OFW_ERROR_INSERTROWS_EXCEPTION;
  }

  if (jresult != NULL) 
    {
      logError(CAT_SQL_HDFS_ORC_FILE_WRITER,
               "AvroFileWriter::insertRows()",
               jresult);
      jenv_->PopLocalFrame(NULL);
      return OFW_ERROR_INSERTROWS_EXCEPTION;
    }
  
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

//////////////////////////////////////////////////////////////////////////////
// AvroFileWriter::releaseJavaAllocation()
//////////////////////////////////////////////////////////////////////////////
void AvroFileWriter::releaseJavaAllocation() {
  /*  if (! m_java_ba_released) {
    jenv_->ReleaseByteArrayElements(m_java_block,
				    m_java_ba,
				    JNI_ABORT);
    m_java_ba_released = true;
  }
  */
}
