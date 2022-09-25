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

#include "OrcFileVectorWriter.h"
#include "QRLogger.h"
#include "HBaseClient_JNI.h"

// ===========================================================================
// ===== Class OrcFileVectorWriter
// ===========================================================================

JavaMethodInit* OrcFileVectorWriter::JavaMethods_ = NULL;
jclass OrcFileVectorWriter::javaClass_ = 0;
bool OrcFileVectorWriter::javaMethodsInitialized_ = false;
pthread_mutex_t OrcFileVectorWriter::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

static const char* const ofvwErrorEnumStr[] = 
{
  "No more data."
 ,"JNI param error in open()"
 ,"Java exception in open()"
 ,"JNI param error in insert()"
 ,"Java exception in insert()"
 ,"Java exception in close()"
 ,"Unknown error returned from ORC interface"
};

//////////////////////////////////////////////////////////////////////////////
// OrcFileVectorWriter::getErrorText
//////////////////////////////////////////////////////////////////////////////
char* OrcFileVectorWriter::getErrorText(OFW_RetCode pv_errEnum)
{
  if (pv_errEnum < (OFW_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)pv_errEnum);
  else    
    return (char*)ofvwErrorEnumStr[pv_errEnum - JOI_LAST];
}

//////////////////////////////////////////////////////////////////////////////
// OrcFileVectorWriter::~OrcFileVectorWriter()
//////////////////////////////////////////////////////////////////////////////
OrcFileVectorWriter::~OrcFileVectorWriter()
{
  close();
}
 
//////////////////////////////////////////////////////////////////////////////
// OrcFileVectorWriter::init()
//////////////////////////////////////////////////////////////////////////////
OFW_RetCode OrcFileVectorWriter::init()
{
  static char className[]="org/trafodion/sql/OrcFileVectorWriter";

  OFW_RetCode lv_retcode = OFW_OK;
 
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"Enter OrcFileVectorWriter::init()");
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
    JavaMethods_[JM_OPEN      ].jm_signature = "(Ljava/lang/String;[Ljava/lang/Object;[Ljava/lang/Object;ZJIILjava/lang/String;)V";
    JavaMethods_[JM_CLOSE     ].jm_name      = "close";
    JavaMethods_[JM_CLOSE     ].jm_signature = "()V";
    JavaMethods_[JM_INSERTROWS].jm_name      = "insertRows";
    JavaMethods_[JM_INSERTROWS].jm_signature = "(Ljava/lang/Object;III)V";

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

OFW_RetCode OrcFileVectorWriter::open(const char * fileName,
                                TextVec * colNameList, 
                                TextVec * colTypeInfoList,
                                bool blockPadding,
                                Int64 stripeSize,
                                Int32 bufferSize,
                                Int32 rowIndexStride,
                                const char * compression,
                                int flags)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"OrcFileVectorWriter::open(%s) called.",
                fileName);

  jstring   js_fileName = NULL;
  jstring   jresult = NULL;
  jobjectArray jor_colNameList = NULL;
  jobjectArray jor_colTypeInfoList = NULL;
  jboolean  jb_blockPadding = blockPadding;  // false
  jlong     jl_stripeSize = stripeSize;
  jint      ji_bufferSize = bufferSize;
  jint      ji_rowIndexStride = rowIndexStride; // 10000
  jstring   js_compression = NULL; 
  
  if (initJNIEnv() != JOI_OK)
     return OFW_ERROR_OPEN_PARAM;
  js_fileName = jenv_->NewStringUTF(fileName);
  if (js_fileName == NULL) {
     jenv_->PopLocalFrame(NULL);
     return OFW_ERROR_OPEN_PARAM;
  }

  js_compression = jenv_->NewStringUTF(compression);
  if (js_compression == NULL) {
     jenv_->PopLocalFrame(NULL);
     return OFW_ERROR_OPEN_PARAM;
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
  jenv_->CallVoidMethod(javaObj_,
					     JavaMethods_[JM_OPEN].methodID,
					     js_fileName,
                                             jor_colNameList,
                                             jor_colTypeInfoList,
                                             jb_blockPadding,
                                             jl_stripeSize,
                                             ji_bufferSize,
                                             ji_rowIndexStride,
                                             js_compression);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorWriter::open()");
    jenv_->PopLocalFrame(NULL);
    return OFW_ERROR_OPEN_EXCEPTION;
  }

  jenv_->PopLocalFrame(NULL);
  return OFW_OK;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
OFW_RetCode OrcFileVectorWriter::close()
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"OrcFileVectorWriter::close() called.");

  OFW_RetCode lv_retcode = OFW_OK;

  if (initJNIEnv() != JOI_OK)
     return OFW_ERROR_CLOSE_EXCEPTION;
    
  tsRecentJMFromJNI = JavaMethods_[JM_CLOSE].jm_full_name;
  jenv_->CallVoidMethod(javaObj_, JavaMethods_[JM_CLOSE].methodID);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorWriter::close()");
    lv_retcode = OFW_ERROR_CLOSE_EXCEPTION;
    goto fn_exit;
  }
 fn_exit:  
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

OFW_RetCode OrcFileVectorWriter::insertRows(char * directBuffer,
                                      int directBufferMaxLen, 
                                      int numRowsInBuffer,
                                      int directBufferCurrLen)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_WRITER,
		LL_DEBUG,
		"OrcFileVectorWriter::insertRows() called.");

  OFW_RetCode lv_retcode = OFW_OK;

  if (initJNIEnv() != JOI_OK)
     return OFW_ERROR_INSERTROWS_EXCEPTION;

  tsRecentJMFromJNI = JavaMethods_[JM_INSERTROWS].jm_full_name;

  jobject jRows = jenv_->NewDirectByteBuffer(directBuffer, directBufferCurrLen);
  if (jRows == NULL) {
    getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorWriter::insertRows()");
    lv_retcode = OFW_ERROR_INSERTROWS_EXCEPTION;
    goto fn_exit;
  }

  jenv_->CallObjectMethod(javaObj_,
                                     JavaMethods_[JM_INSERTROWS].methodID,
                                     jRows,
                                     directBufferMaxLen,
                                     numRowsInBuffer,
                                     directBufferCurrLen);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "OrcFileVectorWriter::insertRows()");
    lv_retcode = OFW_ERROR_INSERTROWS_EXCEPTION;
    goto fn_exit;
  }

 fn_exit:  
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}
