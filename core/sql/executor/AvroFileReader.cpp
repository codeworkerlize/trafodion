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
#include "AvroFileReader.h"
#include "QRLogger.h"
#include "HBaseClient_JNI.h"
#include "ttime.h"
#include "ComCextdecs.h"

// ===========================================================================
// ===== Class AvroFileReader
// ===========================================================================

JavaMethodInit* AvroFileReader::JavaMethods_ = NULL;
jclass AvroFileReader::javaClass_ = 0;
bool AvroFileReader::javaMethodsInitialized_ = false;
pthread_mutex_t AvroFileReader::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

// array is indexed by enum AFR_RetCode
static const char* const afrErrorEnumStr[] = 
{
  "No more data."
 ,"JNI NewStringUTF() in open()"
 ,"Java exception in open()"
 ,"Java file not found exception in open()"
 ,"Java exception in getPos()"
 ,"Java exception in seeknSync()"
 ,"Java exception in isEOF()"
 ,"Java exception in fetchNextRow()"
 ,"Java exception in close()"
 ,"Error from GetStripeInfo()"
 ,"Java exception in GetColStats()"
 ,"Java exception in getSumStringLengths()"
 ,"Unknown error returned from Avro interface"
};

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
char* AvroFileReader::getErrorText(AFR_RetCode pv_errEnum)
{
  if (pv_errEnum < (AFR_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)pv_errEnum);
  else    
    return (char*)afrErrorEnumStr[pv_errEnum - JOI_LAST];
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
AvroFileReader::~AvroFileReader()
{
  close();
  releaseJavaAllocation();
}
 
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
JOI_RetCode AvroFileReader::init()
{
  static char className[]="org/trafodion/sql/TrafAvroFileReader";

  JOI_RetCode lv_retcode = JOI_OK;
 
  QRLogger::log(CAT_SQL_HDFS_AVRO_FILE_READER,
		LL_DEBUG,
		"Enter AvroFileReader::init()");
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
    JavaMethods_[JM_OPEN      ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;JJI[IIII[Ljava/lang/Object;[Ljava/lang/Object;[Ljava/lang/Object;[Ljava/lang/Object;I)Ljava/lang/String;";

    JavaMethods_[JM_FETCHBLOCK].jm_name      = "fetchNextBlock";
    JavaMethods_[JM_FETCHBLOCK].jm_signature = "()Ljava/nio/ByteBuffer;";
    JavaMethods_[JM_FETCHROW  ].jm_name      = "fetchNextRow";
    JavaMethods_[JM_FETCHROW  ].jm_signature = "()[B";

    JavaMethods_[JM_GETCOLSTATS ].jm_name      = "getColStats";
    JavaMethods_[JM_GETCOLSTATS ].jm_signature = "(ILjava/lang/String;I)[Ljava/lang/Object;";

    JavaMethods_[JM_GET_FILE_STATS       ].jm_name      = "getFileStats";
    JavaMethods_[JM_GET_FILE_STATS       ].jm_signature = "(Ljava/lang/String;)[[B";

    JavaMethods_[JM_GET_FILE_SCHEMA       ].jm_name      = "getFileSchema";
    JavaMethods_[JM_GET_FILE_SCHEMA       ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;[Ljava/lang/Object;[Ljava/lang/Object;I)[Ljava/lang/String;";

    JavaMethods_[JM_CLOSE     ].jm_name      = "close";
    JavaMethods_[JM_CLOSE     ].jm_signature = "()Ljava/lang/String;";

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
AFR_RetCode AvroFileReader::open(const char *pv_name,
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
  QRLogger::log(CAT_SQL_HDFS_AVRO_FILE_READER,
		LL_DEBUG,
		"AvroFileReader::open(start_time=%s, %s, %d, %p) called.",
		reportTimestamp(),
		pv_path,
		pv_num_cols_in_projection,
		pv_which_cols);

  startTime_ = NA_JulianTimestamp();

  totalRowsRead_ = 0;
  totalBlocksFetched_ = 0;

  blocksReadTime_ = 0;

  AFR_RetCode lv_retcode = AFR_OK;

  jstring   js_name = NULL;
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
  jobjectArray jor_colNameVec = NULL;
  jobjectArray jor_colTypeVec = NULL;
  jint      ji_flags = flags;

  if (initJNIEnv() != JOI_OK)
     return AFR_ERROR_OPEN_EXCEPTION;

  releaseJavaAllocation();

  if (pv_name != NULL) {
    js_name = jenv_->NewStringUTF(pv_name);
    if (js_name == NULL) {
      lv_retcode = AFR_ERROR_OPEN_PARAM;
      goto fn_exit;
    }
  }

  js_path = jenv_->NewStringUTF(pv_path);
  if (js_path == NULL) {
    lv_retcode = AFR_ERROR_OPEN_PARAM;
    goto fn_exit;
  }
  
  if ((pv_num_cols_in_projection > 0) && 
      (pv_which_cols != 0)) {
    jia_which_cols = jenv_->NewIntArray(pv_num_cols_in_projection);
    if (jia_which_cols == NULL) {


      QRLogger::log(CAT_SQL_HDFS_AVRO_FILE_READER,
		    LL_ERROR,
		    "AvroFileReader::open(%s, %d, %p). Error while allocating memory for j_col_array",
		    pv_path,
		    pv_num_cols_in_projection,
		    pv_which_cols);

      
      lv_retcode = AFR_ERROR_OPEN_PARAM;
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
        lv_retcode = AFR_ERROR_OPEN_PARAM;
	goto fn_exit;
      }

      if (ppiAllCols && (!ppiAllCols->empty()))
        {
          QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  Adding %d cols.", 
                        ppiAllCols->size());
          jor_ppiAllCols = convertToStringObjectArray(*ppiAllCols);
          if (jor_ppiAllCols == NULL)
            {
              getExceptionDetails(__FILE__, __LINE__, "AvroFileReader::open()");
              lv_retcode = AFR_ERROR_OPEN_PARAM;
	      goto fn_exit;
            }
        }
    }

  if (colNameVec && (!colNameVec->empty()))
    {
      jor_colNameVec = convertToStringObjectArray(*colNameVec);
      if (jor_colNameVec == NULL) {
        lv_retcode = AFR_ERROR_OPEN_PARAM;
	goto fn_exit;
      }
    }

  if (colTypeVec && (!colTypeVec->empty()))
    {
      jor_colTypeVec = convertToByteArrayObjectArray(*colTypeVec);
      if (jor_colTypeVec == NULL) {
        lv_retcode = AFR_ERROR_OPEN_PARAM;
	goto fn_exit;
      }
    }

  // String open(java.lang.String, long, long, int, int[]);
  tsRecentJMFromJNI = JavaMethods_[JM_OPEN].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
					     JavaMethods_[JM_OPEN].methodID,
                                             js_name,
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
                                             jor_colNameVec,
                                             jor_colTypeVec,
                                             ji_flags);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "AvroFileReader::open()");
    lv_retcode = AFR_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }

  if (jresult != NULL) {
    const char *my_string = jenv_->GetStringUTFChars(jresult, JNI_FALSE);
    QRLogger::log(CAT_SQL_HDFS_AVRO_FILE_READER,
		  LL_DEBUG,
		  "AvroFileReader::open(%s), error:%s",
		  pv_path,
		  my_string);
    logError(CAT_SQL_HDFS_AVRO_FILE_READER,
	     "AvroFileReader::open()",
	     jresult);
    if (strcmp(my_string, "file not found") == 0)
      lv_retcode = AFR_ERROR_FILE_NOT_FOUND_EXCEPTION;
    else
      lv_retcode = AFR_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }

 fn_exit:  

  path_ = pv_path;

  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

void AvroFileReader::releaseJavaAllocation() {

  if (! m_java_ba_released) {
    jenv_->ReleaseByteArrayElements(m_java_block,
				    m_java_ba,
				    JNI_ABORT);
    m_java_ba_released = true;
  }
    
}

//////////////////////////////////////////////////////////////////////////////
// Uses the java method 'ByteBuffer fetchNextBlock/fetchNextBlockFromVector()' 
//////////////////////////////////////////////////////////////////////////////
AFR_RetCode AvroFileReader::fetchNextRow(char** pv_buffer,
					long& pv_array_length,
					long& pv_rowNumber,
					int& pv_num_columns,
					ExHdfsScanStats *pv_hss
					)
{
  if (initJNIEnv() != JOI_OK)
     return AFR_ERROR_FETCHROW_EXCEPTION;

  tsRecentJMFromJNI = JavaMethods_[JM_FETCHBLOCK].jm_full_name;
  
  if (m_number_of_remaining_rows_in_block == 0) {
    
    Int64 currentTime = NA_JulianTimestamp();
    
    if (pv_hss) pv_hss->getHdfsTimer().start();
    
    m_total_number_of_rows_in_block = 0;
    jobject lv_java_block = (jobject)jenv_->CallObjectMethod
      (javaObj_,
       JavaMethods_[JM_FETCHBLOCK].methodID);
    if (pv_hss)	{
      pv_hss->incMaxHdfsIOTime(pv_hss->getHdfsTimer().stop());
      pv_hss->incHdfsCalls();
    }

    if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "AvroFileReader::fetchNextRow()");
      jenv_->PopLocalFrame(NULL);
      return AFR_ERROR_FETCHROW_EXCEPTION;
    }

    if (lv_java_block == NULL) {
      jenv_->PopLocalFrame(NULL);
      return (AFR_NOMORE);		//No more rows
    }

    m_block = (char *) jenv_->GetDirectBufferAddress(lv_java_block);
    if (m_block == NULL) {
      jenv_->DeleteLocalRef(lv_java_block);
      logError(CAT_SQL_HDFS_AVRO_FILE_READER,
	       "AvroFileReader::fetchNextRow() - ",
	       "NULL pointer returned by GetDirectBufferAddress()"
	       );
      jenv_->PopLocalFrame(NULL);
      return AFR_ERROR_FETCHROW_EXCEPTION;
    }

    jlong lv_block_capacity = jenv_->GetDirectBufferCapacity(lv_java_block);
    jenv_->DeleteLocalRef(lv_java_block);

    blocksReadTime_ +=  NA_JulianTimestamp() - currentTime;

    m_total_number_of_rows_in_block = *(int *) m_block;
    if (m_total_number_of_rows_in_block <= 0) {
      jenv_->PopLocalFrame(NULL);
      return (AFR_NOMORE);
    }
    
    m_block += sizeof(int);

    int lv_bytes_received = *(int *) m_block;
    m_block += sizeof(int);
    if (pv_hss) { 
      pv_hss->incAccessedRows(m_total_number_of_rows_in_block);
      pv_hss->incBytesRead(lv_bytes_received);
    }
    
    m_number_of_remaining_rows_in_block = m_total_number_of_rows_in_block;

    totalBlocksFetched_++;
  }
  
  fillNextRow(pv_buffer,
	      pv_array_length,
	      pv_rowNumber,
	      pv_num_columns);
  
  --m_number_of_remaining_rows_in_block;

  totalRowsRead_ ++;

  jenv_->PopLocalFrame(NULL);
  return (AFR_OK);
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
void AvroFileReader::fillNextRow(char**pv_buffer,
				long& pv_array_length,
				long& pv_rowNumber,
				int&  pv_num_columns)
{
  
  pv_array_length = (long) *(int *) m_block;
  m_block += sizeof(int);

  pv_num_columns = *(int *) m_block;
  m_block += sizeof(int);
  
  pv_rowNumber = *(long *) m_block;
  m_block += sizeof(long);
	
  //  memcpy(pv_buffer, m_block, pv_array_length);
  *pv_buffer = m_block;

  m_block += pv_array_length;

  return;
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////


AFR_RetCode AvroFileReader::close()
{
  Int64 timeDiff = NA_JulianTimestamp() - startTime_;

  QRLogger::log(CAT_SQL_HDFS_AVRO_FILE_READER,
		LL_DEBUG,
		"AvroFileReader::close(file=%s, records=%ld, blocks=%ld, blocksReadTimeA=%ld, blocksReadTimeB=%s, close_time=%s,  et=%s) called.", 
                path_.data(), totalRowsRead_, totalBlocksFetched_, 
                blocksReadTime_, reportTimeDiff(blocksReadTime_), 
                reportTimestamp(), reportTimeDiff(timeDiff));
  if (initJNIEnv() != JOI_OK)
     return AFR_ERROR_CLOSE_EXCEPTION;
  // String close();
  tsRecentJMFromJNI = JavaMethods_[JM_CLOSE].jm_full_name;
  jstring jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
						     JavaMethods_[JM_CLOSE].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "AvroFileReader::close()");
    jenv_->PopLocalFrame(NULL);
    return AFR_ERROR_CLOSE_EXCEPTION;
  }

  if (jresult!=NULL) {
    logError(CAT_SQL_HDFS_AVRO_FILE_READER,
	     "AvroFileReader::close()",
	     jresult);
    jenv_->PopLocalFrame(NULL);
    return AFR_ERROR_CLOSE_EXCEPTION;
  }
  
  jenv_->PopLocalFrame(NULL);
  return AFR_OK;

}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr> *AvroFileReader::getColStats(NAHeap *heap, 
                                                  int colNum, char * colName, int colType)
{
  QRLogger::log(CAT_SQL_HDFS_AVRO_FILE_READER,
		LL_DEBUG,
		"AvroFileReader::getColStats() called.");
    
  if (initJNIEnv() != JOI_OK)
     return NULL;

  jint      ji_colNum = colNum;
  jstring   js_colName = jenv_->NewStringUTF(colName);
  if (js_colName == NULL) {
     jenv_->PopLocalFrame(NULL);
     return NULL;
  }

  jint      ji_colType = colType;

  tsRecentJMFromJNI = JavaMethods_[JM_GETCOLSTATS].jm_full_name;
  jobject j_colStats = jenv_->CallObjectMethod
    (javaObj_,
     JavaMethods_[JM_GETCOLSTATS].methodID,
     ji_colNum,
     js_colName,
     ji_colType);
  
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "AvroFileReader::getColStats()");
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
AFR_RetCode AvroFileReader::fetchRowsIntoBuffer(Int64   stopOffset, 
					       char*   buffer, 
					       Int64   buffSize, 
					       Int64&  bytesRead, 
					       char    rowDelimiter)
{

  QRLogger::log(CAT_SQL_HDFS_AVRO_FILE_READER, 
		LL_DEBUG, 
		"AvroFileReader::fetchRowsIntoBuffer(stopOffset: %ld, buffSize: %ld) called.", 
		stopOffset, 
		buffSize);

  Int32 maxRowLength = 0;
  char* pos = buffer;
  Int64 limit = buffSize;
  AFR_RetCode retCode;
  long rowsRead=0;
  bytesRead = 0;
  do
  {
    //    retCode = fetchNextRow(row, stopOffset, pos);
    retCode = AFR_OK;
    if (retCode == AFR_OK)
    {
      rowsRead++;
      Int32 rowLength = strlen(pos);
      pos += rowLength;
      *pos = rowDelimiter;
      pos++;
      *pos = 0;
      
      bytesRead += rowLength+1;
      if (maxRowLength < rowLength)
        maxRowLength = rowLength;
      limit = buffSize - maxRowLength*2;
    }
  } while (retCode == AFR_OK && bytesRead < limit);
  
  QRLogger::log(CAT_SQL_HDFS_AVRO_FILE_READER, LL_DEBUG, "  =>Returning %d, read %ld bytes in %d rows.", retCode, bytesRead, rowsRead);
  return retCode;
}


AFR_RetCode 
AvroFileReader::getLongArray(AvroFileReader::JAVA_METHODS method, const char* msg, LIST(Int64)& resultArray)
{

  if (initJNIEnv() != JOI_OK)
     return AFR_ERROR_GETSTRIPEINFO_EXCEPTION;

  tsRecentJMFromJNI = JavaMethods_[method].jm_full_name;

  jlongArray jresult = (jlongArray)jenv_->CallObjectMethod(javaObj_, JavaMethods_[method].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "AvroFileReader::getLongArray()");
    jenv_->PopLocalFrame(NULL);
    return AFR_ERROR_GETSTRIPEINFO_EXCEPTION;
  }

  if (jresult == NULL) {
    logError(CAT_SQL_HDFS_AVRO_FILE_READER,
             msg,
             getLastError());
    jenv_->PopLocalFrame(NULL);
    return AFR_ERROR_GETSTRIPEINFO_EXCEPTION;
  }

  int numOffsets = convertLongObjectArrayToList(heap_, jresult, resultArray);

  jenv_->PopLocalFrame(NULL);
  return AFR_OK;
}

//////////////////////////////////////////////////////////////////////////////
// getFileStats 
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr>* AvroFileReader::getFileStats(NAHeap *heap, char * rootDir)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "AvroFileReader::getFileStats() called.");

  if (initJNIEnv() != JOI_OK)
     return NULL;

  jstring   js_rootDir = jenv_->NewStringUTF(rootDir);
  if (js_rootDir == NULL) {
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_GET_FILE_STATS].jm_full_name;
  jarray j_fileInfo = 
    (jarray)jenv_->CallObjectMethod(javaObj_, 
                                    JavaMethods_[JM_GET_FILE_STATS].methodID,
                                    js_rootDir);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "AvroFileReader::getFileStats()");
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  if (j_fileInfo == NULL) {
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  NAArray<HbaseStr> *fileInfo = NULL;
  jint retcode = convertByteArrayObjectArrayToNAArray(heap, j_fileInfo, &fileInfo);

  jenv_->PopLocalFrame(NULL);
  //  if (retcode == 0)
  if (fileInfo == NULL)
     return NULL;
  else
     return fileInfo;
}

//////////////////////////////////////////////////////////////////////////////
// getFileSchema
//////////////////////////////////////////////////////////////////////////////
AFR_RetCode AvroFileReader::getFileSchema(NAHeap *heap, 
                                          char * tableName,
                                          char * rootDir,
                                          TextVec * colNameVec, 
                                          TextVec * colTypeVec,
                                          Lng32 flags,
                                          char* &readSchema,
                                          char* &writeSchema)
{
  AFR_RetCode lv_retcode = AFR_OK;

  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "AvroFileReader::getFileSchema() called.");

  if (initJNIEnv() != JOI_OK)
     return AFR_ERROR_OPEN_PARAM;

  jobjectArray jor_colNameVec = NULL;
  jobjectArray jor_colTypeVec = NULL;
  jarray j_schemas = NULL;
  jstring js_rootDir = NULL;
  jint      ji_flags = flags;

  jstring js_tableName = jenv_->NewStringUTF(tableName);
  if (js_tableName == NULL) {
    lv_retcode = AFR_ERROR_OPEN_PARAM;
    goto fn_exit;
  }

  js_rootDir = jenv_->NewStringUTF(rootDir);
  if (js_rootDir == NULL) {
    lv_retcode = AFR_ERROR_OPEN_PARAM;
    goto fn_exit;
  }

  if (colNameVec && (!colNameVec->empty())) {
    jor_colNameVec = convertToStringObjectArray(*colNameVec);
    if (jor_colNameVec == NULL) {
      lv_retcode = AFR_ERROR_OPEN_PARAM;
      goto fn_exit;
    }
  }
  
  if (colTypeVec && (!colTypeVec->empty())) {
    jor_colTypeVec = convertToByteArrayObjectArray(*colTypeVec);
    if (jor_colTypeVec == NULL) {
      lv_retcode = AFR_ERROR_OPEN_PARAM;
      goto fn_exit;
    }
  }
  
  tsRecentJMFromJNI = JavaMethods_[JM_GET_FILE_SCHEMA].jm_full_name;
  j_schemas = 
    (jarray)jenv_->CallObjectMethod(javaObj_, 
                                    JavaMethods_[JM_GET_FILE_SCHEMA].methodID,
                                    js_tableName,
                                    js_rootDir,
                                    jor_colNameVec,
                                    jor_colTypeVec,
                                    ji_flags);
  
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "AvroFileReader::getFileSchema()");
    lv_retcode = AFR_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }

  readSchema = NULL;
  writeSchema = NULL;
  
  if (j_schemas == NULL) {
    jenv_->PopLocalFrame(NULL);
    return AFR_OK;
  }

  if (j_schemas != NULL) {
    NAArray<HbaseStr> *schemas = NULL;
    jint retcode = convertStringObjectArrayToNAArray(heap, j_schemas, 
                                                     &schemas);
    
    readSchema = schemas->at(0).val;
    writeSchema = schemas->at(1).val;
    
    jenv_->PopLocalFrame(NULL);
    return AFR_OK;
  }

fn_exit:  
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}
