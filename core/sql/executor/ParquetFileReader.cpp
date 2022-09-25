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
#include "ParquetFileReader.h"
#include "QRLogger.h"
#include "HBaseClient_JNI.h"
#include "ttime.h"
#include "ComCextdecs.h"

// ===========================================================================
// ===== Class ParquetFileReader
// ===========================================================================

JavaMethodInit* ParquetFileReader::JavaMethods_ = NULL;
jclass ParquetFileReader::javaClass_ = 0;
bool ParquetFileReader::javaMethodsInitialized_ = false;
pthread_mutex_t ParquetFileReader::javaMethodsInitMutex_ = PTHREAD_MUTEX_INITIALIZER;

static const char* const pfrErrorEnumStr[] = 
{
  "No more data."
 ,"JNI NewStringUTF() in initSerDe()"
 ,"Java exception in initSerDe()"
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
 ,"Unknown error returned from Parquet interface"
};

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
char* ParquetFileReader::getErrorText(PFR_RetCode pv_errEnum)
{
  if (pv_errEnum < (PFR_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)pv_errEnum);
  else    
    return (char*)pfrErrorEnumStr[pv_errEnum - JOI_LAST];
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
ParquetFileReader::~ParquetFileReader()
{
  close();
  releaseJavaAllocation();
}
 
//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
JOI_RetCode ParquetFileReader::init()
{
  static char className[]="org/trafodion/sql/TrafParquetFileReader";

  JOI_RetCode lv_retcode = JOI_OK;
 
  QRLogger::log(CAT_SQL_HDFS_PARQUET_FILE_READER,
		LL_DEBUG,
		"Enter ParquetFileReader::init()");
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
    JavaMethods_[JM_OPEN      ].jm_signature = "(Ljava/lang/String;Ljava/lang/String;JJI[IIII[Ljava/lang/Object;[Ljava/lang/Object;[Ljava/lang/Object;[Ljava/lang/Object;Ljava/lang/String;I)Ljava/lang/String;";

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

    JavaMethods_[JM_GET_FILE_AND_BLOCK_ATTRIBUTES].jm_name      = "getFileAndBlockAttributes";
    JavaMethods_[JM_GET_FILE_AND_BLOCK_ATTRIBUTES].jm_signature = "(Ljava/lang/String;)[[B";

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
PFR_RetCode ParquetFileReader::open(const char *pv_name,
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
                                    const char *parqSchStr,
                                    int flags)
{
  QRLogger::log(CAT_SQL_HDFS_PARQUET_FILE_READER,
		LL_DEBUG,
		"ParquetFileReader::open(start_time=%s, %s, %d, %p, max_row: %d, max_allocation: %d) called.",
		reportTimestamp(),
		pv_path,
		pv_num_cols_in_projection,
		pv_which_cols,
		max_rows_to_fill,
		max_allocation_in_mb);

  if (initJNIEnv() != JOI_OK)
    return PFR_ERROR_OPEN_PARAM;

  static int sv_max_rows_to_fill = 1600;
  static bool sv_envvar_read = false;
  static int sv_max_allocation_in_mb = 32;
  if ( ! sv_envvar_read ) {
    sv_envvar_read = true;
    char* lv_env = 0;
    lv_env = getenv("PARQUET_MAX_ROWS_TO_FILL");
    if (lv_env) {
      int lv_max_rows_to_fill = atoi(lv_env);
      if ((lv_max_rows_to_fill > 0) &&
	  (lv_max_rows_to_fill < (32 * 1024 * 1024))) {
	sv_max_rows_to_fill = lv_max_rows_to_fill;
      }
    }

    lv_env = 0;
    lv_env = getenv("PARQUET_MAX_ALLOCATION_IN_MB");
    if (lv_env) {
      int lv_max_allocation_in_mb = atoi(lv_env);
      if ((lv_max_allocation_in_mb > 0) &&
	  (lv_max_allocation_in_mb < (32 * 1024 * 1024))) {
	sv_max_allocation_in_mb = lv_max_allocation_in_mb;
      }
    }
  }

  startTime_ = NA_JulianTimestamp();

  totalRowsRead_ = 0;
  totalBlocksFetched_ = 0;

  blocksReadTime_ = 0;

  PFR_RetCode lv_retcode = PFR_OK;

  jstring   js_name = NULL;
  jstring   js_path = NULL;
  jintArray jia_which_cols = NULL;
  jint      ji_num_cols_in_projection = pv_num_cols_in_projection;
  jlong     jl_offset = offset;
  jlong     jl_length = length;
  jint      ji_expected_row_size = expected_row_size;
  jint      ji_max_rows_to_fill = sv_max_rows_to_fill;
  jint      ji_max_allocation_in_mb = sv_max_allocation_in_mb;
  jobject   jo_ppiBuf = NULL;
  jstring   jresult = NULL;
  jobjectArray jor_ppiVec = NULL;
  jobjectArray jor_ppiAllCols = NULL;
  jobjectArray jor_colNameVec = NULL;
  jobjectArray jor_colTypeVec = NULL;
  jstring      js_parqSchStr = NULL;
  jint      ji_flags = flags;

  releaseJavaAllocation();

  if (pv_name != NULL) {
    js_name = jenv_->NewStringUTF(pv_name);
    if (js_name == NULL) {
      lv_retcode = PFR_ERROR_OPEN_PARAM;
      goto fn_exit;
    }
  }

  js_path = jenv_->NewStringUTF(pv_path);
  if (js_path == NULL) {
    lv_retcode = PFR_ERROR_OPEN_PARAM;
    goto fn_exit;
  }
  
  if ((pv_num_cols_in_projection > 0) && 
      (pv_which_cols != 0)) {
    jia_which_cols = jenv_->NewIntArray(pv_num_cols_in_projection);
    if (jia_which_cols == NULL) {


      QRLogger::log(CAT_SQL_HDFS_PARQUET_FILE_READER,
		    LL_ERROR,
		    "ParquetFileReader::open(%s, %d, %p). Error while allocating memory for j_col_array",
		    pv_path,
		    pv_num_cols_in_projection,
		    pv_which_cols);

      
      lv_retcode = PFR_ERROR_OPEN_PARAM;
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
        lv_retcode = PFR_ERROR_OPEN_PARAM;
	goto fn_exit;
      }

      if (ppiAllCols && (!ppiAllCols->empty()))
        {
          QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "  Adding %d cols.", 
                        ppiAllCols->size());
          jor_ppiAllCols = convertToStringObjectArray(*ppiAllCols);
          if (jor_ppiAllCols == NULL)
            {
              getExceptionDetails(__FILE__, __LINE__, "ParquetFileReader::open()");
              lv_retcode = PFR_ERROR_OPEN_PARAM;
	      goto fn_exit;
            }
        }
    }

  if (colNameVec && (!colNameVec->empty()))
    {
      jor_colNameVec = convertToStringObjectArray(*colNameVec);
      if (jor_colNameVec == NULL) {
        lv_retcode = PFR_ERROR_OPEN_PARAM;
	goto fn_exit;
      }
    }

  if (colTypeVec && (!colTypeVec->empty()))
    {
      jor_colTypeVec = convertToByteArrayObjectArray(*colTypeVec);
      if (jor_colTypeVec == NULL) {
        lv_retcode = PFR_ERROR_OPEN_PARAM;
	goto fn_exit;
      }
    }

  if (parqSchStr != NULL) {
    js_parqSchStr = jenv_->NewStringUTF(parqSchStr);
    if (js_parqSchStr == NULL) {
      lv_retcode = PFR_ERROR_OPEN_PARAM;
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
                                             js_parqSchStr,
                                             ji_flags);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "ParquetFileReader::open");
    lv_retcode = PFR_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }

  if (jresult != NULL) {
    const char *my_string = jenv_->GetStringUTFChars(jresult, JNI_FALSE);
    QRLogger::log(CAT_SQL_HDFS_PARQUET_FILE_READER,
		  LL_DEBUG,
		  "ParquetFileReader::open(%s), error:%s",
		  pv_path,
		  my_string);
    logError(CAT_SQL_HDFS_PARQUET_FILE_READER,
	     "ParquetFileReader::open()",
	     jresult);
    if (strcmp(my_string, "file not found") == 0)
      lv_retcode = PFR_ERROR_FILE_NOT_FOUND_EXCEPTION;
    else
      lv_retcode = PFR_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }

 fn_exit:  
  path_ = pv_path;

  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

void ParquetFileReader::releaseJavaAllocation() {

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
PFR_RetCode ParquetFileReader::fetchNextRow(char** pv_buffer,
					long& pv_array_length,
					long& pv_rowNumber,
					int& pv_num_columns,
					ExHdfsScanStats *pv_hss
					)
{
  static int  sv_java_fetch_next_row_method = JM_FETCHBLOCK;

  PFR_RetCode lv_retcode = PFR_OK;

  tsRecentJMFromJNI = JavaMethods_[sv_java_fetch_next_row_method].jm_full_name;
  
  if (m_number_of_remaining_rows_in_block == 0) {

    Int64 currentTime = NA_JulianTimestamp();
    
   if (pv_hss) pv_hss->getHdfsTimer().start();

   if (initJNIEnv() != JOI_OK)
     return PFR_ERROR_OPEN_PARAM;

    m_total_number_of_rows_in_block = 0;
    jobject lv_java_block = (jobject)jenv_->CallObjectMethod(javaObj_,
						    JavaMethods_[sv_java_fetch_next_row_method].methodID);
    if (pv_hss)	{
      pv_hss->incMaxHdfsIOTime(pv_hss->getHdfsTimer().stop());
      pv_hss->incHdfsCalls();
    }

    if (jenv_->ExceptionCheck()) {
      getExceptionDetails(__FILE__, __LINE__, "ParquetFileReader::fetchNewRow()");
      lv_retcode = PFR_ERROR_FETCHROW_EXCEPTION;
      goto fn_exit;
    }

    if (lv_java_block == NULL) {
      logError(CAT_SQL_HDFS_PARQUET_FILE_READER,
	       "ParquetFileReader::fetchNextRow()",
	       getLastError());
      lv_retcode = PFR_ERROR_FETCHROW_EXCEPTION;
      goto fn_exit;
    }

    if (lv_java_block == NULL) {
      lv_retcode = (PFR_NOMORE);		//No more rows
      goto fn_exit;
    }

    m_block = (char *) jenv_->GetDirectBufferAddress(lv_java_block);
    if (m_block == NULL) {
      jenv_->DeleteLocalRef(lv_java_block);
      logError(CAT_SQL_HDFS_PARQUET_FILE_READER,
	       "ParquetFileReader::fetchNextRow() - ",
	       "NULL pointer returned by GetDirectBufferAddress()"
	       );
      lv_retcode = PFR_ERROR_FETCHROW_EXCEPTION;
      goto fn_exit;
    }

    //    jlong lv_block_capacity = jenv_->GetDirectBufferCapacity(lv_java_block);
    jenv_->DeleteLocalRef(lv_java_block);

    blocksReadTime_ +=  NA_JulianTimestamp() - currentTime;

    m_total_number_of_rows_in_block = *(int *) m_block;
    if (m_total_number_of_rows_in_block <= 0) {
      lv_retcode = (PFR_NOMORE);
      goto fn_exit;
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

 fn_exit:
  jenv_->PopLocalFrame(NULL);
  return (lv_retcode);
}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
void ParquetFileReader::fillNextRow(char**pv_buffer,
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


PFR_RetCode ParquetFileReader::close()
{
  Int64 timeDiff = NA_JulianTimestamp() - startTime_;
  PFR_RetCode lv_retcode = PFR_OK;

  if (initJNIEnv() != JOI_OK)
    return PFR_ERROR_OPEN_PARAM;

  QRLogger::log(CAT_SQL_HDFS_PARQUET_FILE_READER,
		LL_DEBUG,
		"ParquetFileReader::close(file=%s, records=%ld, blocks=%ld, blocksReadTimeA=%ld, blocksReadTimeB=%s, close_time=%s,  et=%s) called.", 
                path_.data(), totalRowsRead_, totalBlocksFetched_, 
                blocksReadTime_, reportTimeDiff(blocksReadTime_), 
                reportTimestamp(), reportTimeDiff(timeDiff));

  // String close();
  tsRecentJMFromJNI = JavaMethods_[JM_CLOSE].jm_full_name;
  jstring jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
						     JavaMethods_[JM_CLOSE].methodID);

/*
  QRLogger::log(CAT_SQL_HDFS_PARQUET_FILE_READER,
		LL_DEBUG,
		"ParquetFileReader::close(file=%s, close_time=%s,  et=%s) done.", 
                path_.data(), reportTimestamp(), reportTimeDiff(timeDiff));
*/

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "ParquetFileReader::close()");
    lv_retcode = PFR_ERROR_CLOSE_EXCEPTION;
    goto fn_exit;
  }

  if (jresult!=NULL) {
    logError(CAT_SQL_HDFS_PARQUET_FILE_READER,
	     "ParquetFileReader::close()",
	     jresult);
    lv_retcode = PFR_ERROR_CLOSE_EXCEPTION;
    goto fn_exit;
  }
  
 fn_exit:
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;

}

//////////////////////////////////////////////////////////////////////////////
// 
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr> *ParquetFileReader::getColStats(NAHeap *heap, 
                                                  int colNum, char * colName, int colType)
{
  PFR_RetCode lv_retcode = PFR_OK;

  QRLogger::log(CAT_SQL_HDFS_PARQUET_FILE_READER,
		LL_DEBUG,
		"ParquetFileReader::getColStats() called.");

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
    getExceptionDetails(__FILE__, __LINE__, "ParquetFileReader::getColStats()");
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
PFR_RetCode ParquetFileReader::fetchRowsIntoBuffer(Int64   stopOffset, 
					       char*   buffer, 
					       Int64   buffSize, 
					       Int64&  bytesRead, 
					       char    rowDelimiter)
{

  QRLogger::log(CAT_SQL_HDFS_PARQUET_FILE_READER, 
		LL_DEBUG, 
		"ParquetFileReader::fetchRowsIntoBuffer(stopOffset: %ld, buffSize: %ld) called.", 
		stopOffset, 
		buffSize);

  Int32 maxRowLength = 0;
  char* pos = buffer;
  Int64 limit = buffSize;
  PFR_RetCode retCode;
  long rowsRead=0;
  bytesRead = 0;
  do
  {
    //    retCode = fetchNextRow(row, stopOffset, pos);
    retCode = PFR_OK;
    if (retCode == PFR_OK)
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
  } while (retCode == PFR_OK && bytesRead < limit);
  
  QRLogger::log(CAT_SQL_HDFS_PARQUET_FILE_READER, LL_DEBUG, "  =>Returning %d, read %ld bytes in %d rows.", retCode, bytesRead, rowsRead);
  return retCode;
}


PFR_RetCode 
ParquetFileReader::getLongArray(ParquetFileReader::JAVA_METHODS method, const char* msg, LIST(Int64)& resultArray)
{
  PFR_RetCode lv_retcode = PFR_OK;
  tsRecentJMFromJNI = JavaMethods_[method].jm_full_name;

  if (initJNIEnv() != JOI_OK)
    return PFR_ERROR_OPEN_PARAM;

  jlongArray jresult = (jlongArray)jenv_->CallObjectMethod(javaObj_, JavaMethods_[method].methodID);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "ParquetFileReader::getLongArray()");
    jenv_->PopLocalFrame(NULL);
    return PFR_ERROR_GETSTRIPEINFO_EXCEPTION;
  }

  if (jresult == NULL) {
    logError(CAT_SQL_HDFS_PARQUET_FILE_READER,
             msg,
             getLastError());
    jenv_->PopLocalFrame(NULL);
    return PFR_ERROR_GETSTRIPEINFO_EXCEPTION;
  }

  int numOffsets = convertLongObjectArrayToList(heap_, jresult, resultArray);

 fn_exit:
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

//////////////////////////////////////////////////////////////////////////////
// getFileStats 
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr>* ParquetFileReader::getFileStats(NAHeap *heap, char * rootDir)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "ParquetFileReader::getFileStats() called.");

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
    getExceptionDetails(__FILE__, __LINE__, "ParquetFileReader::getFileStats()");
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
// getFileAndBlockAttributes 
//////////////////////////////////////////////////////////////////////////////
NAArray<HbaseStr>* 
ParquetFileReader::getFileAndBlockAttributes(NAHeap *heap, char * rootDir)
{
  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "ParquetFileReader::getFileAndBlockAttributes() called.");
  if (initJNIEnv() != JOI_OK)
    return NULL;

  jstring   js_rootDir = jenv_->NewStringUTF(rootDir);
  if (js_rootDir == NULL) {
    jenv_->PopLocalFrame(NULL);
    return NULL;
  }

  tsRecentJMFromJNI = JavaMethods_[JM_GET_FILE_AND_BLOCK_ATTRIBUTES].jm_full_name;
  jarray j_fileInfo = 
    (jarray)jenv_->CallObjectMethod(javaObj_, 
                                    JavaMethods_[JM_GET_FILE_AND_BLOCK_ATTRIBUTES].methodID,
                                    js_rootDir);

  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__, __LINE__, "ParquetFileReader::getFileAndBlockAttributes()");
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
PFR_RetCode ParquetFileReader::getFileSchema(NAHeap *heap, 
                                             char * tableName,
                                             char * rootDir,
                                             TextVec * colNameVec, 
                                             TextVec * colTypeVec,
                                             Lng32 flags,
                                             char* &readSchema,
                                             char* &writeSchema)
{
  PFR_RetCode lv_retcode = PFR_OK;

  QRLogger::log(CAT_SQL_HBASE, LL_DEBUG, "ParquetFileReader::getFileSchema() called.");

  if (initJNIEnv() != JOI_OK)
    return PFR_ERROR_OPEN_PARAM;

  jobjectArray jor_colNameVec = NULL;
  jobjectArray jor_colTypeVec = NULL;
  jarray j_schemas = NULL;
  jstring js_rootDir = NULL;
  jint      ji_flags = flags;

  jstring js_tableName = jenv_->NewStringUTF(tableName);
  if (js_tableName == NULL) {
    lv_retcode = PFR_ERROR_OPEN_PARAM;
    goto fn_exit;
  }

  js_rootDir = jenv_->NewStringUTF(rootDir);
  if (js_rootDir == NULL) {
    lv_retcode = PFR_ERROR_OPEN_PARAM;
    goto fn_exit;
  }

  if (colNameVec && (!colNameVec->empty())) {
    jor_colNameVec = convertToStringObjectArray(*colNameVec);
    if (jor_colNameVec == NULL) {
      lv_retcode = PFR_ERROR_OPEN_PARAM;
      goto fn_exit;
    }
  }
  
  if (colTypeVec && (!colTypeVec->empty())) {
    jor_colTypeVec = convertToByteArrayObjectArray(*colTypeVec);
    if (jor_colTypeVec == NULL) {
      lv_retcode = PFR_ERROR_OPEN_PARAM;
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
  
  if (jenv_->ExceptionCheck()) {
    getExceptionDetails(__FILE__, __LINE__, "ParquetFileReader::getFileSchema()");
    lv_retcode = PFR_ERROR_OPEN_EXCEPTION;
    goto fn_exit;
  }

  readSchema = NULL;
  writeSchema = NULL;
  
  if (j_schemas == NULL) {
    jenv_->PopLocalFrame(NULL);
    return PFR_OK;
  }
  
  if (j_schemas != NULL) {
    NAArray<HbaseStr> *schemas = NULL;
    jint retcode = convertStringObjectArrayToNAArray(heap, j_schemas, 
                                                     &schemas);
    
    readSchema = schemas->at(0).val;
    writeSchema = schemas->at(1).val;
    
    jenv_->PopLocalFrame(NULL);
    return PFR_OK;
  }

fn_exit:  
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}
