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
#ifndef AVRO_FILE_READER_H
#define AVRO_FILE_READER_H

#include <list>
#include "Platform.h"
#include "Collections.h"
#include "NABasicObject.h"
#include "NAStringDef.h"
#include "TrafExtStorageUtils.h"
#include "JavaObjectInterface.h"
#include "Hbase_types.h"
#include "ExpHbaseDefs.h"
#include "NAMemory.h"

// ===========================================================================
// ===== The AvroFileReader class implements access to the Java 
// ===== AvroFileReader class.
// ===========================================================================

typedef enum {
  AFR_OK     = JOI_OK
 ,AFR_NOMORE = JOI_LAST         // OK, last row read.
 ,AFR_ERROR_OPEN_PARAM          // JNI NewStringUTF() in open()
 ,AFR_ERROR_OPEN_EXCEPTION      // Java exception in open()
 ,AFR_ERROR_FETCHROW_EXCEPTION  // Java exception in fetchNextRow()
 ,AFR_ERROR_CLOSE_EXCEPTION     // Java exception in close()
 ,AFR_ERROR_GETSTRIPEINFO_EXCEPTION
 ,AFR_ERROR_FILE_NOT_FOUND_EXCEPTION
 ,AFR_UNKNOWN_ERROR
 ,AFR_LAST
} AFR_RetCode;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------

class AvroFileReader : public TrafExtStorageUtils
{
public:
  // Default constructor - for creating a new JVM		
  AvroFileReader(NAHeap *heap)
    :  TrafExtStorageUtils(heap, JavaMethods_)
    , m_total_number_of_rows_in_block(0)
    , m_number_of_remaining_rows_in_block(0)
    , m_block(0)
    , m_java_block(0)
    , m_java_ba(0)
    , m_java_ba_released(true)
    , startTime_(0)
    , blocksReadTime_(0)
    , totalRowsRead_(0)
    , totalBlocksFetched_(0)
    {}

  // Constructor for reusing an existing JVM.
  AvroFileReader(NAHeap *heap, JavaVM *jvm, JNIEnv *jenv)
    :  TrafExtStorageUtils(heap, jvm, jenv, JavaMethods_)
  {}

  // Destructor
  virtual ~AvroFileReader();
  
  // Initialize JVM and all the JNI configuration.
  // Must be called.
  JOI_RetCode    init();


  /*******
   * Open the HDFS OrcFile 'path' for reading.
   *
   * path                  : HDFS OrcFile path
   *
   * expected_row_size     : an estimate of the expected size of a row in bytes
   *
   * max_rows_to_fill      : max number of rows to return on a block fetch
   *
   * max_allocation_in_mb  : max allocation for buffer to read rows in MB
   *
   * offset                : offset to start scan
   *
   * length                : scan upto offset + length 
   *
   * num_cols_to_project   : The number of columns to be returned 
   *                         set it to -1 to get all the columns
   *
   * which_cols            : array containing the column numbers to be returned
   *                         (Column numbers are one based)
   *
   * ppiBuflen:   length of buffer containing PPI (pred pushdown info)
   * ppiBuf:      buffer containing PPI
   * Format of data in ppiBuf:
   *   <numElems><type><nameLen><name><numOpers><opValLen><opVal>... 
   *    4-bytes    4B     4B      nlB     4B         4B      ovl B
   * ppiAllCols:  list of all columns. Used by ORC during pred evaluation.
   *******/
  AFR_RetCode    open(const char * tableName,
                      const char* path,
                      const int expected_row_size,
                      const int max_rows_to_fill,
                      const int max_allocation_in_mb,
                      Int64 offset=0L, Int64 length=ULLONG_MAX, 
                      int num_cols_in_projection=-1, 
                      int *which_cols=NULL,
                      TextVec *ppiVec=NULL,
                      TextVec *ppiAllCols=NULL,
                      TextVec *colNameVec=NULL,
                      TextVec *colTypeVec=NULL,
                      int flags=0);

  // Fetch the next row as a raw string into 'buffer'.
  AFR_RetCode fetchNextRow(char** buffer, 
			   long& array_length, 
			   long& rowNumber, 
			   int& num_columns,
			   ExHdfsScanStats *pv_hss = 0
			   );
  
  // Close the file.
  AFR_RetCode    close();

  AFR_RetCode    fetchRowsIntoBuffer(Int64 stopOffset, char* buffer, Int64 buffSize, Int64& bytesRead, char rowDelimiter);

  NAArray<HbaseStr> *getColStats(NAHeap *heap, 
                                 int colNum, char * colName, int colType);
                                 
  static char*  getErrorText(AFR_RetCode errEnum);

  NAArray<HbaseStr>* getFileStats(NAHeap *heap, char * rootDir);

  AFR_RetCode getFileSchema(NAHeap *heap, 
                            char * tableName,
                            char * rootDir, 
                            TextVec * colNameList, 
                            TextVec * colTypeInfoList,
                            Lng32 flags,
                            char* &readSchema,
                            char* &writeSchema);
private:
  void fillNextRow(char**pv_buffer,
		   long& pv_array_length,
		   long& pv_rowNumber,
		   int&  pv_num_columns);
  void releaseJavaAllocation();

  int   m_total_number_of_rows_in_block;
  int   m_number_of_remaining_rows_in_block;
  char *m_block;

  jbyteArray m_java_block;
  jbyte     *m_java_ba;
  bool       m_java_ba_released;

  enum JAVA_METHODS {
    JM_OPEN = JM_LAST_TRAF_EXT_STORAGE_UTILS, 
    JM_FETCHBLOCK,
    JM_FETCHROW,
    JM_GETCOLSTATS,
    JM_GET_FILE_STATS,
    JM_GET_FILE_SCHEMA,
    JM_CLOSE,
    JM_LAST
  };
 
  static jclass          javaClass_;
  static JavaMethodInit *JavaMethods_;
  static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;


private:
  AFR_RetCode getLongArray(JAVA_METHODS method, const char* msg, LIST(Int64)& resultArray);

  Int64 startTime_;
  Int64 blocksReadTime_;
  NAString path_;

  Int64 totalRowsRead_;
  Int64 totalBlocksFetched_;
};


#endif
