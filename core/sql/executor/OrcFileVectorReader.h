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
#ifndef ORC_FILE_VECTOR_READER_H
#define ORC_FILE_VECTOR_READER_H

#include <list>
#include "Platform.h"
#include "Collections.h"
#include "NABasicObject.h"
#include "NAStringDef.h"
#include "TrafExtStorageUtils.h"
#include "JavaObjectInterface.h"
#include "ExStats.h"
#include "ExpHbaseDefs.h"
#include "NAMemory.h"
#include "ExStats.h"

// ===========================================================================
// ===== The OrcFileVectorReader class implements access to th Java 
// ===== OrcFileVectorReader class.
// ===========================================================================

typedef enum {
  OFVR_OK     = JOI_OK
 ,OFVR_NOMORE = JOI_LAST         // OK, last row read.
 ,OFVR_ERROR_OPEN_PARAM          // JNI NewStringUTF() in open()
 ,OFVR_ERROR_OPEN_EXCEPTION      // Java exception in open()
 ,OFVR_ERROR_FETCH_NEXTBATCH_EXCEPTION  // Java exception in fetchNextRow()
 ,OFVR_ERROR_CLOSE_EXCEPTION     // Java exception in close()
 ,OFVR_ERROR_INCOMPATIABLE_COL_CONVERSION
 ,OFVR_ERROR_COL_VECTOR_NULL
 ,OFVR_ERROR_COL_VECTOR_DATA_COPY_ERROR
 ,OFVR_ERROR_COL_VECTOR_EXT1_NULL
 ,OFVR_ERROR_COL_VECTOR_EXT2_NULL
 ,OFVR_ERROR_COL_NULL_VAL_NULL
 ,OFVR_ERROR_BIG_NUM_BUFFER_OVERFLOW
 ,OFVR_LAST
} OFVR_RetCode;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------

class OrcFileVectorReader : public TrafExtStorageUtils
{
public:
  // Default constructor - for creating a new JVM		
  OrcFileVectorReader(NAHeap *heap)
    :  TrafExtStorageUtils(heap, JavaMethods_)
  { 
     initMembers();
  }

  // Constructor for reusing an existing JVM.
  OrcFileVectorReader(NAHeap *heap, JavaVM *jvm, JNIEnv *jenv)
    :  TrafExtStorageUtils(heap, jvm, jenv, JavaMethods_)
  {
     initMembers();
  }

  // Destructor
  virtual ~OrcFileVectorReader();
 
  void initMembers()
  {
     path_ = NULL;
     qualifyingRowCount_ = 0;
     useUTCTimestamp_ = FALSE;
     colCount_ = 0;
     p_colIds_ = NULL;
     p_colTypeOrdinals_ = NULL;
     p_colNoNulls_ = NULL;
     p_isRepeating_ = NULL;
     vectorSize_ = 0;
     jColValArrays_ = NULL;
     jColValExtArrays1_ = NULL;
     jColValExtArrays2_ = NULL;
     jColNullValArrays_ = NULL;
     jColIds_ = NULL;
     jColTypeOrdinals_ = NULL;
     jColNoNulls_ = NULL;
     jIsRepeatable_ = NULL;
     currRowNum_ = -1;
     cleanupDone_ = TRUE;
     isEOR_ = FALSE;
     hdfsStats_ = NULL; 
     p_colValArrays_ = NULL; 
     p_colValExtArrays1_ = NULL;
     p_colValExtArrays2_ = NULL;
     p_colNullVals_ = NULL;
     jColVector_ = NULL;
     jColVectorExt1_ = NULL;
     jColVectorExt2_ = NULL; 
     jColNullVals_ = NULL;
  }
  
  // Initialize JVM and all the JNI configuration.
  // Must be called.
  JOI_RetCode    init();

  // Open the HDFS OrcFileVector 'path' for reading (returns all the columns)
  // offset                : offset to start scan
  // length                : scan upto offset + length 
  //  OFVR_RetCode    open(const char* path, Int64 offset=0L, Int64 length=ULLONG_MAX);


  /*******
   * Open the HDFS OrcFileVector 'path' for reading.
   *
   * path                  : HDFS OrcFileVector path
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
  OFVR_RetCode    open(ExHdfsScanStats *hdfsStats, const char * tableName,
                      const char* path,
                      const int vector_size,
                      Int64 offset=0L, Int64 length=ULLONG_MAX, 
                      int num_cols_in_projection=-1, 
                      int *which_cols=NULL,
                      TextVec *ppiVec=NULL,
                      TextVec *ppiAllCols=NULL,
                      TextVec *colNameVec=NULL,
                      TextVec *colTypeVec=NULL);
  
  // Fetch the next row as a raw string into 'buffer'.
  OFVR_RetCode fetchNextBatch();
  OFVR_RetCode nextRow();
  // Close the file.
  OFVR_RetCode    close();
  
  static char*  getErrorText(OFVR_RetCode errEnum);
  void setResultInfo(JNIEnv *jenv, jobject jobj, jboolean useUTCTimestmap, jint colCount, jintArray colIds, jintArray colTypeOrdinal, 
                    jint vectorSize, jobjectArray colValArrays, jobjectArray colValExtArrays1, jobjectArray colValExtArrays2,
                     jbooleanArray noNullsArray, jobjectArray nullValArrays, jbooleanArray isRepeating);
  void cleanupResultInfo();
  OFVR_RetCode getBytesCol(int colNo, BYTE *colVal, Lng32 &colValLen, short &nullVal);
  OFVR_RetCode getNumericCol(int colNo, Int16 dataType, BYTE *colVal,
                                        Lng32 &colValLen, short &nullVal);
  OFVR_RetCode getDecimalCol(int colNo, Int16 dataType, BYTE *colVal,
                                        Lng32 &colValLen, short &nullVal, char *tempStrVal, Lng32 &tempStrValLen, bool &convFromStr);
  OFVR_RetCode getDoubleOrFloatCol(int colNo, Int16 dataType, BYTE *colVal,
                                        Lng32 &colValLen, short &nullVal);
  OFVR_RetCode getDateOrTimestampCol(int colNo, Int16 datetimeCode, BYTE *colVal,
                                        Lng32 &colValLen, short &nullVal);
private:
  OFVR_RetCode getNullValue(int colNo, short &nullVal);
  enum JAVA_METHODS {
    JM_OPEN = JM_LAST_TRAF_EXT_STORAGE_UTILS, 
    JM_FETCH_NEXT_BATCH,
    JM_CLOSE,
    JM_LAST
  };
 
   static jclass          javaClass_;
   static JavaMethodInit *JavaMethods_;
   static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
   static pthread_mutex_t javaMethodsInitMutex_;
private:
   OFVR_RetCode getLongArray(JAVA_METHODS method, const char* msg, LIST(Int64)& resultArray);
   char *path_;
   Int64 qualifyingRowCount_;
   jboolean useUTCTimestamp_;
   int colCount_;
   int *p_colIds_;
   int *p_colTypeOrdinals_;
   jboolean *p_colNoNulls_;
   jboolean *p_isRepeating_;
   int vectorSize_;
   jobjectArray jColValArrays_;
   jobjectArray jColValExtArrays1_;
   jobjectArray jColValExtArrays2_;
   jobjectArray jColNullValArrays_;
   jintArray jColIds_;
   jintArray jColTypeOrdinals_;
   jbooleanArray jColNoNulls_;
   jbooleanArray jIsRepeatable_;
   int currRowNum_;
   NABoolean cleanupDone_;
   NABoolean isEOR_;
   ExHdfsScanStats *hdfsStats_;
   // Array containing the pointers to jColValArrays_, jColValExtArrays1_, jColValExtArrays2
   // If these array belong to primitive data type, the whole array is read into native layer
   jlong **p_colValArrays_; 
   jint **p_colValExtArrays1_;
   jint **p_colValExtArrays2_;
   jboolean **p_colNullVals_;
   jlongArray *jColVector_;
   jintArray *jColVectorExt1_;
   jintArray *jColVectorExt2_; 
   jbooleanArray *jColNullVals_;
};
#endif
