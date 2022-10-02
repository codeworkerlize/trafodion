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
#ifndef ORC_FILE_READER_H
#define ORC_FILE_READER_H

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

namespace {
  typedef std::vector<Text> TextVec;
}

// ===========================================================================
// ===== The OrcFileReader class implements access to th Java 
// ===== OrcFileReader class.
// ===========================================================================

typedef enum {
  OFR_OK     = JOI_OK
 ,OFR_NOMORE = JOI_LAST         // OK, last row read.
 ,OFR_ERROR_OPEN_PARAM          // JNI NewStringUTF() in open()
 ,OFR_ERROR_OPEN_EXCEPTION      // Java exception in open()
 ,OFR_ERROR_FILE_NOT_FOUND_EXCEPTION // Jave exception in open()
 ,OFR_ERROR_GETPOS_EXCEPTION    // Java exception in getPos()
 ,OFR_ERROR_SYNC_EXCEPTION      // Java exception in seeknSync(
 ,OFR_ERROR_ISEOF_EXCEPTION     // Java exception in isEOF()
 ,OFR_ERROR_FETCHROW_EXCEPTION  // Java exception in fetchNextRow()
 ,OFR_ERROR_CLOSE_EXCEPTION     // Java exception in close()
 ,OFR_ERROR_GETSTRIPEINFO_EXCEPTION 
 ,OFR_ERROR_GETCOLSTATS_EXCEPTION 
 ,OFR_ERROR_GETSUMSTRINGLENGTHS_EXCEPTION
 ,OFR_ERROR_GSS_EXCEPTION       // Kerberos ticket not available
 ,OFR_UNKNOWN_ERROR
 ,OFR_ERROR_GETNUMROWS_EXCEPTION

 // The following are for reporting errors within the Arrow Reader 
 // or in the interface to it.
 ,OFR_ERROR_ARROW_READER_CONNECT  // Connect() error 
 ,OFR_ERROR_ARROW_READER_OPEN_READABLE // openReadable() 
 ,OFR_ERROR_ARROW_READER_OPEN_FILE   // openfile() error 
 ,OFR_ERROR_ARROW_READER_DISCONNECT  // Disconnect() error 
 ,OFR_ERROR_ARROW_READER_READTABLE // ReadTable() error 
 ,OFR_ERROR_ARROW_TRAFODION_CONVERSION // Conversion error (from arrow to trafodion)
 ,OFR_ERROR_ARROW_READER_PUSHDOWN_PRED // pushdown predicate error
 ,OFR_ERROR_ARROW_READER_GET_BATCH_READER // obtain a batch reader error 
 ,OFR_ERROR_ARROW_READER_READ_NEXT_BATCH //  read nexts batch error

 ,OFR_LAST
} OFR_RetCode;

// -----------------------------------------------------------------------
// Classes referenced in this file
// -----------------------------------------------------------------------

class OrcFileReader : public TrafExtStorageUtils
{
public:
  // Default constructor - for creating a new JVM		
  OrcFileReader(NAHeap *heap)
    :  TrafExtStorageUtils(heap, JavaMethods_)
    , startTime_(0)
    {}

  // Constructor for reusing an existing JVM.
  OrcFileReader(NAHeap *heap, JavaVM *jvm, JNIEnv *jenv)
    :  TrafExtStorageUtils(heap, jvm, jenv, JavaMethods_)
  {}

  // Destructor
  virtual ~OrcFileReader();
  
  // Initialize JVM and all the JNI configuration.
  // Must be called.
  JOI_RetCode    init();

  // Open the HDFS OrcFile 'path' for reading (returns all the columns)
  // offset                : offset to start scan
  // length                : scan upto offset + length 
  //  OFR_RetCode    open(const char* path, Int64 offset=0L, Int64 length=ULLONG_MAX);


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
  OFR_RetCode    open(const char * tableName,
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
  // Close the file.
  OFR_RetCode    close();

  NAArray<HbaseStr> *getColStats(NAHeap *heap, 
                                 int colNum, char * colName, int colType);

  // Get the sum of the lengths of the strings across all string columns
  OFR_RetCode    getSumStringLengths(Int64& result);

  static char*  getErrorText(OFR_RetCode errEnum);

  OFR_RetCode getStripeInfo(LIST(Int64)& numOfRowsInStripe,  
                            LIST(Int64)& offsetOfStripe,  
                            LIST(Int64)& totalBytesOfStripe
                            );

private:

  enum JAVA_METHODS {
    JM_OPEN = JM_LAST_TRAF_EXT_STORAGE_UTILS, 
    JM_GETCOLSTATS,
    JM_GETSUMSTRINGLENGTHS,
    JM_CLOSE,
    JM_GETSTRIPE_OFFSETS,
    JM_GETSTRIPE_LENGTHS,
    JM_GETSTRIPE_NUMROWS,
    JM_LAST
  };
 
  static jclass          javaClass_;
  static JavaMethodInit *JavaMethods_;
  static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;

private:
  OFR_RetCode getLongArray(JAVA_METHODS method, const char* msg, LIST(Int64)& resultArray);
  NAString path_;
  Int64    startTime_;
};


#endif
