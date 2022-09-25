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
#ifndef ORC_FILE_VECTOR_WRITER_H
#define ORC_FILE_VECTOR_WRITER_H

#include <list>
#include "Platform.h"
#include "Collections.h"
#include "NABasicObject.h"

#include "JavaObjectInterface.h"
#include "Hbase_types.h"
#include "ExpHbaseDefs.h"
#include "NAMemory.h"

using namespace apache::hadoop::hbase::thrift;
namespace {
  typedef std::vector<Text> TextVec;
}

// ===========================================================================
// ===== The OrcFileVectorWriter class implements access to the Java 
// ===== OrcFileVectorWriter class.
// ===========================================================================

typedef enum {
  OFW_OK     = JOI_OK
 ,OFW_NOMORE = JOI_LAST           // OK, last row read.
 ,OFW_ERROR_OPEN_PARAM            // JNI NewStringUTF() in open()
 ,OFW_ERROR_OPEN_EXCEPTION        // Java exception in open()
 ,OFW_ERROR_INSERTROWS_PARAM      // JNI NewStringUTF() in insertRows()
 ,OFW_ERROR_INSERTROWS_EXCEPTION  // Java exception in insertRows()
 ,OFW_ERROR_CLOSE_EXCEPTION       // Java exception in close()
 ,OFW_UNKNOWN_ERROR
 ,OFW_LAST
} OFW_RetCode;

class OrcFileVectorWriter : public JavaObjectInterface
{
public:
  // Default constructor - for creating a new JVM		
  OrcFileVectorWriter(NAHeap *heap)
    :  JavaObjectInterface(heap)
    {}

  // Constructor for reusing an existing JVM.
  OrcFileVectorWriter(NAHeap *heap, JavaVM *jvm, JNIEnv *jenv)
    :  JavaObjectInterface(heap)
  {}

  // Destructor
  virtual ~OrcFileVectorWriter();
  
  // Initialize JVM and all the JNI configuration.
  // Must be called.
  OFW_RetCode    init();

  OFW_RetCode    open(const char* path, 
                      TextVec * colNameList, 
                      TextVec * colTypeInfoList,
                      bool blockPadding,
                      Int64 stripeSize,
                      Int32 bufferSize,
                      Int32 rowIndexStride,
                      const char * compression,
                      int flags = 0);
 
  OFW_RetCode    close();

  OFW_RetCode insertRows(char * directBuffer,
                         Lng32 directBufferMaxLen, 
                         Lng32 numRowsInBuffer,
                         Lng32 directBufferCurrLen);

  static char*  getErrorText(OFW_RetCode errEnum);

private:
  enum JAVA_METHODS {
    JM_CTOR = 0, 
    JM_OPEN,
    JM_INSERTROWS,
    JM_CLOSE,
    JM_LAST
  };
 
  static jclass          javaClass_;
  static JavaMethodInit *JavaMethods_;
  static bool javaMethodsInitialized_;
  // this mutex protects both JaveMethods_ and javaClass_ initialization
  static pthread_mutex_t javaMethodsInitMutex_;

private:
  OFW_RetCode getLongArray(JAVA_METHODS method, const char* msg, 
                           LIST(Int64)& resultArray);
};


#endif
