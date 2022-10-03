/**********************************************************************
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
**********************************************************************/

#ifndef __EX_FAST_TRANSPORT_H
#define __EX_FAST_TRANSPORT_H

#include "ex_tcb.h"
#include "ComSmallDefs.h"
#include "ExStats.h"

#include "ExpLOBinterface.h"
#include "ex_exe_stmt_globals.h"

namespace {
  typedef std::vector<Text> TextVec;
}

// -----------------------------------------------------------------------
// Forward class declarations
// -----------------------------------------------------------------------
class SequenceFileWriter;
class OrcFileVectorWriter;
class ParquetFileWriter;
class AvroFileWriter;

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------


class IOBuffer
{

public:

  enum BufferStatus {PARTIAL = 0, FULL, EMPTY, ERR};

  IOBuffer(char *buffer, Int32 bufSize)
    : bytesLeft_(bufSize),
      bufSize_(bufSize),
      numRows_(0),
      status_(EMPTY)
  {
    data_ = buffer;
    //memset(data_, '\0', bufSize);

  }

  ~IOBuffer()
  {
  }

  void setStatus(BufferStatus val)
  {
    status_ = val;
  }
  BufferStatus getStatus()
  {
    return status_ ;
  }

  char* data_;
  Int32 bytesLeft_;
  Int32 bufSize_;
  Int32 numRows_;
  BufferStatus status_;

};
//----------------------------------------------------------------------
// Task control block



#endif // __EX_FAST_TRANSPORT_H
