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

#ifndef _COMMEMLOG_H_
#define _COMMEMLOG_H_

#include <limits.h>
//#include "Platform.h"

#define DEFAULT_MEMLOG_SIZE 10485760 //1024*1024*10

class ComMemLog
{
public:
        ComMemLog();
        virtual ~ComMemLog() { deinitialize(); }
        int initialize(long pid, bool read=true);
        static ComMemLog& instance();
        void memlog(const char*format, ...);
        void memshow();
private:
        void initForRead();
        void initForWrite();
        void deinitialize();
        void buildPath(long pid);
        void setSize();
        int doMap();
        void loopback();
        void updateCurrentLength(long len);
        // Copy constructor and assignment operator are not defined.
        ComMemLog(const ComMemLog&);
        ComMemLog& operator=(const ComMemLog&);
private:
        char *startPtr;
        long maxLength;
        long currLength;
        long loopbackNm;
        char fileName[NAME_MAX];
        int  fileHandle;
        bool isRead;
};


#endif  /* _COMMEMLOG_H_ */
