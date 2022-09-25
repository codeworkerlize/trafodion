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
//
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <math.h>

#include "ComCextdecs.h"
#include "ttime.h"

const char* reportTimestamp()
{
 char buffer[26];
 int millisec;
 struct tm* tm_info;
 struct timeval tv;

  gettimeofday(&tv, NULL);

  millisec = lrint(tv.tv_usec/1000.0); // Round to nearest millisec
  if (millisec>=1000) { // Allow for rounding up to nearest second
    millisec -=1000;
    tv.tv_sec++;
  }

 tm_info = localtime(&tv.tv_sec);


 strftime(buffer, 26, "%Y:%m:%d %H:%M:%S", tm_info);

 static THREAD_P char buf[100];
 sprintf(buf, "%s.%03d, ", buffer, millisec);

 return buf;
}

const char* reportTimeDiff(Int64 time)
{
   static THREAD_P char tdiff[200];
   ULng32 ms  = (ULng32) (((time % 1000000) + 500) / 1000);
   ULng32 sec = (ULng32) (time / 1000000);
   ULng32 min = sec/60;
   sec = sec % 60;
   ULng32 hour = min/60;
   min = min % 60;

   sprintf(tdiff, "%02u:%02u:%02u.%03u (us=%ld)", hour, min, sec, ms, time);

   return tdiff;
}

Int64 currentTimeStampInUsec()
{
  struct timeval tv;

  gettimeofday(&tv, NULL);

  return (Int64)(tv.tv_sec * 1000000) + tv.tv_usec;
}

