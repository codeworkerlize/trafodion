// @@@ START COPYRIGHT @@@
// //
// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.
// //
// // @@@ END COPYRIGHT @@@
// //
#include <stdio.h>
#include <sys/time.h>
#include <math.h>
#include "ttime.h"

const char* reportTime(int64_t time)
{
   static __thread char tdiff[200];
   uint32_t ms  = (uint32_t) (((time % 1000000) + 500) / 1000);
   uint32_t sec = (uint32_t) (time / 1000000);
   uint32_t min = sec/60;
   sec = sec % 60;
   uint32_t hour = min/60;
   min = min % 60;

   sprintf(tdiff, "%02u:%02u:%02u.%03u (us=%ld)", hour, min, sec, ms, time);

   return tdiff;
}

int64_t currentTimeInUsec()
{
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return (int64_t)(tv.tv_sec * 1000000) + tv.tv_usec;
}

