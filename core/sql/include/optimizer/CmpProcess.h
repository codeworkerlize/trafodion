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
#ifndef CMPPROCESS_H
#define CMPPROCESS_H

#include "common/CmpCommon.h"
#include "common/ComCextdecs.h"

/************************************************************************
class CmpProcess

Used to get information about this compiler
process through Guardian procedure calls

************************************************************************/
class CmpProcess {
 public:
  CmpProcess();

  // getters
  inline short getNodeNum() { return nodeNum_; }
  inline short getPin() { return pin_; }
  inline int getSegmentNum() { return segmentNum_; }
  inline long getProcessStartTime() { return processStartTime_; }

  long getProcessDuration();
  int getCurrentSystemHeapSize();

  // generate the compiler id out of cpu#, pin, node#, and process start timestamp
  void getCompilerId(char *id, int len);

 private:
  short nodeNum_;  // cpu num
  short pin_;
  int segmentNum_;
  //
  // timestamp for when this CmpProcess was created
  long processStartTime_;
};
/************************************************************************
helper functions for timestamps

************************************************************************/
static void getTimestampAsBuffer(long juliantimestamp, char *datetime) {
  short timestamp[8];

  CMPASSERT(NULL != datetime);

  INTERPRETTIMESTAMP(juliantimestamp, timestamp);
  str_sprintf(datetime, "%04d/%02d/%02d %02d:%02d:%02d.%03u%03u",
              timestamp[0],   // year
              timestamp[1],   // month
              timestamp[2],   // day
              timestamp[3],   // hour
              timestamp[4],   // minute
              timestamp[5],   // second
              timestamp[6],   // fraction
              timestamp[7]);  // fraction
}
static long getCurrentTimestamp() {
  // return timestamp in local civil time
  // return CONVERTTIMESTAMP(JULIANTIMESTAMP(0,0,0,-1),0,-1,0);
  return JULIANTIMESTAMP(0, 0, 0, -1);
}

static long getCurrentTimestampUEpoch() {
  // return local timestamp in unix epoch (since January 1, 1970). This is what
  // is expected by the repository UNC
  time_t utcTimeStamp = time(0);
  tm localTime;
  localtime_r(&utcTimeStamp, &localTime);
  time_t lctTimeStamp = timegm(&localTime);

  long usLctTimestamp = 1000000 * (long)lctTimeStamp;
  return usLctTimestamp;
}

#endif  // CMPPROCESS_H
